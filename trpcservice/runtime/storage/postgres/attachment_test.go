package postgres_test

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/XnLemon/trpc-agent-service/trpcservice/attachment"
	runtimestorage "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage"
	runtimepostgres "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage/postgres"
)

var runtimeAttachmentColumns = []string{"tenant_id", "attachment_id", "kind", "mime_type", "name", "size", "sha256", "provider", "provider_id", "event_id", "expires_at"}

func TestPostgresAttachmentLifecycleContracts(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	store := runtimepostgres.New(db)
	data := []byte("document")
	reference := attachmentReference(data)
	when := time.Now().UTC()

	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta("SELECT tenant_id,attachment_id,kind,mime_type,name,size,sha256,provider,provider_id,event_id,expires_at FROM public.runtime_attachment WHERE tenant_id=$1 AND attachment_id=$2 FOR UPDATE")).
		WithArgs("tenant-a", reference.ID).WillReturnError(sql.ErrNoRows)
	mock.ExpectExec(regexp.QuoteMeta("INSERT INTO public.runtime_object (tenant_id,object_key,content_type,content,size,etag) VALUES ($1,$2,$3,$4,$5,$6) ON CONFLICT (tenant_id,object_key) DO NOTHING")).
		WithArgs("tenant-a", reference.ID, reference.MIMEType, data, reference.Size, reference.SHA256).WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectQuery(regexp.QuoteMeta("SELECT content_type,size,etag FROM public.runtime_object WHERE tenant_id=$1 AND object_key=$2")).
		WithArgs("tenant-a", reference.ID).WillReturnRows(sqlmock.NewRows([]string{"content_type", "size", "etag"}).AddRow(reference.MIMEType, reference.Size, reference.SHA256))
	mock.ExpectExec(regexp.QuoteMeta("INSERT INTO public.runtime_attachment (tenant_id,attachment_id,kind,mime_type,name,size,sha256,provider,provider_id,expires_at) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) ON CONFLICT (tenant_id,attachment_id) DO NOTHING")).
		WithArgs("tenant-a", reference.ID, reference.Kind, reference.MIMEType, reference.Name, reference.Size, reference.SHA256, reference.Provider, reference.ProviderID, sqlmock.AnyArg()).WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()
	stored, err := store.PutAttachment(context.Background(), "tenant-a", attachment.Upload{ID: reference.ID, Kind: reference.Kind, MIMEType: reference.MIMEType, Size: reference.Size}, strings.NewReader(string(data)))
	if err != nil || stored != reference {
		t.Fatalf("PutAttachment = %+v, %v", stored, err)
	}

	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta("SELECT EXISTS(SELECT 1 FROM public.message_event WHERE tenant_id=$1 AND event_id=$2)")).
		WithArgs("tenant-a", "event-a").WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(true))
	mock.ExpectQuery(regexp.QuoteMeta("SELECT tenant_id,attachment_id,kind,mime_type,name,size,sha256,provider,provider_id,event_id,expires_at FROM public.runtime_attachment WHERE tenant_id=$1 AND attachment_id=$2 FOR UPDATE")).
		WithArgs("tenant-a", reference.ID).WillReturnRows(runtimeAttachmentRow(reference, nil, when.Add(time.Hour)))
	mock.ExpectExec(regexp.QuoteMeta("UPDATE public.runtime_attachment SET event_id=$3 WHERE tenant_id=$1 AND attachment_id=$2 AND (event_id IS NULL OR event_id=$3)")).
		WithArgs("tenant-a", reference.ID, "event-a").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	if err := store.BindAttachments(context.Background(), "tenant-a", "event-a", []attachment.Reference{reference}); err != nil {
		t.Fatalf("BindAttachments = %v; expectations = %v", err, mock.ExpectationsWereMet())
	}

	mock.ExpectQuery(regexp.QuoteMeta("SELECT tenant_id,attachment_id,kind,mime_type,name,size,sha256,provider,provider_id,event_id,expires_at FROM public.runtime_attachment WHERE tenant_id=$1 AND attachment_id=$2")).
		WithArgs("tenant-a", reference.ID).WillReturnRows(runtimeAttachmentRow(reference, "event-a", when.Add(time.Hour)))
	mock.ExpectQuery(regexp.QuoteMeta("SELECT content FROM public.runtime_object WHERE tenant_id=$1 AND object_key=$2")).
		WithArgs("tenant-a", reference.ID).WillReturnRows(sqlmock.NewRows([]string{"content"}).AddRow(data))
	content, err := store.Load(context.Background(), "tenant-a", "event-a", reference)
	if err != nil || string(content.Data) != string(data) {
		t.Fatalf("Load = %q, %v", content.Data, err)
	}

	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta("DELETE FROM public.runtime_attachment AS a WHERE a.tenant_id=$1 AND a.expires_at <= $2 AND (a.event_id IS NULL OR EXISTS (SELECT 1 FROM public.message_event AS e WHERE e.tenant_id=a.tenant_id AND e.event_id=a.event_id AND e.status IN ('completed','failed'))) RETURNING a.attachment_id")).
		WithArgs("tenant-a", when).WillReturnRows(sqlmock.NewRows([]string{"attachment_id"}).AddRow(reference.ID))
	mock.ExpectExec(regexp.QuoteMeta("DELETE FROM public.runtime_object WHERE tenant_id=$1 AND object_key=$2")).WithArgs("tenant-a", reference.ID).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	removed, err := store.CleanupAttachments(context.Background(), "tenant-a", when)
	if err != nil || removed != 1 {
		t.Fatalf("CleanupAttachments = %d, %v", removed, err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestPostgresAttachmentConcurrentPutKeepsExactReference(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	data := []byte("document")
	reference := attachmentReference(data)
	when := time.Now().UTC().Add(time.Hour)
	mock.ExpectBegin()
	mock.ExpectQuery("FROM public.runtime_attachment WHERE tenant_id=\\$1 AND attachment_id=\\$2 FOR UPDATE").WithArgs("tenant-a", reference.ID).WillReturnError(sql.ErrNoRows)
	mock.ExpectExec("INSERT INTO public.runtime_object").WithArgs("tenant-a", reference.ID, reference.MIMEType, data, reference.Size, reference.SHA256).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery("SELECT content_type,size,etag FROM public.runtime_object").WithArgs("tenant-a", reference.ID).WillReturnRows(sqlmock.NewRows([]string{"content_type", "size", "etag"}).AddRow(reference.MIMEType, reference.Size, reference.SHA256))
	mock.ExpectExec("INSERT INTO public.runtime_attachment").WithArgs("tenant-a", reference.ID, reference.Kind, reference.MIMEType, reference.Name, reference.Size, reference.SHA256, reference.Provider, reference.ProviderID, sqlmock.AnyArg()).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery("FROM public.runtime_attachment WHERE tenant_id=\\$1 AND attachment_id=\\$2 FOR UPDATE").WithArgs("tenant-a", reference.ID).WillReturnRows(runtimeAttachmentRow(reference, nil, when))
	mock.ExpectCommit()
	stored, err := runtimepostgres.New(db).PutAttachment(context.Background(), "tenant-a", attachment.Upload{ID: reference.ID, Kind: reference.Kind, MIMEType: reference.MIMEType, Size: reference.Size}, strings.NewReader(string(data)))
	if err != nil || stored != reference {
		t.Fatalf("PutAttachment concurrent result = %+v, %v; expectations = %v", stored, err, mock.ExpectationsWereMet())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestPostgresAttachmentMethodsFailClosed(t *testing.T) {
	var nilStore *runtimepostgres.Store
	data := []byte("document")
	reference := attachmentReference(data)
	if _, err := nilStore.PutAttachment(context.Background(), "tenant-a", attachment.Upload{ID: reference.ID, Kind: reference.Kind, MIMEType: reference.MIMEType, Size: reference.Size}, strings.NewReader(string(data))); !errors.Is(err, runtimestorage.ErrStorage) {
		t.Fatalf("nil PutAttachment = %v", err)
	}
	if _, err := nilStore.Load(context.Background(), "tenant-a", "event-a", reference); !errors.Is(err, runtimestorage.ErrStorage) {
		t.Fatalf("nil Load = %v", err)
	}
	if err := nilStore.BindAttachments(context.Background(), "tenant-a", "event-a", []attachment.Reference{reference}); !errors.Is(err, runtimestorage.ErrStorage) {
		t.Fatalf("nil BindAttachments = %v", err)
	}
	if _, err := nilStore.CleanupAttachments(context.Background(), "tenant-a", time.Now()); !errors.Is(err, runtimestorage.ErrStorage) {
		t.Fatalf("nil CleanupAttachments = %v", err)
	}
}

func attachmentReference(data []byte) attachment.Reference {
	digest := sha256.Sum256(data)
	return attachment.Reference{ID: "attachment-1", Kind: attachment.KindDocument, MIMEType: "application/pdf", Size: int64(len(data)), SHA256: hex.EncodeToString(digest[:])}
}

func runtimeAttachmentRow(reference attachment.Reference, eventID any, expiresAt time.Time) *sqlmock.Rows {
	return sqlmock.NewRows(runtimeAttachmentColumns).AddRow("tenant-a", reference.ID, string(reference.Kind), reference.MIMEType, reference.Name, reference.Size, reference.SHA256, reference.Provider, reference.ProviderID, eventID, expiresAt)
}
