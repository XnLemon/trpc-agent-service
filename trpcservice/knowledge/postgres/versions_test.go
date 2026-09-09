package postgres_test

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	knowledgeadmin "github.com/XnLemon/trpc-agent-service/trpcservice/knowledge"
	knowledgepostgres "github.com/XnLemon/trpc-agent-service/trpcservice/knowledge/postgres"
)

const (
	versionTenant = "t_00000000000000000000000000"
	versionApp    = "app_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	versionDigest = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
)

func TestVersionRepositoryAllocatesAndListsDurableManifest(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	repository := knowledgepostgres.NewVersionRepository(db)
	publishedAt := time.Unix(100, 0).UTC()
	value := knowledgeadmin.Version{
		TenantID: versionTenant, AppID: versionApp, DocumentCount: 2,
		ContentDigest: versionDigest, ActorID: "admin", PublishedAt: publishedAt,
	}

	mock.ExpectBegin()
	mock.ExpectExec("SELECT pg_catalog.pg_advisory_xact_lock").WithArgs(versionTenant + "\x00" + versionApp).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery("SELECT tenant_id, app_id, version, document_count, content_digest, actor_id, published_at").
		WithArgs(versionTenant, versionApp, versionDigest).
		WillReturnError(sql.ErrNoRows)
	mock.ExpectQuery("SELECT COALESCE\\(MAX\\(version\\), 0\\) \\+ 1").
		WithArgs(versionTenant, versionApp).
		WillReturnRows(sqlmock.NewRows([]string{"version"}).AddRow(int64(1)))
	mock.ExpectExec("INSERT INTO public.runtime_knowledge_version").
		WithArgs(versionTenant, versionApp, int64(1), 2, versionDigest, "admin", publishedAt).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	stored, err := repository.PublishVersion(context.Background(), value)
	if err != nil {
		t.Fatalf("PublishVersion() error = %v", err)
	}
	if stored.Version != 1 || stored.ContentDigest != versionDigest {
		t.Fatalf("stored version = %#v", stored)
	}

	mock.ExpectQuery("SELECT tenant_id, app_id, version, document_count, content_digest, actor_id, published_at").
		WithArgs(versionTenant, versionApp).
		WillReturnRows(sqlmock.NewRows([]string{"tenant_id", "app_id", "version", "document_count", "content_digest", "actor_id", "published_at"}).
			AddRow(versionTenant, versionApp, int64(1), 2, versionDigest, "admin", publishedAt))
	values, err := repository.ListVersions(context.Background(), knowledgeadmin.Scope{TenantID: versionTenant, AppID: versionApp})
	if err != nil {
		t.Fatalf("ListVersions() error = %v", err)
	}
	if len(values) != 1 || values[0].Version != 1 {
		t.Fatalf("listed versions = %#v", values)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestVersionRepositoryReturnsExistingDigestIdempotently(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	repository := knowledgepostgres.NewVersionRepository(db)
	publishedAt := time.Unix(100, 0).UTC()
	value := knowledgeadmin.Version{TenantID: versionTenant, AppID: versionApp, DocumentCount: 1, ContentDigest: versionDigest, ActorID: "new-admin", PublishedAt: publishedAt.Add(time.Minute)}

	mock.ExpectBegin()
	mock.ExpectExec("SELECT pg_catalog.pg_advisory_xact_lock").WithArgs(versionTenant + "\x00" + versionApp).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery("SELECT tenant_id, app_id, version, document_count, content_digest, actor_id, published_at").
		WithArgs(versionTenant, versionApp, versionDigest).
		WillReturnRows(sqlmock.NewRows([]string{"tenant_id", "app_id", "version", "document_count", "content_digest", "actor_id", "published_at"}).
			AddRow(versionTenant, versionApp, int64(4), 7, versionDigest, "original-admin", publishedAt))
	mock.ExpectCommit()
	stored, err := repository.PublishVersion(context.Background(), value)
	if err != nil {
		t.Fatalf("idempotent PublishVersion() error = %v", err)
	}
	if stored.Version != 4 || stored.DocumentCount != 7 || stored.ActorID != "original-admin" {
		t.Fatalf("idempotent version = %#v", stored)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestVersionRepositoryRejectsInvalidManifest(t *testing.T) {
	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	repository := knowledgepostgres.NewVersionRepository(db)
	_, err = repository.PublishVersion(context.Background(), knowledgeadmin.Version{TenantID: versionTenant, AppID: versionApp, ContentDigest: "not-a-digest", ActorID: "admin", PublishedAt: time.Now().UTC()})
	if !errors.Is(err, knowledgeadmin.ErrInvalid) {
		t.Fatalf("invalid manifest error = %v", err)
	}
}
