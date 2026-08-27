package mysql

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	driver "github.com/go-sql-driver/mysql"
)

func TestMapErrorRedactsMySQLDriverCategories(t *testing.T) {
	notFound := errors.New("not found")
	duplicate := errors.New("duplicate")
	conflict := errors.New("conflict")
	invalid := errors.New("invalid")
	cases := []struct {
		name   string
		number uint16
		want   error
	}{
		{name: "duplicate", number: 1062, want: duplicate},
		{name: "deadlock", number: 1213, want: conflict},
		{name: "lock wait", number: 1205, want: conflict},
		{name: "foreign key", number: 1452, want: invalid},
		{name: "invalid value", number: 1366, want: invalid},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			if got := MapError(context.Background(), &driver.MySQLError{Number: test.number, Message: "secret host detail"}, notFound, duplicate, conflict, invalid); !errors.Is(got, test.want) {
				t.Fatalf("mapped error = %v, want %v", got, test.want)
			}
			if got := MapError(context.Background(), &driver.MySQLError{Number: test.number, Message: "secret host detail"}, notFound, duplicate, conflict, invalid); got.Error() == "secret host detail" {
				t.Fatal("driver detail leaked")
			}
		})
	}
	if got := MapError(context.Background(), sql.ErrNoRows, notFound, duplicate, conflict, invalid); !errors.Is(got, notFound) {
		t.Fatalf("not found mapping = %v", got)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if got := MapError(canceled, &driver.MySQLError{Number: 1062}, notFound, duplicate, conflict, invalid); !errors.Is(got, context.Canceled) {
		t.Fatalf("context mapping = %v", got)
	}
}

func TestBeginUsesDatabaseSQLTransactionAndRollback(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	mock.ExpectBegin()
	tx, err := Begin(context.Background(), db)
	if err != nil {
		t.Fatal(err)
	}
	Rollback(tx)
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestNormalizeDSNAddsUTCParseTime(t *testing.T) {
	if got := normalizeDSN("user:password@tcp(localhost:3306)/control"); got != "user:password@tcp(localhost:3306)/control?parseTime=true&loc=UTC" {
		t.Fatalf("normalized DSN = %q", got)
	}
	if got := normalizeDSN("user:password@tcp(localhost:3306)/control?parseTime=true"); got != "user:password@tcp(localhost:3306)/control?parseTime=true&loc=UTC" {
		t.Fatalf("existing parameter DSN = %q", got)
	}
	if got := normalizeDSN("user:password@tcp(localhost:3306)/control?parseTime=false&loc=Local&charset=utf8mb4"); got != "user:password@tcp(localhost:3306)/control?charset=utf8mb4&parseTime=true&loc=UTC" {
		t.Fatalf("conflicting parameter DSN = %q", got)
	}
	if normalizeDSN(" ") != "" {
		t.Fatal("blank DSN was accepted")
	}
}
