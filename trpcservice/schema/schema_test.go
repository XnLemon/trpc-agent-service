package schema

import (
	"context"
	"errors"
	"regexp"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestOrderResolvesDependenciesAndRejectsInvalidGraphs(t *testing.T) {
	modules := []Module{
		{Name: "child", Driver: DriverPostgres, Dependencies: []string{"parent"}, SQL: "CREATE TABLE child (id INT);"},
		{Name: "parent", Driver: DriverPostgres, SQL: "CREATE TABLE parent (id INT);"},
	}
	ordered, err := Order(modules)
	if err != nil {
		t.Fatal(err)
	}
	if len(ordered) != 2 || ordered[0].Name != "parent" || ordered[1].Name != "child" {
		t.Fatalf("ordered modules = %#v", ordered)
	}
	for name, invalid := range map[string][]Module{
		"missing dependency": {{Name: "child", Driver: DriverPostgres, Dependencies: []string{"missing"}, SQL: "SELECT 1"}},
		"cycle": {
			{Name: "a", Driver: DriverPostgres, Dependencies: []string{"b"}, SQL: "SELECT 1"},
			{Name: "b", Driver: DriverPostgres, Dependencies: []string{"a"}, SQL: "SELECT 1"},
		},
		"duplicate": {
			{Name: "same", Driver: DriverPostgres, SQL: "SELECT 1"},
			{Name: "same", Driver: DriverPostgres, SQL: "SELECT 1"},
		},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := Order(invalid); !errors.Is(err, ErrInvalidModule) {
				t.Fatalf("Order error = %v", err)
			}
		})
	}
}

func TestExecuteSplitsPostgreSQLDollarQuotesAndMySQLCompoundStatements(t *testing.T) {
	postgresDB, postgresMock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = postgresDB.Close() })
	postgresMock.ExpectExec("CREATE TABLE example").WillReturnResult(sqlmock.NewResult(0, 0))
	postgresMock.ExpectExec("CREATE FUNCTION example_guard").WillReturnResult(sqlmock.NewResult(0, 0))
	postgresScript := `-- comment;
CREATE TABLE example (id INT);
CREATE FUNCTION example_guard() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF NEW.id IS NULL THEN
    RAISE EXCEPTION 'id;required';
  END IF;
  RETURN NEW;
END;
$$;`
	if err := Execute(context.Background(), postgresDB, DriverPostgres, postgresScript); err != nil {
		t.Fatal(err)
	}
	if err := postgresMock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}

	mysqlDB, mysqlMock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = mysqlDB.Close() })
	mysqlMock.ExpectExec("CREATE TABLE example").WillReturnResult(sqlmock.NewResult(0, 0))
	mysqlMock.ExpectExec("CREATE TRIGGER example_guard").WillReturnResult(sqlmock.NewResult(0, 0))
	mysqlScript := `CREATE TABLE example (value VARCHAR(32) DEFAULT 'a;b');
CREATE TRIGGER example_guard BEFORE INSERT ON example
FOR EACH ROW
BEGIN
  IF NEW.value IS NULL THEN
    SET NEW.value = 'a;b';
  END IF;
END;`
	if err := Execute(context.Background(), mysqlDB, DriverMySQL, mysqlScript); err != nil {
		t.Fatal(err)
	}
	if err := mysqlMock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestVerifyChecksPackageOwnedTables(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	query := regexp.QuoteMeta("SELECT EXISTS (\n\t\t\tSELECT 1 FROM information_schema.tables\n\t\t\tWHERE table_schema = current_schema() AND table_name = $1\n\t\t)")
	mock.ExpectQuery(query).WithArgs("tenant").WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(true))
	mock.ExpectQuery(query).WithArgs("missing").WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(false))
	if err := Verify(context.Background(), db, DriverPostgres, []string{"tenant", "missing"}); !errors.Is(err, ErrSchema) {
		t.Fatalf("Verify error = %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
