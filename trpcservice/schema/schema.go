// Package schema contains the small, driver-aware contract shared by
// package-owned database schemas and the migration orchestrator.
package schema

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
)

// Driver identifies the SQL dialect used by a schema module.
type Driver string

const (
	DriverPostgres Driver = "postgres"
	DriverMySQL    Driver = "mysql"
)

// Module describes one package-owned schema. SQL is intentionally carried by
// the package that owns the tables; the migration package only orders and
// executes these modules.
type Module struct {
	Name         string
	Driver       Driver
	Dependencies []string
	Tables       []string
	SQL          string
}

// Executor is implemented by sql.DB, sql.Conn, and sql.Tx.
type Executor interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}

var (
	// ErrSchema reports an invalid schema module or schema initialization
	// failure without exposing database driver details to callers.
	ErrSchema = errors.New("schema initialization failed")
	// ErrInvalidModule reports a malformed or cyclic module graph.
	ErrInvalidModule = errors.New("invalid schema module")
)

// Apply executes one package-owned schema. It is idempotent when the module's
// SQL uses the database's IF NOT EXISTS primitives, as the built-in modules do.
func Apply(ctx context.Context, db *sql.DB, module Module) error {
	if db == nil {
		return ErrSchema
	}
	return Execute(ctx, db, module.Driver, module.SQL)
}

// Execute executes a schema script one statement at a time. Keeping the
// statement boundary here lets MySQL run without multiStatements while still
// allowing PostgreSQL functions and dollar-quoted bodies in shared helpers.
func Execute(ctx context.Context, executor Executor, driver Driver, script string) error {
	if ctx == nil || executor == nil {
		return ErrSchema
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if driver != DriverPostgres && driver != DriverMySQL {
		return fmt.Errorf("%w: unsupported driver %q", ErrInvalidModule, driver)
	}
	statements := splitStatements(script, driver)
	for _, statement := range statements {
		if _, err := executor.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("%w: execute %s schema statement", ErrSchema, driver)
		}
	}
	return nil
}

// ApplyModules initializes modules in dependency order on an existing pool.
func ApplyModules(ctx context.Context, db *sql.DB, modules []Module) error {
	if db == nil {
		return ErrSchema
	}
	ordered, err := Order(modules)
	if err != nil {
		return err
	}
	for _, module := range ordered {
		if err := Apply(ctx, db, module); err != nil {
			return fmt.Errorf("%w: module %s", err, module.Name)
		}
	}
	return nil
}

// Verify checks that every table declared by a module exists in the current
// database. Detailed column/index verification remains package-specific when
// a backend needs stronger guarantees.
func Verify(ctx context.Context, db *sql.DB, driver Driver, tables []string) error {
	if ctx == nil || db == nil {
		return ErrSchema
	}
	if driver != DriverPostgres && driver != DriverMySQL {
		return fmt.Errorf("%w: unsupported driver %q", ErrInvalidModule, driver)
	}
	for _, table := range tables {
		table = strings.TrimSpace(table)
		if table == "" {
			return fmt.Errorf("%w: empty table name", ErrInvalidModule)
		}
		query := `SELECT EXISTS (
			SELECT 1 FROM information_schema.tables
			WHERE table_schema = current_schema() AND table_name = $1
		)`
		if driver == DriverMySQL {
			query = `SELECT EXISTS (
				SELECT 1 FROM information_schema.tables
				WHERE table_schema = DATABASE() AND table_name = ?
			)`
		}
		var exists bool
		if err := db.QueryRowContext(ctx, query, table).Scan(&exists); err != nil {
			return fmt.Errorf("%w: verify table %s", ErrSchema, table)
		}
		if !exists {
			return fmt.Errorf("%w: table %s is missing", ErrSchema, table)
		}
	}
	return nil
}

// VerifyModules verifies all declared module tables in dependency order.
func VerifyModules(ctx context.Context, db *sql.DB, modules []Module) error {
	if db == nil {
		return ErrSchema
	}
	ordered, err := Order(modules)
	if err != nil {
		return err
	}
	for _, module := range ordered {
		if err := Verify(ctx, db, module.Driver, module.Tables); err != nil {
			return fmt.Errorf("%w: module %s", err, module.Name)
		}
	}
	return nil
}

// ExecuteModules executes modules on a pinned connection. The migration
// runner uses this form so schema creation and history updates share its lock.
func ExecuteModules(ctx context.Context, executor Executor, modules []Module) error {
	if executor == nil {
		return ErrSchema
	}
	ordered, err := Order(modules)
	if err != nil {
		return err
	}
	for _, module := range ordered {
		if err := Execute(ctx, executor, module.Driver, module.SQL); err != nil {
			return fmt.Errorf("%w: module %s", err, module.Name)
		}
	}
	return nil
}

// Order validates and topologically sorts modules. Ties retain the caller's
// order, which makes the dependency graph deterministic and reviewable.
func Order(modules []Module) ([]Module, error) {
	byName := make(map[string]Module, len(modules))
	for _, module := range modules {
		if strings.TrimSpace(module.Name) == "" || (module.Driver != DriverPostgres && module.Driver != DriverMySQL) || strings.TrimSpace(module.SQL) == "" {
			return nil, fmt.Errorf("%w: malformed module %q", ErrInvalidModule, module.Name)
		}
		if _, exists := byName[module.Name]; exists {
			return nil, fmt.Errorf("%w: duplicate module %q", ErrInvalidModule, module.Name)
		}
		byName[module.Name] = module
	}
	state := make(map[string]uint8, len(modules))
	ordered := make([]Module, 0, len(modules))
	var visit func(string) error
	visit = func(name string) error {
		switch state[name] {
		case 1:
			return fmt.Errorf("%w: dependency cycle at %s", ErrInvalidModule, name)
		case 2:
			return nil
		}
		module, exists := byName[name]
		if !exists {
			return fmt.Errorf("%w: module %s is missing", ErrInvalidModule, name)
		}
		state[name] = 1
		for _, dependency := range module.Dependencies {
			dependency = strings.TrimSpace(dependency)
			if dependency == "" {
				return fmt.Errorf("%w: module %s has an empty dependency", ErrInvalidModule, name)
			}
			dependencyModule, exists := byName[dependency]
			if !exists {
				return fmt.Errorf("%w: module %s depends on missing module %s", ErrInvalidModule, name, dependency)
			}
			if dependencyModule.Driver != module.Driver {
				return fmt.Errorf("%w: module %s depends on %s with another driver", ErrInvalidModule, name, dependency)
			}
			if err := visit(dependency); err != nil {
				return err
			}
		}
		state[name] = 2
		ordered = append(ordered, module)
		return nil
	}
	for _, module := range modules {
		if err := visit(module.Name); err != nil {
			return nil, err
		}
	}
	return ordered, nil
}

func splitStatements(script string, driver Driver) []string {
	var statements []string
	start := 0
	var quote byte
	var dollarTag string
	lineComment, blockComment := false, false
	parenDepth := 0
	compoundDepth := 0
	skipCompoundKeyword := false
	for index := 0; index < len(script); index++ {
		ch := script[index]
		if lineComment {
			if ch == '\n' {
				lineComment = false
			}
			continue
		}
		if blockComment {
			if ch == '*' && index+1 < len(script) && script[index+1] == '/' {
				blockComment = false
				index++
			}
			continue
		}
		if dollarTag != "" {
			if strings.HasPrefix(script[index:], dollarTag) {
				index += len(dollarTag) - 1
				dollarTag = ""
			}
			continue
		}
		if quote != 0 {
			if ch == quote {
				if index+1 < len(script) && script[index+1] == quote {
					index++
				} else {
					quote = 0
				}
			}
			continue
		}
		if ch == '-' && index+1 < len(script) && script[index+1] == '-' {
			lineComment = true
			index++
			continue
		}
		if ch == '/' && index+1 < len(script) && script[index+1] == '*' {
			blockComment = true
			index++
			continue
		}
		if driver == DriverPostgres && ch == '$' {
			if tag, ok := dollarQuoteTag(script[index:]); ok {
				dollarTag = tag
				index += len(tag) - 1
				continue
			}
		}
		if driver == DriverMySQL && isIdentifierStart(ch) {
			end := index + 1
			for end < len(script) && isIdentifierPart(script[end]) {
				end++
			}
			word := strings.ToUpper(script[index:end])
			if skipCompoundKeyword {
				skipCompoundKeyword = false
			} else {
				switch word {
				case "BEGIN":
					compoundDepth++
				case "IF", "CASE", "LOOP", "WHILE", "REPEAT":
					if compoundDepth > 0 {
						compoundDepth++
					}
				case "END":
					compoundDepth--
					if compoundDepth < 0 {
						compoundDepth = 0
					}
					next, _ := nextWord(script, end)
					if next == "IF" || next == "CASE" || next == "LOOP" || next == "WHILE" || next == "REPEAT" {
						skipCompoundKeyword = true
					}
				}
			}
			index = end - 1
			continue
		}
		if ch == '\'' || ch == '"' || (driver == DriverMySQL && ch == '`') {
			quote = ch
			continue
		}
		if ch == '(' {
			parenDepth++
			continue
		}
		if ch == ')' && parenDepth > 0 {
			parenDepth--
			continue
		}
		if ch == ';' && parenDepth == 0 && compoundDepth == 0 {
			if statement := strings.TrimSpace(script[start:index]); statement != "" {
				statements = append(statements, statement)
			}
			start = index + 1
		}
	}
	if statement := strings.TrimSpace(script[start:]); statement != "" {
		statements = append(statements, statement)
	}
	return statements
}

func isIdentifierStart(ch byte) bool {
	return ch >= 'a' && ch <= 'z' || ch >= 'A' && ch <= 'Z' || ch == '_'
}

func isIdentifierPart(ch byte) bool {
	return isIdentifierStart(ch) || ch >= '0' && ch <= '9' || ch == '$'
}

func nextWord(script string, start int) (string, int) {
	for start < len(script) {
		if script[start] == ' ' || script[start] == '\t' || script[start] == '\n' || script[start] == '\r' {
			start++
			continue
		}
		if !isIdentifierStart(script[start]) {
			return "", start
		}
		end := start + 1
		for end < len(script) && isIdentifierPart(script[end]) {
			end++
		}
		return strings.ToUpper(script[start:end]), end
	}
	return "", start
}

func dollarQuoteTag(script string) (string, bool) {
	end := strings.IndexByte(script[1:], '$')
	if end < 0 {
		return "", false
	}
	end++
	tag := script[:end+1]
	if len(tag) < 2 {
		return "", false
	}
	for index, ch := range tag[1 : len(tag)-1] {
		if !(ch == '_' || ch >= 'a' && ch <= 'z' || ch >= 'A' && ch <= 'Z' || index > 0 && ch >= '0' && ch <= '9') {
			return "", false
		}
	}
	return tag, true
}
