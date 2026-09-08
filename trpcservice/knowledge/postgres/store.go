// Package postgres provides a tenant-scoped PostgreSQL implementation of the
// upstream knowledge/vectorstore.VectorStore contract.
//
// Embeddings are kept as JSONB rather than requiring a database extension. This
// keeps migrations portable across managed PostgreSQL offerings; the adapter
// still exposes the same durable contract and can be replaced by a pgvector
// query implementation without changing the Agent or Backend interfaces.
package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"trpc.group/trpc-go/trpc-agent-go/knowledge/document"
	"trpc.group/trpc-go/trpc-agent-go/knowledge/searchfilter"
	"trpc.group/trpc-go/trpc-agent-go/knowledge/source"
	"trpc.group/trpc-go/trpc-agent-go/knowledge/vectorstore"
)

const (
	defaultMaxResults = 10
	maxDimension      = 65536
)

var (
	// ErrInvalid reports malformed documents, vectors, filters, or options.
	ErrInvalid = errors.New("invalid PostgreSQL knowledge vector store request")
	// ErrNotFound reports a missing document.
	ErrNotFound = errors.New("PostgreSQL knowledge document not found")
	// ErrConflict reports a concurrent or duplicate write conflict.
	ErrConflict = errors.New("PostgreSQL knowledge vector store conflict")
	// ErrStorage reports a database failure without exposing driver details.
	ErrStorage = errors.New("PostgreSQL knowledge vector store failure")
	// ErrClosed reports use after Close.
	ErrClosed = errors.New("PostgreSQL knowledge vector store is closed")
)

// Option configures one tenant-scoped Store.
type Option func(*Store)

// WithMaxResults sets the default result limit for searches.
func WithMaxResults(limit int) Option {
	return func(store *Store) {
		if limit > 0 {
			store.maxResults = limit
		}
	}
}

// WithDimension fixes the embedding dimension for this store. A zero value
// accepts the dimension reported by each write, which is useful during a
// controlled migration from another vector provider.
func WithDimension(dimension int) Option {
	return func(store *Store) {
		store.dimension = dimension
	}
}

// Store implements vectorstore.VectorStore for one explicit tenant. The SQL
// pool is borrowed and is never closed by Store.Close.
type Store struct {
	db       *sql.DB
	tenantID string

	maxResults int
	dimension  int

	mu     sync.RWMutex
	closed bool
}

var _ vectorstore.VectorStore = (*Store)(nil)

// New creates a tenant-scoped PostgreSQL vector store. It does not issue a
// network query; migration/readiness checks remain owned by Bootstrap.
func New(db *sql.DB, tenantID string, opts ...Option) (*Store, error) {
	if db == nil || strings.TrimSpace(tenantID) == "" {
		return nil, ErrInvalid
	}
	store := &Store{db: db, tenantID: tenantID, maxResults: defaultMaxResults}
	for _, opt := range opts {
		if opt != nil {
			opt(store)
		}
	}
	if store.dimension < 0 || store.dimension > maxDimension {
		return nil, ErrInvalid
	}
	return store, nil
}

// Add inserts or replaces a document for this tenant. Replacing a document is
// intentionally idempotent by document ID, while the original creation time
// remains durable in PostgreSQL.
func (store *Store) Add(ctx context.Context, doc *document.Document, embedding []float64) error {
	if err := store.check(ctx); err != nil {
		return err
	}
	metadata, vector, value, err := normalizeDocument(doc, embedding, store.dimension)
	if err != nil {
		return err
	}
	const query = `INSERT INTO public.runtime_knowledge_document
		(tenant_id, document_id, name, content, embedding_text, metadata, embedding, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7::jsonb, $8, $9)
		ON CONFLICT (tenant_id, document_id) DO UPDATE SET
		name = EXCLUDED.name, content = EXCLUDED.content,
		embedding_text = EXCLUDED.embedding_text, metadata = EXCLUDED.metadata,
		embedding = EXCLUDED.embedding, updated_at = EXCLUDED.updated_at
		RETURNING created_at, updated_at`
	if err := store.db.QueryRowContext(ctx, query, store.tenantID, value.ID, value.Name, value.Content, value.EmbeddingText, metadata, vector, value.CreatedAt, value.UpdatedAt).Scan(&value.CreatedAt, &value.UpdatedAt); err != nil {
		return mapError(ctx, err)
	}
	return nil
}

// Get retrieves one document and its embedding from this tenant.
func (store *Store) Get(ctx context.Context, id string) (*document.Document, []float64, error) {
	if err := store.check(ctx); err != nil {
		return nil, nil, err
	}
	if strings.TrimSpace(id) == "" {
		return nil, nil, ErrInvalid
	}
	const query = `SELECT document_id, name, content, embedding_text, metadata, embedding, created_at, updated_at
		FROM public.runtime_knowledge_document WHERE tenant_id = $1 AND document_id = $2`
	value, err := scanDocument(store.db.QueryRowContext(ctx, query, store.tenantID, id), store.dimension)
	if err != nil {
		return nil, nil, mapError(ctx, err)
	}
	return value.doc, value.embedding, nil
}

// Update modifies an existing document and preserves its creation timestamp.
func (store *Store) Update(ctx context.Context, doc *document.Document, embedding []float64) error {
	if err := store.check(ctx); err != nil {
		return err
	}
	metadata, vector, value, err := normalizeDocument(doc, embedding, store.dimension)
	if err != nil {
		return err
	}
	const query = `UPDATE public.runtime_knowledge_document
		SET name = $3, content = $4, embedding_text = $5, metadata = $6::jsonb,
			embedding = $7::jsonb, updated_at = $8
		WHERE tenant_id = $1 AND document_id = $2
		RETURNING created_at, updated_at`
	if err := store.db.QueryRowContext(ctx, query, store.tenantID, value.ID, value.Name, value.Content, value.EmbeddingText, metadata, vector, value.UpdatedAt).Scan(&value.CreatedAt, &value.UpdatedAt); err != nil {
		return mapError(ctx, err)
	}
	return nil
}

// Delete removes one document from this tenant.
func (store *Store) Delete(ctx context.Context, id string) error {
	if err := store.check(ctx); err != nil {
		return err
	}
	if strings.TrimSpace(id) == "" {
		return ErrInvalid
	}
	result, err := store.db.ExecContext(ctx, `DELETE FROM public.runtime_knowledge_document WHERE tenant_id = $1 AND document_id = $2`, store.tenantID, id)
	if err != nil {
		return mapError(ctx, err)
	}
	count, err := result.RowsAffected()
	if err != nil {
		return ErrStorage
	}
	if count == 0 {
		return ErrNotFound
	}
	return nil
}

// Search loads only this tenant's rows, applies the upstream filter contract,
// and computes cosine scores. The schema deliberately stores portable JSONB;
// a future pgvector implementation can replace this bounded read path while
// preserving the public VectorStore contract.
func (store *Store) Search(ctx context.Context, query *vectorstore.SearchQuery) (*vectorstore.SearchResult, error) {
	if err := store.check(ctx); err != nil {
		return nil, err
	}
	if query == nil || math.IsNaN(query.MinScore) || math.IsInf(query.MinScore, 0) {
		return nil, ErrInvalid
	}
	needsVector := query.SearchMode == vectorstore.SearchModeVector || query.SearchMode == vectorstore.SearchModeHybrid
	if query.SearchMode != vectorstore.SearchModeFilter && query.SearchMode != vectorstore.SearchModeKeyword && query.SearchMode != vectorstore.SearchModeVector && query.SearchMode != vectorstore.SearchModeHybrid {
		needsVector = true
	}
	if needsVector && len(query.Vector) == 0 {
		return nil, ErrInvalid
	}
	if len(query.Vector) > 0 && !validDimension(len(query.Vector), store.dimension) {
		return nil, ErrInvalid
	}
	rows, err := store.listDocuments(ctx)
	if err != nil {
		return nil, err
	}
	minScore := query.MinScore
	if minScore < 0 {
		minScore = 0
	}
	results := make([]*vectorstore.ScoredDocument, 0, len(rows))
	for _, row := range rows {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		matches, matchErr := matchesFilter(row.doc, query.Filter)
		if matchErr != nil {
			return nil, matchErr
		}
		if !matches {
			continue
		}
		score := 1.0
		if len(query.Vector) > 0 {
			score = cosineSimilarity(query.Vector, row.embedding)
		}
		if score < minScore {
			continue
		}
		results = append(results, &vectorstore.ScoredDocument{Document: row.doc.Clone(), Score: score})
	}
	sort.SliceStable(results, func(i, j int) bool {
		if results[i].Score != results[j].Score {
			return results[i].Score > results[j].Score
		}
		if !results[i].Document.CreatedAt.Equal(results[j].Document.CreatedAt) {
			return results[i].Document.CreatedAt.After(results[j].Document.CreatedAt)
		}
		return results[i].Document.ID < results[j].Document.ID
	})
	limit := query.Limit
	if limit <= 0 {
		limit = store.maxResults
	}
	if len(results) > limit {
		results = results[:limit]
	}
	return &vectorstore.SearchResult{Results: results}, nil
}

// DeleteByFilter deletes matching rows. Filtering is always intersected with
// the fixed tenant predicate and never accepts a caller-provided tenant field.
func (store *Store) DeleteByFilter(ctx context.Context, opts ...vectorstore.DeleteOption) error {
	if err := store.check(ctx); err != nil {
		return err
	}
	config := vectorstore.ApplyDeleteOptions(opts...)
	if config.DeleteAll {
		if len(config.DocumentIDs) > 0 || len(config.Filter) > 0 {
			return ErrInvalid
		}
		if _, err := store.db.ExecContext(ctx, `DELETE FROM public.runtime_knowledge_document WHERE tenant_id = $1`, store.tenantID); err != nil {
			return mapError(ctx, err)
		}
		return nil
	}
	if len(config.DocumentIDs) == 0 && len(config.Filter) == 0 {
		return ErrInvalid
	}
	rows, err := store.listDocuments(ctx)
	if err != nil {
		return err
	}
	ids := make([]string, 0)
	for _, row := range rows {
		if len(config.DocumentIDs) > 0 && !contains(config.DocumentIDs, row.doc.ID) {
			continue
		}
		if !matchesMetadata(row.doc.Metadata, config.Filter) {
			continue
		}
		ids = append(ids, row.doc.ID)
	}
	return store.deleteIDs(ctx, ids)
}

// UpdateByFilter applies the upstream update fields to rows selected in this
// tenant. It validates all update keys before changing any row.
func (store *Store) UpdateByFilter(ctx context.Context, opts ...vectorstore.UpdateByFilterOption) (int64, error) {
	if err := store.check(ctx); err != nil {
		return 0, err
	}
	config, err := vectorstore.ApplyUpdateByFilterOptions(opts...)
	if err != nil || len(config.Updates) == 0 {
		return 0, ErrInvalid
	}
	if err := validateUpdateKeys(config.Updates, store.dimension); err != nil {
		return 0, err
	}
	rows, err := store.listDocuments(ctx)
	if err != nil {
		return 0, err
	}
	var updated int64
	for _, row := range rows {
		if err := ctx.Err(); err != nil {
			return updated, err
		}
		if len(config.DocumentIDs) > 0 && !contains(config.DocumentIDs, row.doc.ID) {
			continue
		}
		matches, filterErr := matchesFilter(row.doc, &vectorstore.SearchFilter{FilterCondition: config.FilterCondition})
		if filterErr != nil {
			return updated, filterErr
		}
		if !matches {
			continue
		}
		candidate, vector, applyErr := applyUpdates(row.doc, row.embedding, config.Updates, store.dimension)
		if applyErr != nil {
			return updated, applyErr
		}
		if err := store.Update(ctx, candidate, vector); err != nil {
			return updated, err
		}
		updated++
	}
	return updated, nil
}

// Count returns the number of documents matching a metadata filter in this
// tenant.
func (store *Store) Count(ctx context.Context, opts ...vectorstore.CountOption) (int, error) {
	if err := store.check(ctx); err != nil {
		return 0, err
	}
	config := vectorstore.ApplyCountOptions(opts...)
	rows, err := store.listDocuments(ctx)
	if err != nil {
		return 0, err
	}
	count := 0
	for _, row := range rows {
		if matchesMetadata(row.doc.Metadata, config.Filter) {
			count++
		}
	}
	return count, nil
}

// GetMetadata returns defensive metadata copies with deterministic pagination.
func (store *Store) GetMetadata(ctx context.Context, opts ...vectorstore.GetMetadataOption) (map[string]vectorstore.DocumentMetadata, error) {
	if err := store.check(ctx); err != nil {
		return nil, err
	}
	config, err := vectorstore.ApplyGetMetadataOptions(opts...)
	if err != nil {
		return nil, ErrInvalid
	}
	rows, err := store.listDocuments(ctx)
	if err != nil {
		return nil, err
	}
	selected := make([]*document.Document, 0, len(rows))
	for _, row := range rows {
		if len(config.IDs) > 0 && !contains(config.IDs, row.doc.ID) {
			continue
		}
		if !matchesMetadata(row.doc.Metadata, config.Filter) {
			continue
		}
		selected = append(selected, row.doc)
	}
	sort.Slice(selected, func(i, j int) bool { return selected[i].ID < selected[j].ID })
	start := config.Offset
	if start < 0 {
		start = 0
	}
	if start >= len(selected) {
		return map[string]vectorstore.DocumentMetadata{}, nil
	}
	end := len(selected)
	if config.Limit >= 0 && start+config.Limit < end {
		end = start + config.Limit
	}
	result := make(map[string]vectorstore.DocumentMetadata, end-start)
	for _, doc := range selected[start:end] {
		metadata, cloneErr := cloneJSONMap(doc.Metadata)
		if cloneErr != nil {
			return nil, ErrStorage
		}
		result[doc.ID] = vectorstore.DocumentMetadata{Metadata: metadata}
	}
	return result, nil
}

// Close marks this adapter closed but deliberately leaves the borrowed SQL
// pool open. Close is idempotent.
func (store *Store) Close() error {
	if store == nil {
		return nil
	}
	store.mu.Lock()
	store.closed = true
	store.mu.Unlock()
	return nil
}

type storedDocument struct {
	doc       *document.Document
	embedding []float64
}

type rowScanner interface {
	Scan(dest ...any) error
}

func (store *Store) listDocuments(ctx context.Context) ([]storedDocument, error) {
	rows, err := store.db.QueryContext(ctx, `SELECT document_id, name, content, embedding_text, metadata, embedding, created_at, updated_at
		FROM public.runtime_knowledge_document WHERE tenant_id = $1`, store.tenantID)
	if err != nil {
		return nil, mapError(ctx, err)
	}
	defer rows.Close()
	values := make([]storedDocument, 0)
	for rows.Next() {
		value, scanErr := scanDocument(rows, store.dimension)
		if scanErr != nil {
			return nil, mapError(ctx, scanErr)
		}
		values = append(values, value)
	}
	if err := rows.Err(); err != nil {
		return nil, mapError(ctx, err)
	}
	return values, nil
}

func scanDocument(scanner rowScanner, dimension int) (storedDocument, error) {
	var (
		id, name, content, embeddingText string
		metadataRaw, embeddingRaw        []byte
		createdAt, updatedAt             time.Time
	)
	if err := scanner.Scan(&id, &name, &content, &embeddingText, &metadataRaw, &embeddingRaw, &createdAt, &updatedAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return storedDocument{}, ErrNotFound
		}
		return storedDocument{}, err
	}
	metadata := map[string]any{}
	if len(metadataRaw) > 0 {
		if err := json.Unmarshal(metadataRaw, &metadata); err != nil || metadata == nil {
			return storedDocument{}, ErrStorage
		}
	}
	var embedding []float64
	if err := json.Unmarshal(embeddingRaw, &embedding); err != nil || !validDimension(len(embedding), dimension) {
		return storedDocument{}, ErrStorage
	}
	return storedDocument{
		doc:       &document.Document{ID: id, Name: name, Content: content, EmbeddingText: embeddingText, Metadata: metadata, CreatedAt: createdAt, UpdatedAt: updatedAt},
		embedding: embedding,
	}, nil
}

func (store *Store) deleteIDs(ctx context.Context, ids []string) error {
	if len(ids) == 0 {
		return nil
	}
	placeholders := make([]string, len(ids))
	args := make([]any, 0, len(ids)+1)
	args = append(args, store.tenantID)
	for index, id := range ids {
		placeholders[index] = fmt.Sprintf("$%d", index+2)
		args = append(args, id)
	}
	query := `DELETE FROM public.runtime_knowledge_document WHERE tenant_id = $1 AND document_id IN (` + strings.Join(placeholders, ",") + ")"
	if _, err := store.db.ExecContext(ctx, query, args...); err != nil {
		return mapError(ctx, err)
	}
	return nil
}

func (store *Store) check(ctx context.Context) error {
	if ctx == nil {
		return ErrInvalid
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if store == nil || store.db == nil || strings.TrimSpace(store.tenantID) == "" {
		return ErrInvalid
	}
	store.mu.RLock()
	closed := store.closed
	store.mu.RUnlock()
	if closed {
		return ErrClosed
	}
	return nil
}

func normalizeDocument(doc *document.Document, embedding []float64, dimension int) ([]byte, []byte, *document.Document, error) {
	if doc == nil || strings.TrimSpace(doc.ID) == "" || !validDimension(len(embedding), dimension) {
		return nil, nil, nil, ErrInvalid
	}
	value := doc.Clone()
	now := time.Now().UTC()
	if value.CreatedAt.IsZero() {
		value.CreatedAt = now
	}
	if value.UpdatedAt.IsZero() {
		value.UpdatedAt = now
	}
	metadata, err := json.Marshal(value.Metadata)
	if err != nil {
		return nil, nil, nil, ErrInvalid
	}
	if value.Metadata == nil {
		metadata = []byte(`{}`)
	}
	vector, err := json.Marshal(embedding)
	if err != nil {
		return nil, nil, nil, ErrInvalid
	}
	return metadata, vector, value, nil
}

func validDimension(got, expected int) bool {
	return got > 0 && got <= maxDimension && (expected == 0 || got == expected)
}

func mapError(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}
	if ctx != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
	}
	if errors.Is(err, ErrInvalid) || errors.Is(err, ErrNotFound) || errors.Is(err, ErrConflict) || errors.Is(err, ErrStorage) {
		return err
	}
	if errors.Is(err, sql.ErrNoRows) {
		return ErrNotFound
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch pgErr.Code {
		case "23505", "40001", "40P01":
			return ErrConflict
		case "23503", "23514", "22P02", "22001":
			return ErrInvalid
		}
	}
	return ErrStorage
}

func contains(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

func matchesMetadata(metadata map[string]any, filter map[string]any) bool {
	for key, expected := range filter {
		actual, ok := metadata[key]
		if !ok || !jsonEqual(actual, expected) {
			return false
		}
	}
	return true
}

func matchesFilter(doc *document.Document, filter *vectorstore.SearchFilter) (bool, error) {
	if filter == nil {
		return true, nil
	}
	if len(filter.IDs) > 0 && !contains(filter.IDs, doc.ID) {
		return false, nil
	}
	if !matchesMetadata(doc.Metadata, filter.Metadata) {
		return false, nil
	}
	if filter.FilterCondition == nil {
		return true, nil
	}
	return evaluateCondition(doc, filter.FilterCondition)
}

func evaluateCondition(doc *document.Document, condition *searchfilter.UniversalFilterCondition) (bool, error) {
	if condition == nil {
		return false, ErrInvalid
	}
	switch strings.ToLower(condition.Operator) {
	case searchfilter.OperatorAnd, searchfilter.OperatorOr:
		conditions, err := conditionList(condition.Value)
		if err != nil || len(conditions) == 0 {
			return false, ErrInvalid
		}
		wantAnd := strings.EqualFold(condition.Operator, searchfilter.OperatorAnd)
		for _, child := range conditions {
			value, childErr := evaluateCondition(doc, child)
			if childErr != nil {
				return false, childErr
			}
			if wantAnd && !value {
				return false, nil
			}
			if !wantAnd && value {
				return true, nil
			}
		}
		return wantAnd, nil
	case searchfilter.OperatorIn, searchfilter.OperatorNotIn:
		field, ok := fieldValue(doc, condition.Field)
		if !ok {
			return false, nil
		}
		values, ok := sliceValue(condition.Value)
		if !ok || len(values) == 0 {
			return false, ErrInvalid
		}
		found := false
		for _, candidate := range values {
			if compareValue(field, candidate, searchfilter.OperatorEqual) {
				found = true
				break
			}
		}
		if condition.Operator == searchfilter.OperatorNotIn {
			return !found, nil
		}
		return found, nil
	case searchfilter.OperatorBetween:
		values, ok := sliceValue(condition.Value)
		if !ok || len(values) != 2 {
			return false, ErrInvalid
		}
		field, ok := fieldValue(doc, condition.Field)
		if !ok {
			return false, nil
		}
		return compareValue(field, values[0], searchfilter.OperatorGreaterThanOrEqual) && compareValue(field, values[1], searchfilter.OperatorLessThanOrEqual), nil
	case searchfilter.OperatorLike, searchfilter.OperatorNotLike:
		field, ok := fieldValue(doc, condition.Field)
		pattern, patternOK := condition.Value.(string)
		if !ok || !patternOK {
			return false, ErrInvalid
		}
		value, valueOK := field.(string)
		if !valueOK {
			return false, nil
		}
		matched, err := regexp.MatchString("^"+strings.ReplaceAll(strings.ReplaceAll(regexp.QuoteMeta(pattern), "%", ".*"), "_", ".")+"$", value)
		if err != nil {
			return false, ErrInvalid
		}
		if condition.Operator == searchfilter.OperatorNotLike {
			return !matched, nil
		}
		return matched, nil
	case searchfilter.OperatorEqual, searchfilter.OperatorNotEqual,
		searchfilter.OperatorGreaterThan, searchfilter.OperatorGreaterThanOrEqual,
		searchfilter.OperatorLessThan, searchfilter.OperatorLessThanOrEqual:
		field, ok := fieldValue(doc, condition.Field)
		if !ok {
			return false, nil
		}
		return compareValue(field, condition.Value, strings.ToLower(condition.Operator)), nil
	default:
		return false, ErrInvalid
	}
}

func conditionList(value any) ([]*searchfilter.UniversalFilterCondition, error) {
	switch values := value.(type) {
	case []*searchfilter.UniversalFilterCondition:
		return values, nil
	case []searchfilter.UniversalFilterCondition:
		result := make([]*searchfilter.UniversalFilterCondition, len(values))
		for index := range values {
			copyValue := values[index]
			result[index] = &copyValue
		}
		return result, nil
	case []any:
		result := make([]*searchfilter.UniversalFilterCondition, 0, len(values))
		for _, value := range values {
			payload, err := json.Marshal(value)
			if err != nil {
				return nil, err
			}
			var condition searchfilter.UniversalFilterCondition
			if err := json.Unmarshal(payload, &condition); err != nil {
				return nil, err
			}
			result = append(result, &condition)
		}
		return result, nil
	default:
		return nil, ErrInvalid
	}
}

func sliceValue(value any) ([]any, bool) {
	if values, ok := value.([]any); ok {
		return values, true
	}
	reflected := reflect.ValueOf(value)
	if !reflected.IsValid() || (reflected.Kind() != reflect.Slice && reflected.Kind() != reflect.Array) {
		return nil, false
	}
	values := make([]any, reflected.Len())
	for index := range values {
		values[index] = reflected.Index(index).Interface()
	}
	return values, true
}

func fieldValue(doc *document.Document, field string) (any, bool) {
	if doc == nil {
		return nil, false
	}
	switch field {
	case "id":
		return doc.ID, true
	case "name":
		return doc.Name, true
	case "content":
		return doc.Content, true
	case "created_at":
		return doc.CreatedAt, true
	case "updated_at":
		return doc.UpdatedAt, true
	case "metadata":
		return doc.Metadata, true
	}
	if strings.HasPrefix(field, source.MetadataFieldPrefix) {
		value, ok := doc.Metadata[strings.TrimPrefix(field, source.MetadataFieldPrefix)]
		return value, ok
	}
	return nil, false
}

func compareValue(left, right any, operator string) bool {
	if leftTime, ok := asTime(left); ok {
		if rightTime, rightOK := asTime(right); rightOK {
			switch operator {
			case searchfilter.OperatorEqual:
				return leftTime.Equal(rightTime)
			case searchfilter.OperatorNotEqual:
				return !leftTime.Equal(rightTime)
			case searchfilter.OperatorGreaterThan:
				return leftTime.After(rightTime)
			case searchfilter.OperatorGreaterThanOrEqual:
				return leftTime.After(rightTime) || leftTime.Equal(rightTime)
			case searchfilter.OperatorLessThan:
				return leftTime.Before(rightTime)
			case searchfilter.OperatorLessThanOrEqual:
				return leftTime.Before(rightTime) || leftTime.Equal(rightTime)
			}
		}
	}
	if leftNumber, ok := asNumber(left); ok {
		if rightNumber, rightOK := asNumber(right); rightOK {
			switch operator {
			case searchfilter.OperatorEqual:
				return leftNumber == rightNumber
			case searchfilter.OperatorNotEqual:
				return leftNumber != rightNumber
			case searchfilter.OperatorGreaterThan:
				return leftNumber > rightNumber
			case searchfilter.OperatorGreaterThanOrEqual:
				return leftNumber >= rightNumber
			case searchfilter.OperatorLessThan:
				return leftNumber < rightNumber
			case searchfilter.OperatorLessThanOrEqual:
				return leftNumber <= rightNumber
			}
		}
	}
	if leftString, ok := left.(string); ok {
		if rightString, rightOK := right.(string); rightOK {
			switch operator {
			case searchfilter.OperatorEqual:
				return leftString == rightString
			case searchfilter.OperatorNotEqual:
				return leftString != rightString
			case searchfilter.OperatorGreaterThan:
				return leftString > rightString
			case searchfilter.OperatorGreaterThanOrEqual:
				return leftString >= rightString
			case searchfilter.OperatorLessThan:
				return leftString < rightString
			case searchfilter.OperatorLessThanOrEqual:
				return leftString <= rightString
			}
		}
	}
	if operator == searchfilter.OperatorEqual || operator == searchfilter.OperatorNotEqual {
		equal := jsonEqual(left, right)
		if operator == searchfilter.OperatorNotEqual {
			return !equal
		}
		return equal
	}
	return false
}

func asNumber(value any) (float64, bool) {
	reflected := reflect.ValueOf(value)
	if !reflected.IsValid() {
		return 0, false
	}
	switch reflected.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return float64(reflected.Int()), true
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return float64(reflected.Uint()), true
	case reflect.Float32, reflect.Float64:
		value := reflected.Float()
		return value, !math.IsNaN(value) && !math.IsInf(value, 0)
	default:
		return 0, false
	}
}

func asTime(value any) (time.Time, bool) {
	if result, ok := value.(time.Time); ok {
		return result, true
	}
	if text, ok := value.(string); ok {
		result, err := time.Parse(time.RFC3339Nano, text)
		return result, err == nil
	}
	return time.Time{}, false
}

func cosineSimilarity(left, right []float64) float64 {
	if len(left) == 0 || len(left) != len(right) {
		return 0
	}
	var dot, leftNorm, rightNorm float64
	for index := range left {
		dot += left[index] * right[index]
		leftNorm += left[index] * left[index]
		rightNorm += right[index] * right[index]
	}
	if leftNorm == 0 || rightNorm == 0 {
		return 0
	}
	return dot / (math.Sqrt(leftNorm) * math.Sqrt(rightNorm))
}

func jsonEqual(left, right any) bool {
	leftValue, leftErr := normalizeJSONValue(left)
	rightValue, rightErr := normalizeJSONValue(right)
	return leftErr == nil && rightErr == nil && reflect.DeepEqual(leftValue, rightValue)
}

func normalizeJSONValue(value any) (any, error) {
	payload, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	var normalized any
	if err := json.Unmarshal(payload, &normalized); err != nil {
		return nil, err
	}
	return normalized, nil
}

func cloneJSONMap(value map[string]any) (map[string]any, error) {
	if value == nil {
		return map[string]any{}, nil
	}
	cloned, err := normalizeJSONValue(value)
	if err != nil {
		return nil, err
	}
	result, ok := cloned.(map[string]any)
	if !ok {
		return nil, ErrStorage
	}
	return result, nil
}

func validateUpdateKeys(updates map[string]any, dimension int) error {
	for field, value := range updates {
		switch field {
		case "name", "content":
			if _, ok := value.(string); !ok {
				return ErrInvalid
			}
		case "embedding":
			if _, err := embeddingValue(value, dimension); err != nil {
				return err
			}
		default:
			if !strings.HasPrefix(field, source.MetadataFieldPrefix) || strings.TrimPrefix(field, source.MetadataFieldPrefix) == "" {
				return ErrInvalid
			}
			if _, err := json.Marshal(value); err != nil {
				return ErrInvalid
			}
		}
	}
	return nil
}

func applyUpdates(doc *document.Document, embedding []float64, updates map[string]any, dimension int) (*document.Document, []float64, error) {
	candidate := doc.Clone()
	candidate.Metadata, _ = cloneJSONMap(candidate.Metadata)
	vector := append([]float64(nil), embedding...)
	for field, value := range updates {
		switch field {
		case "name":
			candidate.Name = value.(string)
		case "content":
			candidate.Content = value.(string)
		case "embedding":
			var err error
			vector, err = embeddingValue(value, dimension)
			if err != nil {
				return nil, nil, err
			}
		default:
			candidate.Metadata[strings.TrimPrefix(field, source.MetadataFieldPrefix)] = value
		}
	}
	return candidate, vector, nil
}

func embeddingValue(value any, dimension int) ([]float64, error) {
	reflected := reflect.ValueOf(value)
	if !reflected.IsValid() || (reflected.Kind() != reflect.Slice && reflected.Kind() != reflect.Array) {
		return nil, ErrInvalid
	}
	result := make([]float64, reflected.Len())
	for index := range result {
		number, ok := asNumber(reflected.Index(index).Interface())
		if !ok {
			return nil, ErrInvalid
		}
		result[index] = number
	}
	if !validDimension(len(result), dimension) {
		return nil, ErrInvalid
	}
	return result, nil
}
