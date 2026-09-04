// Package pgvector provides a tenant-scoped PostgreSQL/pgvector knowledge
// provider. Source documents are durable in PostgreSQL; vector rows are an
// eventually consistent projection maintained by an owned bounded worker.
package pgvector

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	runtimestorage "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage"
	pgstorage "github.com/XnLemon/trpc-agent-service/trpcservice/storage/postgres"
)

const (
	defaultSchema       = "public"
	defaultCollection   = "knowledge"
	defaultModel        = "deterministic"
	defaultModelVersion = "v1"
	defaultDimension    = 32
	defaultQueueSize    = 128
	defaultWorkers      = 1
	maxLimit            = 100
)

// IndexStatus describes whether a durable source document is searchable.
type IndexStatus string

const (
	// IndexPending means the source is durable but has not been indexed.
	IndexPending IndexStatus = "pending"
	// IndexReady means the index matches the source document version.
	IndexReady IndexStatus = "ready"
	// IndexFailed means the last embedding or index attempt failed.
	IndexFailed IndexStatus = "failed"
	// IndexDeleted means the source was deleted and must never be returned.
	IndexDeleted IndexStatus = "deleted"
	// IndexDeadLetter means bounded retries were exhausted and operator repair
	// is required before the document can become searchable.
	IndexDeadLetter IndexStatus = "dead_letter"
)

var (
	// ErrClosed reports an operation on a closed provider.
	ErrClosed = errors.New("pgvector provider is closed")
	// ErrEmbedding reports an embedding or index handoff failure.
	ErrEmbedding = errors.New("embedding provider failed")
	// ErrIncompatible reports a model or vector dimension mismatch.
	ErrIncompatible = errors.New("incompatible embedding")
)

// Embedder is the provider-owned embedding boundary. Implementations must not
// select a network endpoint or credentials from tenant document data.
type Embedder interface {
	Embed(context.Context, string) ([]float64, error)
}

// EmbeddingClient is the explicit client-oriented alias for Embedder.
type EmbeddingClient = Embedder

// EmbeddingFunc adapts a function to Embedder.
type EmbeddingFunc func(context.Context, string) ([]float64, error)

// Embed implements Embedder.
func (f EmbeddingFunc) Embed(ctx context.Context, text string) ([]float64, error) {
	return f(ctx, text)
}

// DeterministicEmbedder produces stable local vectors for CI and development.
// It has no network or credential dependencies.
type DeterministicEmbedder struct{ dimension int }

// NewDeterministicEmbedder creates a deterministic embedder of the requested
// dimension. Non-positive dimensions use the provider default.
func NewDeterministicEmbedder(dimension int) DeterministicEmbedder {
	if dimension <= 0 {
		dimension = defaultDimension
	}
	return DeterministicEmbedder{dimension: dimension}
}

// Embed hashes text into a normalized deterministic vector.
func (e DeterministicEmbedder) Embed(ctx context.Context, text string) ([]float64, error) {
	if ctx == nil {
		return nil, context.Canceled
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	dimension := e.dimension
	if dimension <= 0 {
		dimension = defaultDimension
	}
	result := make([]float64, dimension)
	for offset := 0; offset < dimension; offset += sha256.Size {
		digest := sha256.Sum256([]byte(strconv.Itoa(offset) + ":" + text))
		for index := 0; index < sha256.Size && offset+index < dimension; index++ {
			result[offset+index] = (float64(digest[index]) - 127.5) / 127.5
		}
	}
	return result, nil
}

// Config controls a tenant-bound provider. DB is supplied to New so the
// environment retains ownership of the shared PostgreSQL pool.
type Config struct {
	Schema           string
	Collection       string
	EmbeddingModel   string
	EmbeddingVersion string
	Dimension        int
	QueueSize        int
	Workers          int
	MaxAttempts      int
	Embedder         Embedder
}

// DocumentInput contains source metadata for one stable document chunk.
// Tenant identity is supplied by the Store and cannot be selected by callers.
type DocumentInput struct {
	KnowledgeID      string
	DocumentID       string
	ChunkID          string
	Source           string
	SourceVersion    string
	Content          string
	ContentRef       string
	Metadata         map[string]any
	Embedding        []float64
	EmbeddingModel   string
	EmbeddingVersion string
	Checksum         string
}

// Document is the durable source and its current index handoff state.
type Document struct {
	TenantID           string
	KnowledgeID        string
	DocumentID         string
	ChunkID            string
	Source             string
	SourceVersion      string
	Content            string
	ContentRef         string
	Metadata           map[string]any
	EmbeddingModel     string
	EmbeddingVersion   string
	EmbeddingDimension int
	Checksum           string
	Version            int64
	IndexStatus        IndexStatus
	Attempts           int
	LastErrorClass     string
	DeletedAt          *time.Time
	CreatedAt          time.Time
	UpdatedAt          time.Time
}

// SearchOptions bounds retrieval and applies trusted authorization metadata
// filters before similarity ranking. Filter keys are matched against JSON
// metadata values and are never interpreted as SQL.
type SearchOptions struct {
	Limit     int
	MinScore  float64
	Metadata  map[string]string
	Authorize func(Document) bool
}

// SearchResult is one authorized, ready document ordered by cosine score.
type SearchResult struct {
	Document Document
	Score    float64
}

// Store implements the existing tenant-scoped KnowledgeStore and VectorStore
// contracts while exposing richer document lifecycle methods.
type Store struct {
	db          *sql.DB
	tenantID    string
	schema      string
	collection  string
	model       string
	modelVer    string
	dimension   int
	embedder    Embedder
	queue       chan indexJob
	stop        chan struct{}
	done        chan struct{}
	closeOnce   sync.Once
	closeErr    error
	workers     int
	maxAttempts int
}

type indexJob struct{ key documentKey }
type documentKey struct{ knowledgeID, documentID, chunkID string }

// New creates a tenant-bound provider and starts its bounded index workers.
// The caller retains ownership of db and owns Store.Close.
func New(db *sql.DB, tenantID string, config Config) (*Store, error) {
	if db == nil || runtimestorage.ValidateTenant(tenantID) != nil {
		return nil, runtimestorage.ErrInvalid
	}
	config.Schema = strings.TrimSpace(config.Schema)
	if config.Schema == "" {
		config.Schema = defaultSchema
	}
	config.Collection = strings.TrimSpace(config.Collection)
	if config.Collection == "" {
		config.Collection = defaultCollection
	}
	if !validIdentifier(config.Schema) || !validIdentifier(config.Collection) {
		return nil, runtimestorage.ErrInvalid
	}
	if config.EmbeddingModel == "" {
		config.EmbeddingModel = defaultModel
	}
	if config.EmbeddingVersion == "" {
		config.EmbeddingVersion = defaultModelVersion
	}
	if !validModelValue(config.EmbeddingModel) || !validModelValue(config.EmbeddingVersion) {
		return nil, runtimestorage.ErrInvalid
	}
	if config.Dimension <= 0 {
		config.Dimension = defaultDimension
	}
	if config.Dimension > 4096 {
		return nil, runtimestorage.ErrInvalid
	}
	if config.QueueSize <= 0 {
		config.QueueSize = defaultQueueSize
	}
	if config.QueueSize > 10000 {
		return nil, runtimestorage.ErrInvalid
	}
	if config.Workers <= 0 {
		config.Workers = defaultWorkers
	}
	if config.Workers > 32 {
		return nil, runtimestorage.ErrInvalid
	}
	if config.Embedder == nil {
		config.Embedder = NewDeterministicEmbedder(config.Dimension)
	}
	if config.MaxAttempts <= 0 {
		config.MaxAttempts = 3
	}
	if config.MaxAttempts > 100 {
		return nil, runtimestorage.ErrInvalid
	}
	store := &Store{db: db, tenantID: tenantID, schema: config.Schema, collection: config.Collection, model: config.EmbeddingModel, modelVer: config.EmbeddingVersion, dimension: config.Dimension, embedder: config.Embedder, queue: make(chan indexJob, config.QueueSize), stop: make(chan struct{}), done: make(chan struct{}), workers: config.Workers, maxAttempts: config.MaxAttempts}
	var wg sync.WaitGroup
	for index := 0; index < config.Workers; index++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			store.worker()
		}()
	}
	go func() {
		wg.Wait()
		close(store.done)
	}()
	return store, nil
}

// Close stops owned workers. It does not close the caller-owned database.
func (s *Store) Close() error {
	if s == nil {
		return nil
	}
	s.closeOnce.Do(func() {
		close(s.stop)
		<-s.done
	})
	return s.closeErr
}

// Ping checks the shared PostgreSQL readiness boundary.
func (s *Store) Ping(ctx context.Context) error {
	if err := s.check(ctx); err != nil {
		return err
	}
	if err := s.db.PingContext(ctx); err != nil {
		return runtimestorage.ErrStorage
	}
	return nil
}

func (s *Store) check(ctx context.Context) error {
	if ctx == nil {
		return runtimestorage.ErrInvalid
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if s == nil || s.db == nil {
		return runtimestorage.ErrStorage
	}
	select {
	case <-s.stop:
		return ErrClosed
	default:
		return nil
	}
}

func validIdentifier(value string) bool {
	if len(value) == 0 || len(value) > 63 {
		return false
	}
	for index, character := range value {
		if (character >= 'a' && character <= 'z') || (character >= 'A' && character <= 'Z') || (index > 0 && character >= '0' && character <= '9') || character == '_' {
			continue
		}
		return false
	}
	return true
}

func validModelValue(value string) bool {
	if len(value) == 0 || len(value) > 256 {
		return false
	}
	for _, character := range value {
		if (character >= 'a' && character <= 'z') || (character >= 'A' && character <= 'Z') || (character >= '0' && character <= '9') || strings.ContainsRune("_-.:/", character) {
			continue
		}
		return false
	}
	return true
}

func validKey(value string, required bool) bool {
	trimmed := strings.TrimSpace(value)
	if required && trimmed == "" {
		return false
	}
	return len([]rune(value)) <= 256 && !strings.ContainsAny(value, "\x00\r\n")
}

func (s *Store) table(name string) string { return s.schema + ".runtime_pgvector_" + name }

func encodeVector(values []float64) (string, error) {
	if len(values) == 0 || !runtimestorage.ValidateEmbedding(values) {
		return "", runtimestorage.ErrInvalid
	}
	parts := make([]string, len(values))
	for index, value := range values {
		parts[index] = strconv.FormatFloat(value, 'g', -1, 64)
	}
	return "[" + strings.Join(parts, ",") + "]", nil
}

func decodeVector(raw string) ([]float64, error) {
	raw = strings.TrimSpace(raw)
	if strings.HasPrefix(raw, "[") && strings.HasSuffix(raw, "]") {
		raw = strings.TrimSpace(raw[1 : len(raw)-1])
	}
	if raw == "" {
		return nil, runtimestorage.ErrInvalid
	}
	parts := strings.Split(raw, ",")
	result := make([]float64, len(parts))
	for index, part := range parts {
		value, err := strconv.ParseFloat(strings.TrimSpace(part), 64)
		if err != nil || math.IsNaN(value) || math.IsInf(value, 0) {
			return nil, runtimestorage.ErrStorage
		}
		result[index] = value
	}
	return result, nil
}

func cloneMap(input map[string]any) map[string]any {
	if input == nil {
		return map[string]any{}
	}
	data, err := json.Marshal(input)
	if err != nil {
		return nil
	}
	var output map[string]any
	if json.Unmarshal(data, &output) != nil {
		return nil
	}
	return output
}

func cloneDocument(value Document) Document {
	value.Metadata = cloneMap(value.Metadata)
	if value.DeletedAt != nil {
		deleted := *value.DeletedAt
		value.DeletedAt = &deleted
	}
	return value
}

func (s *Store) enqueue(ctx context.Context, key documentKey) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-s.stop:
		return ErrClosed
	case s.queue <- indexJob{key: key}:
		return nil
	default:
		return runtimestorage.ErrConflict
	}
}

// Upsert durably records a source document before handing it to the index
// worker. Replays of the same key are idempotent and increment its version.
func (s *Store) Upsert(ctx context.Context, input DocumentInput) (Document, error) {
	if err := s.check(ctx); err != nil {
		return Document{}, err
	}
	if !validKey(input.KnowledgeID, true) || !validKey(input.DocumentID, true) || !validKey(input.ChunkID, true) || !validKey(input.Source, false) || !validKey(input.SourceVersion, false) || strings.TrimSpace(input.Content) == "" || len(input.Content) > 16<<20 || !validKey(input.ContentRef, false) || !runtimestorage.ValidateEmbedding(input.Embedding) {
		return Document{}, runtimestorage.ErrInvalid
	}
	metadata := cloneMap(input.Metadata)
	if metadata == nil {
		return Document{}, runtimestorage.ErrInvalid
	}
	model, modelVersion := input.EmbeddingModel, input.EmbeddingVersion
	if model != "" && model != s.model || modelVersion != "" && modelVersion != s.modelVer {
		return Document{}, ErrIncompatible
	}
	if model == "" {
		model = s.model
	}
	if modelVersion == "" {
		modelVersion = s.modelVer
	}
	if !validModelValue(model) || !validModelValue(modelVersion) {
		return Document{}, runtimestorage.ErrInvalid
	}
	if len(input.Embedding) > 0 && len(input.Embedding) != s.dimension {
		return Document{}, ErrIncompatible
	}
	checksum := strings.TrimSpace(input.Checksum)
	if checksum == "" {
		digest := sha256.Sum256([]byte(input.Content))
		checksum = hex.EncodeToString(digest[:])
	}
	if !validKey(checksum, true) {
		return Document{}, runtimestorage.ErrInvalid
	}
	metadataJSON, err := json.Marshal(metadata)
	if err != nil {
		return Document{}, runtimestorage.ErrInvalid
	}
	status := string(IndexPending)
	if len(input.Embedding) > 0 {
		status = string(IndexPending)
	}
	vector := ""
	if len(input.Embedding) > 0 {
		vector, err = encodeVector(input.Embedding)
		if err != nil {
			return Document{}, err
		}
	}
	key := documentKey{knowledgeID: input.KnowledgeID, documentID: input.DocumentID, chunkID: input.ChunkID}
	query := fmt.Sprintf("INSERT INTO %s (tenant_id,collection,knowledge_id,document_id,chunk_id,source,source_version,content,content_ref,metadata,embedding_model,embedding_version,embedding_dimension,checksum,index_status,embedding,version) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,NULLIF($16,'' )::vector,1) ON CONFLICT (tenant_id,collection,knowledge_id,document_id,chunk_id) DO UPDATE SET source=EXCLUDED.source,source_version=EXCLUDED.source_version,content=EXCLUDED.content,content_ref=EXCLUDED.content_ref,metadata=EXCLUDED.metadata,embedding_model=EXCLUDED.embedding_model,embedding_version=EXCLUDED.embedding_version,embedding_dimension=EXCLUDED.embedding_dimension,checksum=EXCLUDED.checksum,index_status=$15,embedding=NULLIF($16,'')::vector,version=%s.version+1,attempts=0,last_error_class='',deleted_at=NULL,updated_at=now() RETURNING tenant_id,knowledge_id,document_id,chunk_id,source,source_version,content,content_ref,metadata,embedding_model,embedding_version,embedding_dimension,checksum,version,index_status,attempts,last_error_class,deleted_at,created_at,updated_at", s.table("documents"), s.table("documents"))
	var value Document
	var metadataRaw []byte
	var embeddingDimension int
	var deletedAt sql.NullTime
	var embeddingModel, embeddingVersion string
	err = s.db.QueryRowContext(ctx, query, s.tenantID, s.collection, key.knowledgeID, key.documentID, key.chunkID, input.Source, input.SourceVersion, input.Content, input.ContentRef, metadataJSON, model, modelVersion, s.dimension, checksum, status, vector).Scan(&value.TenantID, &value.KnowledgeID, &value.DocumentID, &value.ChunkID, &value.Source, &value.SourceVersion, &value.Content, &value.ContentRef, &metadataRaw, &embeddingModel, &embeddingVersion, &embeddingDimension, &value.Checksum, &value.Version, &value.IndexStatus, &value.Attempts, &value.LastErrorClass, &deletedAt, &value.CreatedAt, &value.UpdatedAt)
	if err != nil {
		return Document{}, pgstorage.MapError(ctx, err, runtimestorage.ErrNotFound, runtimestorage.ErrDuplicate, runtimestorage.ErrConflict, runtimestorage.ErrInvalid)
	}
	if err := json.Unmarshal(metadataRaw, &value.Metadata); err != nil {
		return Document{}, runtimestorage.ErrStorage
	}
	value.EmbeddingModel, value.EmbeddingVersion, value.EmbeddingDimension = embeddingModel, embeddingVersion, embeddingDimension
	if deletedAt.Valid {
		value.DeletedAt = &deletedAt.Time
	}
	if err := s.enqueue(ctx, key); err != nil {
		return cloneDocument(value), err
	}
	return cloneDocument(value), nil
}

// PutDocument is the explicit document-oriented name for Upsert.
func (s *Store) PutDocument(ctx context.Context, input DocumentInput) (Document, error) {
	return s.Upsert(ctx, input)
}

// Get returns one source document, including pending or failed state.
func (s *Store) Get(ctx context.Context, knowledgeID, documentID, chunkID string) (Document, error) {
	if err := s.check(ctx); err != nil {
		return Document{}, err
	}
	if !validKey(knowledgeID, true) || !validKey(documentID, true) || !validKey(chunkID, true) {
		return Document{}, runtimestorage.ErrInvalid
	}
	query := fmt.Sprintf("SELECT tenant_id,knowledge_id,document_id,chunk_id,source,source_version,content,content_ref,metadata,embedding_model,embedding_version,embedding_dimension,checksum,version,index_status,attempts,last_error_class,deleted_at,created_at,updated_at FROM %s WHERE tenant_id=$1 AND collection=$2 AND knowledge_id=$3 AND document_id=$4 AND chunk_id=$5", s.table("documents"))
	return s.scanDocument(ctx, s.db.QueryRowContext(ctx, query, s.tenantID, s.collection, knowledgeID, documentID, chunkID))
}

// GetDocument is the explicit document-oriented name for Get.
func (s *Store) GetDocument(ctx context.Context, knowledgeID, documentID, chunkID string) (Document, error) {
	return s.Get(ctx, knowledgeID, documentID, chunkID)
}

// Search applies tenant and readiness boundaries in SQL, then applies trusted
// authorization filters before ranking and limiting results.
func (s *Store) Search(ctx context.Context, embedding []float64, options SearchOptions) ([]SearchResult, error) {
	if err := s.check(ctx); err != nil {
		return nil, err
	}
	if len(embedding) != s.dimension || !runtimestorage.ValidateEmbedding(embedding) || options.Limit < 0 || options.Limit > maxLimit || math.IsNaN(options.MinScore) || math.IsInf(options.MinScore, 0) {
		return nil, runtimestorage.ErrInvalid
	}
	for key, value := range options.Metadata {
		if !validKey(key, true) || !validKey(value, false) {
			return nil, runtimestorage.ErrInvalid
		}
	}
	query := fmt.Sprintf("SELECT tenant_id,knowledge_id,document_id,chunk_id,source,source_version,content,content_ref,metadata,embedding_model,embedding_version,embedding_dimension,checksum,version,index_status,attempts,last_error_class,deleted_at,created_at,updated_at,embedding::text FROM %s WHERE tenant_id=$1 AND collection=$2 AND index_status='ready' AND deleted_at IS NULL AND embedding IS NOT NULL", s.table("documents"))
	rows, err := s.db.QueryContext(ctx, query, s.tenantID, s.collection)
	if err != nil {
		return nil, pgstorage.MapError(ctx, err, runtimestorage.ErrNotFound, runtimestorage.ErrDuplicate, runtimestorage.ErrConflict, runtimestorage.ErrInvalid)
	}
	defer rows.Close()
	values := make([]SearchResult, 0)
	for rows.Next() {
		var value Document
		var metadataRaw []byte
		var deletedAt sql.NullTime
		var vectorRaw string
		if err := rows.Scan(&value.TenantID, &value.KnowledgeID, &value.DocumentID, &value.ChunkID, &value.Source, &value.SourceVersion, &value.Content, &value.ContentRef, &metadataRaw, &value.EmbeddingModel, &value.EmbeddingVersion, &value.EmbeddingDimension, &value.Checksum, &value.Version, &value.IndexStatus, &value.Attempts, &value.LastErrorClass, &deletedAt, &value.CreatedAt, &value.UpdatedAt, &vectorRaw); err != nil || json.Unmarshal(metadataRaw, &value.Metadata) != nil {
			return nil, runtimestorage.ErrStorage
		}
		if deletedAt.Valid {
			value.DeletedAt = &deletedAt.Time
		}
		if value.TenantID != s.tenantID || !metadataMatches(value.Metadata, options.Metadata) || (options.Authorize != nil && !options.Authorize(cloneDocument(value))) {
			continue
		}
		vector, err := decodeVector(vectorRaw)
		if err != nil || len(vector) != s.dimension {
			continue
		}
		score, ok := cosine(embedding, vector)
		if ok && score >= options.MinScore {
			values = append(values, SearchResult{Document: cloneDocument(value), Score: score})
		}
	}
	if err := rows.Err(); err != nil {
		return nil, runtimestorage.ErrStorage
	}
	sort.Slice(values, func(i, j int) bool {
		if values[i].Score == values[j].Score {
			if values[i].Document.DocumentID == values[j].Document.DocumentID {
				return values[i].Document.ChunkID < values[j].Document.ChunkID
			}
			return values[i].Document.DocumentID < values[j].Document.DocumentID
		}
		return values[i].Score > values[j].Score
	})
	if options.Limit > 0 && len(values) > options.Limit {
		values = values[:options.Limit]
	}
	return values, nil
}

// SearchDocuments is the explicit document-oriented name for Search.
func (s *Store) SearchDocuments(ctx context.Context, embedding []float64, options SearchOptions) ([]SearchResult, error) {
	return s.Search(ctx, embedding, options)
}

func metadataMatches(metadata map[string]any, filters map[string]string) bool {
	for key, expected := range filters {
		value, ok := metadata[key]
		if !ok || fmt.Sprint(value) != expected {
			return false
		}
	}
	return true
}

func cosine(left, right []float64) (float64, bool) {
	if len(left) != len(right) || len(left) == 0 {
		return 0, false
	}
	var dot, leftNorm, rightNorm float64
	for index := range left {
		dot += left[index] * right[index]
		leftNorm += left[index] * left[index]
		rightNorm += right[index] * right[index]
	}
	if leftNorm == 0 || rightNorm == 0 {
		return 0, false
	}
	return dot / (math.Sqrt(leftNorm) * math.Sqrt(rightNorm)), true
}

func (s *Store) scanDocument(ctx context.Context, row *sql.Row) (Document, error) {
	var value Document
	var metadataRaw []byte
	var deletedAt sql.NullTime
	if err := row.Scan(&value.TenantID, &value.KnowledgeID, &value.DocumentID, &value.ChunkID, &value.Source, &value.SourceVersion, &value.Content, &value.ContentRef, &metadataRaw, &value.EmbeddingModel, &value.EmbeddingVersion, &value.EmbeddingDimension, &value.Checksum, &value.Version, &value.IndexStatus, &value.Attempts, &value.LastErrorClass, &deletedAt, &value.CreatedAt, &value.UpdatedAt); err != nil {
		return Document{}, pgstorage.MapError(ctx, err, runtimestorage.ErrNotFound, runtimestorage.ErrDuplicate, runtimestorage.ErrConflict, runtimestorage.ErrInvalid)
	}
	if json.Unmarshal(metadataRaw, &value.Metadata) != nil {
		return Document{}, runtimestorage.ErrStorage
	}
	if deletedAt.Valid {
		value.DeletedAt = &deletedAt.Time
	}
	return cloneDocument(value), nil
}

// Delete marks a document deleted and removes its vector projection.
func (s *Store) Delete(ctx context.Context, knowledgeID, documentID, chunkID string) error {
	if err := s.check(ctx); err != nil {
		return err
	}
	if !validKey(knowledgeID, true) || !validKey(documentID, true) || !validKey(chunkID, true) {
		return runtimestorage.ErrInvalid
	}
	query := fmt.Sprintf("UPDATE %s SET index_status='deleted',deleted_at=now(),embedding=NULL,version=version+1,updated_at=now() WHERE tenant_id=$1 AND collection=$2 AND knowledge_id=$3 AND document_id=$4 AND chunk_id=$5 AND deleted_at IS NULL", s.table("documents"))
	result, err := s.db.ExecContext(ctx, query, s.tenantID, s.collection, knowledgeID, documentID, chunkID)
	if err != nil {
		return pgstorage.MapError(ctx, err, runtimestorage.ErrNotFound, runtimestorage.ErrDuplicate, runtimestorage.ErrConflict, runtimestorage.ErrInvalid)
	}
	count, _ := result.RowsAffected()
	if count == 0 {
		return runtimestorage.ErrNotFound
	}
	return nil
}

// Retry requeues a failed or pending document without changing its source.
func (s *Store) Retry(ctx context.Context, knowledgeID, documentID, chunkID string) error {
	if err := s.check(ctx); err != nil {
		return err
	}
	if !validKey(knowledgeID, true) || !validKey(documentID, true) || !validKey(chunkID, true) {
		return runtimestorage.ErrInvalid
	}
	key := documentKey{knowledgeID: knowledgeID, documentID: documentID, chunkID: chunkID}
	query := fmt.Sprintf("UPDATE %s SET index_status='pending',attempts=0,last_error_class='',updated_at=now() WHERE tenant_id=$1 AND collection=$2 AND knowledge_id=$3 AND document_id=$4 AND chunk_id=$5 AND deleted_at IS NULL AND index_status IN ('failed','pending','ready','dead_letter')", s.table("documents"))
	result, err := s.db.ExecContext(ctx, query, s.tenantID, s.collection, knowledgeID, documentID, chunkID)
	if err != nil {
		return pgstorage.MapError(ctx, err, runtimestorage.ErrNotFound, runtimestorage.ErrDuplicate, runtimestorage.ErrConflict, runtimestorage.ErrInvalid)
	}
	count, _ := result.RowsAffected()
	if count == 0 {
		return runtimestorage.ErrNotFound
	}
	return s.enqueue(ctx, key)
}

// Reindex requeues a durable document and is equivalent to Retry for ready rows.
func (s *Store) Reindex(ctx context.Context, knowledgeID, documentID, chunkID string) error {
	return s.Retry(ctx, knowledgeID, documentID, chunkID)
}

// Reconcile requeues failed and dead-letter documents for this tenant. It is
// intended for an operator-owned repair loop and never crosses collections.
func (s *Store) Reconcile(ctx context.Context) (int, error) {
	if err := s.check(ctx); err != nil {
		return 0, err
	}
	query := fmt.Sprintf("SELECT knowledge_id,document_id,chunk_id FROM %s WHERE tenant_id=$1 AND collection=$2 AND deleted_at IS NULL AND index_status IN ('failed','dead_letter') ORDER BY updated_at,knowledge_id,document_id,chunk_id", s.table("documents"))
	rows, err := s.db.QueryContext(ctx, query, s.tenantID, s.collection)
	if err != nil {
		return 0, pgstorage.MapError(ctx, err, runtimestorage.ErrNotFound, runtimestorage.ErrDuplicate, runtimestorage.ErrConflict, runtimestorage.ErrInvalid)
	}
	defer rows.Close()
	keys := make([]documentKey, 0)
	for rows.Next() {
		var key documentKey
		if err := rows.Scan(&key.knowledgeID, &key.documentID, &key.chunkID); err != nil {
			return len(keys), runtimestorage.ErrStorage
		}
		keys = append(keys, key)
	}
	if err := rows.Err(); err != nil {
		return len(keys), runtimestorage.ErrStorage
	}
	_ = rows.Close()
	for _, key := range keys {
		if err := s.Retry(ctx, key.knowledgeID, key.documentID, key.chunkID); err != nil {
			return len(keys), err
		}
	}
	return len(keys), nil
}

func (s *Store) worker() {
	for {
		select {
		case <-s.stop:
			return
		case job := <-s.queue:
			s.index(context.Background(), job.key)
		}
	}
}

func (s *Store) index(ctx context.Context, key documentKey) {
	var content, model, modelVersion, status, vectorRaw string
	var dimension, version int
	query := fmt.Sprintf("SELECT content,embedding_model,embedding_version,embedding_dimension,version,index_status,COALESCE(embedding::text,'') FROM %s WHERE tenant_id=$1 AND collection=$2 AND knowledge_id=$3 AND document_id=$4 AND chunk_id=$5 AND deleted_at IS NULL", s.table("documents"))
	if err := s.db.QueryRowContext(ctx, query, s.tenantID, s.collection, key.knowledgeID, key.documentID, key.chunkID).Scan(&content, &model, &modelVersion, &dimension, &version, &status, &vectorRaw); err != nil {
		return
	}
	if status == string(IndexDeleted) || model != s.model || modelVersion != s.modelVer || dimension != s.dimension {
		s.markFailed(ctx, key, ErrIncompatible)
		return
	}
	var vector []float64
	var err error
	if vectorRaw != "" {
		vector, err = decodeVector(vectorRaw)
	} else {
		vector, err = s.embedder.Embed(ctx, content)
	}
	if err == nil && len(vector) != s.dimension {
		err = ErrIncompatible
	}
	if err != nil {
		s.markFailed(ctx, key, err)
		return
	}
	encoded, err := encodeVector(vector)
	if err != nil {
		s.markFailed(ctx, key, err)
		return
	}
	query = fmt.Sprintf("UPDATE %s SET embedding=$6::vector,index_status='ready',attempts=attempts+1,last_error_class='',updated_at=now() WHERE tenant_id=$1 AND collection=$2 AND knowledge_id=$3 AND document_id=$4 AND chunk_id=$5 AND version=$7 AND index_status='pending' AND deleted_at IS NULL", s.table("documents"))
	if _, err := s.db.ExecContext(ctx, query, s.tenantID, s.collection, key.knowledgeID, key.documentID, key.chunkID, encoded, version); err != nil {
		s.markFailed(ctx, key, err)
	}
}

func (s *Store) markFailed(ctx context.Context, key documentKey, cause error) {
	class := "provider_error"
	if errors.Is(cause, ErrIncompatible) {
		class = "incompatible_embedding"
	}
	query := fmt.Sprintf("UPDATE %s SET index_status=CASE WHEN attempts+1 >= $6 THEN 'dead_letter' ELSE 'failed' END,attempts=attempts+1,last_error_class=$7,updated_at=now() WHERE tenant_id=$1 AND collection=$2 AND knowledge_id=$3 AND document_id=$4 AND chunk_id=$5 AND deleted_at IS NULL", s.table("documents"))
	_, _ = s.db.ExecContext(ctx, query, s.tenantID, s.collection, key.knowledgeID, key.documentID, key.chunkID, s.maxAttempts, class)
}

// PutKnowledge adapts the legacy document contract to one default chunk.
func (s *Store) PutKnowledge(ctx context.Context, value runtimestorage.KnowledgeDocument) (runtimestorage.KnowledgeDocument, error) {
	if s == nil || value.TenantID != s.tenantID {
		return runtimestorage.KnowledgeDocument{}, runtimestorage.ErrInvalid
	}
	doc, err := s.Upsert(ctx, DocumentInput{KnowledgeID: value.DocumentID, DocumentID: value.DocumentID, ChunkID: "default", Content: value.Content, Metadata: value.Metadata, Embedding: value.Embedding, Checksum: value.Digest})
	if err != nil {
		return runtimestorage.KnowledgeDocument{}, err
	}
	return runtimestorage.KnowledgeDocument{TenantID: doc.TenantID, DocumentID: doc.DocumentID, Content: doc.Content, Metadata: cloneMap(doc.Metadata), Embedding: nil, Version: doc.Version, Digest: doc.Checksum, CreatedAt: doc.CreatedAt, UpdatedAt: doc.UpdatedAt}, nil
}

// GetKnowledge returns the default chunk for a legacy document.
func (s *Store) GetKnowledge(ctx context.Context, tenantID, documentID string) (runtimestorage.KnowledgeDocument, error) {
	if s == nil || tenantID != s.tenantID {
		return runtimestorage.KnowledgeDocument{}, runtimestorage.ErrInvalid
	}
	doc, err := s.Get(ctx, documentID, documentID, "default")
	if err != nil {
		return runtimestorage.KnowledgeDocument{}, err
	}
	return runtimestorage.KnowledgeDocument{TenantID: doc.TenantID, DocumentID: doc.DocumentID, Content: doc.Content, Metadata: cloneMap(doc.Metadata), Version: doc.Version, Digest: doc.Checksum, CreatedAt: doc.CreatedAt, UpdatedAt: doc.UpdatedAt}, nil
}

// SearchKnowledge adapts the legacy contract and returns only ready rows.
func (s *Store) SearchKnowledge(ctx context.Context, tenantID string, embedding []float64, limit int) ([]runtimestorage.KnowledgeSearchResult, error) {
	if s == nil || tenantID != s.tenantID {
		return nil, runtimestorage.ErrInvalid
	}
	values, err := s.Search(ctx, embedding, SearchOptions{Limit: limit})
	if err != nil {
		return nil, err
	}
	result := make([]runtimestorage.KnowledgeSearchResult, 0, len(values))
	for _, value := range values {
		result = append(result, runtimestorage.KnowledgeSearchResult{Document: runtimestorage.KnowledgeDocument{TenantID: value.Document.TenantID, DocumentID: value.Document.DocumentID, Content: value.Document.Content, Metadata: cloneMap(value.Document.Metadata), Version: value.Document.Version, Digest: value.Document.Checksum, CreatedAt: value.Document.CreatedAt, UpdatedAt: value.Document.UpdatedAt}, Score: value.Score})
	}
	return result, nil
}

// DeleteKnowledge deletes the default chunk for a legacy document.
func (s *Store) DeleteKnowledge(ctx context.Context, tenantID, documentID string) error {
	if s == nil || tenantID != s.tenantID {
		return runtimestorage.ErrInvalid
	}
	return s.Delete(ctx, documentID, documentID, "default")
}

// UpsertVector stores a generic vector as a pgvector document projection.
func (s *Store) UpsertVector(ctx context.Context, value runtimestorage.VectorRecord) error {
	if s == nil || value.TenantID != s.tenantID {
		return runtimestorage.ErrInvalid
	}
	source := string(value.Source)
	if source == "" {
		source = "generic"
	}
	_, err := s.Upsert(ctx, DocumentInput{KnowledgeID: source, DocumentID: value.DocumentID, ChunkID: "vector", Source: source, Content: value.Content, Metadata: value.Metadata, Embedding: value.Embedding})
	return err
}

// SearchVectors searches the provider's ready vector projections.
func (s *Store) SearchVectors(ctx context.Context, tenantID string, embedding []float64, limit int) ([]runtimestorage.VectorSearchResult, error) {
	if s == nil || tenantID != s.tenantID {
		return nil, runtimestorage.ErrInvalid
	}
	values, err := s.Search(ctx, embedding, SearchOptions{Limit: limit})
	if err != nil {
		return nil, err
	}
	result := make([]runtimestorage.VectorSearchResult, 0, len(values))
	for _, value := range values {
		result = append(result, runtimestorage.VectorSearchResult{Record: runtimestorage.VectorRecord{TenantID: value.Document.TenantID, Source: runtimestorage.VectorSource(value.Document.Source), DocumentID: value.Document.DocumentID, Content: value.Document.Content, Metadata: cloneMap(value.Document.Metadata), Version: value.Document.Version, UpdatedAt: value.Document.UpdatedAt}, Score: value.Score})
	}
	return result, nil
}

// DeleteVector deletes every source projection for the requested document.
func (s *Store) DeleteVector(ctx context.Context, tenantID, documentID string) error {
	if s == nil || tenantID != s.tenantID {
		return runtimestorage.ErrInvalid
	}
	if !validKey(documentID, true) {
		return runtimestorage.ErrInvalid
	}
	query := fmt.Sprintf("UPDATE %s SET index_status='deleted',deleted_at=now(),embedding=NULL,version=version+1,updated_at=now() WHERE tenant_id=$1 AND collection=$2 AND document_id=$3 AND deleted_at IS NULL", s.table("documents"))
	result, err := s.db.ExecContext(ctx, query, s.tenantID, s.collection, documentID)
	if err != nil {
		return pgstorage.MapError(ctx, err, runtimestorage.ErrNotFound, runtimestorage.ErrDuplicate, runtimestorage.ErrConflict, runtimestorage.ErrInvalid)
	}
	count, _ := result.RowsAffected()
	if count == 0 {
		return runtimestorage.ErrNotFound
	}
	return nil
}

var _ runtimestorage.KnowledgeStore = (*Store)(nil)
var _ runtimestorage.VectorStore = (*Store)(nil)
