package wildcat

import (
	"context"
	"fmt"

	"github.com/TFMV/wildcat/execution"
	"github.com/TFMV/wildcat/format"
	"github.com/TFMV/wildcat/query"
	"github.com/TFMV/wildcat/storage"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

type Engine struct {
	storage *storage.Engine
	parser  *query.Parser
	planner *query.Planner
	exec    *execution.Engine
	mem     memory.Allocator
}

type EngineConfig struct {
	StoragePath string
	Memory      memory.Allocator
}

func NewEngine(config *EngineConfig) (*Engine, error) {
	if config == nil {
		config = &EngineConfig{
			StoragePath: "wildcat.db",
			Memory:      memory.DefaultAllocator,
		}
	}

	storageEngine, err := storage.NewEngine(config.StoragePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open storage: %w", err)
	}

	parser := query.NewParser()
	planner := query.NewPlanner(storageEngine)
	execEngine := execution.New(storageEngine)

	return &Engine{
		storage: storageEngine,
		parser:  parser,
		planner: planner,
		exec:    execEngine,
		mem:     config.Memory,
	}, nil
}

func (e *Engine) Query(ctx context.Context, sql string) (*QueryResult, error) {
	if sql == "" {
		return nil, ErrEmptyQuery
	}

	ast, err := e.parser.Parse(sql)
	if err != nil {
		return nil, fmt.Errorf("parse error: %w", err)
	}

	plan, err := e.planner.Plan(ast)
	if err != nil {
		return nil, fmt.Errorf("plan error: %w", err)
	}

	result, err := plan.Execute()
	if err != nil {
		return nil, fmt.Errorf("execute error: %w", err)
	}

	return &QueryResult{
		Batch:  result,
		Schema: plan.Schema(),
	}, nil
}

func (e *Engine) QueryIterator(ctx context.Context, sql string) (*QueryIterator, error) {
	if sql == "" {
		return nil, ErrEmptyQuery
	}

	ast, err := e.parser.Parse(sql)
	if err != nil {
		return nil, fmt.Errorf("parse error: %w", err)
	}

	plan, err := e.planner.Plan(ast)
	if err != nil {
		return nil, fmt.Errorf("plan error: %w", err)
	}

	return &QueryIterator{
		plan: plan,
		ctx:  ctx,
	}, nil
}

func (e *Engine) ExecutePlan(ctx context.Context, plan query.ExecutionPlan) (*QueryResult, error) {
	result, err := plan.Execute()
	if err != nil {
		return nil, fmt.Errorf("execute error: %w", err)
	}

	return &QueryResult{
		Batch:  result,
		Schema: plan.Schema(),
	}, nil
}

func (e *Engine) Explain(sql string) (string, error) {
	ast, err := e.parser.Parse(sql)
	if err != nil {
		return "", fmt.Errorf("parse error: %w", err)
	}

	return e.planner.Explain(ast)
}

func (e *Engine) Stats() *EngineStats {
	return &EngineStats{
		storage: e.storage,
	}
}

func (e *Engine) Close() error {
	return e.storage.Close()
}

type QueryResult struct {
	Batch  format.RecordBatch
	Schema *arrow.Schema
}

func (r *QueryResult) NumRows() int64 {
	if r.Batch == nil {
		return 0
	}
	return r.Batch.NumRows()
}

func (r *QueryResult) NumCols() int {
	if r.Batch == nil {
		return 0
	}
	return int(r.Batch.NumCols())
}

func (r *QueryResult) ColumnNames() []string {
	if r.Batch == nil {
		return nil
	}

	names := make([]string, r.Batch.NumCols())
	for i := 0; i < int(r.Batch.NumCols()); i++ {
		names[i] = r.Batch.ColumnName(i)
	}
	return names
}

func (r *QueryResult) Print() {
	if r.Batch == nil {
		fmt.Println("Empty result")
		return
	}

	fmt.Printf("Rows: %d, Columns: %d\n", r.NumRows(), r.NumCols())
	fmt.Println("Columns:", r.ColumnNames())
}

func (r *QueryResult) Release() {
	if r.Batch != nil {
		r.Batch.Release()
		r.Batch = nil
	}
}

type QueryIterator struct {
	plan    query.ExecutionPlan
	ctx     context.Context
	current format.RecordBatch
	done    bool
}

func (it *QueryIterator) Next() (format.RecordBatch, error) {
	if it.done {
		return nil, nil
	}

	result, err := it.plan.Execute()
	if err != nil {
		return nil, err
	}

	it.done = true
	it.current = result
	return result, nil
}

func (it *QueryIterator) Close() error {
	if it.current != nil {
		it.current.Release()
		it.current = nil
	}
	return nil
}

type EngineStats struct {
	storage *storage.Engine
}

func (s *EngineStats) String() string {
	return "Engine Stats"
}

var (
	ErrEmptyQuery  = fmt.Errorf("empty query")
	ErrInvalidPlan = fmt.Errorf("invalid plan")
	ErrExecution   = fmt.Errorf("execution failed")
)

type IngestOption func(*ingestOptions)

type ingestOptions struct {
	chunkSize int
	partition string
}

func WithChunkSize(size int) IngestOption {
	return func(o *ingestOptions) {
		o.chunkSize = size
	}
}

func WithPartition(partition string) IngestOption {
	return func(o *ingestOptions) {
		o.partition = partition
	}
}

func (e *Engine) Ingest(ctx context.Context, dataset string, batch format.RecordBatch, opts ...IngestOption) (*IngestResult, error) {
	options := &ingestOptions{
		chunkSize: 65536,
		partition: "default",
	}

	for _, opt := range opts {
		opt(options)
	}

	ingestor := storage.NewIngestor(e.storage, options.chunkSize)

	result, err := ingestor.Insert(ctx, dataset, options.partition, batch)
	if err != nil {
		return nil, fmt.Errorf("ingest error: %w", err)
	}

	return &IngestResult{
		RowsWritten: result.RowsWritten,
		ChunksCount: result.ChunksCount,
		PartitionID: result.PartitionID,
	}, nil
}

type IngestResult struct {
	RowsWritten int64
	ChunksCount int
	PartitionID string
}

func (e *Engine) ListDatasets(ctx context.Context) ([]string, error) {
	return nil, nil
}

func (e *Engine) GetDatasetMetadata(ctx context.Context, dataset string) (*DatasetInfo, error) {
	meta, err := e.storage.LoadMetadata(ctx, dataset, "default")
	if err != nil {
		return nil, err
	}

	if meta == nil {
		return nil, fmt.Errorf("dataset not found")
	}

	return &DatasetInfo{
		Name:      dataset,
		RowCount:  meta.RowCount,
		ChunkSize: meta.ChunkSize,
		NumChunks: meta.NumChunks,
	}, nil
}

type DatasetInfo struct {
	Name      string
	RowCount  int64
	ChunkSize int64
	NumChunks int
}

type Transaction struct {
	engine   *Engine
	readOnly bool
}

func (e *Engine) BeginTransaction(readOnly bool) *Transaction {
	return &Transaction{
		engine:   e,
		readOnly: readOnly,
	}
}

func (t *Transaction) Query(ctx context.Context, sql string) (*QueryResult, error) {
	return t.engine.Query(ctx, sql)
}

func (t *Transaction) Commit() error {
	return nil
}

func (t *Transaction) Rollback() error {
	return nil
}
