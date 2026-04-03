package storage

import (
	"bytes"
	"context"
	"encoding/binary"
	"sync"
	"sync/atomic"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/TFMV/wildcat/format"
	"go.etcd.io/bbolt"
)

const (
	DefaultChunkSize = 65536

	MaxConcurrentWrites = 4
)

// bufferPool provides scratch buffers to eliminate serialization allocations.
// Note: You will need to update format.SerializeColumn, EncodeKey, etc.,
// to accept an io.Writer or *bytes.Buffer to fully utilize this.
var bufferPool = sync.Pool{
	New: func() interface{} {
		return bytes.NewBuffer(make([]byte, 0, 65536))
	},
}

type Ingestor struct {
	engine       *Engine
	chunkSize    int
	stats        *IngestStats
	computeStats bool
}

type IngestStats struct {
	RowsIngested  int64
	ChunksWritten int64
	BytesWritten  int64
	Errors        int64
}

type IngestResult struct {
	RowsWritten int64
	ChunksCount int
	PartitionID string
}

func NewIngestor(engine *Engine, chunkSize int) *Ingestor {
	if chunkSize <= 0 {
		chunkSize = DefaultChunkSize
	}
	return &Ingestor{
		engine:       engine,
		chunkSize:    chunkSize,
		stats:        &IngestStats{},
		computeStats: true,
	}
}

func (i *Ingestor) WithStats(enabled bool) *Ingestor {
	i.computeStats = enabled
	return i
}

func (i *Ingestor) Insert(ctx context.Context, dataset, partition string, batch arrow.RecordBatch) (*IngestResult, error) {
	if batch == nil || batch.NumRows() == 0 {
		return &IngestResult{}, nil
	}

	if err := i.validateSchema(dataset, batch.Schema()); err != nil {
		atomic.AddInt64(&i.stats.Errors, 1)
		return nil, err
	}

	result := &IngestResult{
		RowsWritten: batch.NumRows(),
	}

	columnChunks := format.SplitRecordBatch(batch, i.chunkSize)

	// 1. Precompute static keys outside the transactions to avoid repeated concatenations
	metaKeyStr := dataset + "/" + partition + "/next_chunk_id"
	metaKey := []byte(metaKeyStr)

	var nextChunkID uint32
	if err := i.engine.db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte(BucketMeta))
		if bucket != nil {
			if data := bucket.Get(metaKey); len(data) >= 4 {
				nextChunkID = binary.BigEndian.Uint32(data)
			}
		}
		return nil
	}); err != nil {
		return nil, err
	}

	totalChunks := 0
	for _, cc := range columnChunks {
		totalChunks += len(cc.Chunks)
	}

	result.ChunksCount = totalChunks

	err := i.engine.db.Update(func(tx *bbolt.Tx) error {
		dataBucket, err := tx.CreateBucketIfNotExists([]byte(BucketData))
		if err != nil {
			return err
		}

		metaBucket, err := tx.CreateBucketIfNotExists([]byte(BucketMeta))
		if err != nil {
			return err
		}

		chunkID := nextChunkID

		// 2. Allocate the struct once outside the loop and mutate it
		var colMeta ColumnMetadata

		dataBuf := bufferPool.Get().(*bytes.Buffer)
		keyBuf := bufferPool.Get().(*bytes.Buffer)
		defer bufferPool.Put(dataBuf)
		defer bufferPool.Put(keyBuf)
		dataBuf.Reset()
		keyBuf.Reset()

		for _, cc := range columnChunks {
			for _, chunk := range cc.Chunks {
				dataBuf.Reset()
				if err := format.SerializeColumnTo(dataBuf, chunk.Array); err != nil {
					chunk.Array.Release()
					return err
				}

				keyBuf.Reset()
				EncodeKeyTo(keyBuf, dataset, partition, cc.Column, chunkID+chunk.ChunkID)
				if err := dataBucket.Put(keyBuf.Bytes(), dataBuf.Bytes()); err != nil {
					chunk.Array.Release()
					return err
				}

				atomic.AddInt64(&i.stats.BytesWritten, int64(dataBuf.Len()))

				if i.computeStats {
					stats := ComputeArrayStats(chunk.Array, dataset, partition, cc.Column, chunkID+chunk.ChunkID)
					if stats != nil {
						statsData, _ := stats.Encode()
						keyBuf.Reset()
						chunkStatsKeyTo(keyBuf, dataset, partition, cc.Column, chunkID+chunk.ChunkID)
						metaBucket.Put(keyBuf.Bytes(), statsData)
					}

					// 3. Reuse the struct instead of allocating a new pointer on the heap
					colMeta = ColumnMetadata{
						Dataset:    dataset,
						Partition:  partition,
						Column:     cc.Column,
						DataType:   chunk.Array.DataType().Name(),
						NumChunks:  int64(totalChunks),
						TotalRows:  result.RowsWritten,
						TotalNulls: stats.NullCount,
						MinValue:   stats.MinValue,
						MaxValue:   stats.MaxValue,
					}

					keyBuf.Reset()
					columnMetaKeyTo(keyBuf, dataset, partition, cc.Column)
					metaData, _ := colMeta.Encode() // Ensure Encode() uses value receiver
					metaBucket.Put(keyBuf.Bytes(), metaData)
				}

				chunk.Array.Release()
				chunkID++
			}
		}

		// 4. Use a stack-allocated array instead of make([]byte, 4)
		var nextIDBuf [4]byte
		binary.BigEndian.PutUint32(nextIDBuf[:], nextChunkID+uint32(totalChunks))
		if err := metaBucket.Put(metaKey, nextIDBuf[:]); err != nil {
			return err
		}

		result.PartitionID = partition

		return nil
	})

	if err != nil {
		atomic.AddInt64(&i.stats.Errors, 1)
		return nil, err
	}

	atomic.AddInt64(&i.stats.RowsIngested, result.RowsWritten)
	atomic.AddInt64(&i.stats.ChunksWritten, int64(totalChunks))

	if err := i.updateMetadata(ctx, dataset, partition, result.RowsWritten); err != nil {
		return nil, err
	}

	return result, nil
}

func (i *Ingestor) InsertMany(ctx context.Context, dataset, partition string, batches []arrow.RecordBatch) (*IngestResult, error) {
	totalRows := int64(0)
	totalChunks := 0

	for _, batch := range batches {
		if batch == nil || batch.NumRows() == 0 {
			continue
		}

		columnChunks := format.SplitRecordBatch(batch, i.chunkSize)
		for _, cc := range columnChunks {
			totalChunks += len(cc.Chunks)
		}

		_, err := i.Insert(ctx, dataset, partition, batch)
		if err != nil {
			return nil, err
		}
		totalRows += batch.NumRows()
	}

	return &IngestResult{
		RowsWritten: totalRows,
		ChunksCount: totalChunks,
		PartitionID: partition,
	}, nil
}

func (i *Ingestor) validateSchema(dataset string, schema *arrow.Schema) error {
	if schema == nil {
		return ErrInvalidDataset
	}

	for _, field := range schema.Fields() {
		if field.Name == "" {
			return ErrInvalidDataset
		}
		if len(field.Name) > MaxColumnLen {
			return ErrInvalidDataset
		}
	}

	return nil
}

func (i *Ingestor) updateMetadata(ctx context.Context, dataset, partition string, rowsAdded int64) error {
	meta, err := i.engine.db.LoadMetadata(ctx, dataset, partition)
	if err != nil || meta == nil {
		meta = &Metadata{
			RowCount:   0,
			ChunkSize:  int64(i.chunkSize),
			NumChunks:  0,
			MinValues:  make(map[string]interface{}),
			MaxValues:  make(map[string]interface{}),
			NullCounts: make(map[string]int64),
		}
	}

	meta.RowCount += rowsAdded
	meta.NumChunks++

	return i.engine.db.StoreMetadata(ctx, dataset, partition, meta)
}

func (i *Ingestor) Stats() *IngestStats {
	return &IngestStats{
		RowsIngested:  atomic.LoadInt64(&i.stats.RowsIngested),
		ChunksWritten: atomic.LoadInt64(&i.stats.ChunksWritten),
		BytesWritten:  atomic.LoadInt64(&i.stats.BytesWritten),
		Errors:        atomic.LoadInt64(&i.stats.Errors),
	}
}

func (i *Ingestor) ResetStats() {
	atomic.StoreInt64(&i.stats.RowsIngested, 0)
	atomic.StoreInt64(&i.stats.ChunksWritten, 0)
	atomic.StoreInt64(&i.stats.BytesWritten, 0)
	atomic.StoreInt64(&i.stats.Errors, 0)
}

type ChunkedIngestor struct {
	ingestor *Ingestor
	mem      memory.Allocator
}

func NewChunkedIngestor(engine *Engine, chunkSize int, mem memory.Allocator) *ChunkedIngestor {
	if mem == nil {
		mem = memory.DefaultAllocator
	}
	return &ChunkedIngestor{
		ingestor: NewIngestor(engine, chunkSize),
		mem:      mem,
	}
}

// NOTE: This row-by-row ingestion is fundamentally allocation-heavy due to `map[string]interface{}`.
// Use this only for testing/convenience. In production, ingest directly into Arrow RecordBatches.
func (c *ChunkedIngestor) InsertFromRows(ctx context.Context, dataset, partition string, rows []map[string]interface{}, schema *arrow.Schema) (*IngestResult, error) {
	if len(rows) == 0 {
		return &IngestResult{}, nil
	}

	batch, err := rowsToRecordBatch(rows, schema, c.mem)
	if err != nil {
		return nil, err
	}
	defer batch.Release()

	return c.ingestor.Insert(ctx, dataset, partition, batch)
}

func rowsToRecordBatch(rows []map[string]interface{}, schema *arrow.Schema, mem memory.Allocator) (arrow.RecordBatch, error) {
	columns := make([]arrow.Array, schema.NumFields())

	for colIdx, field := range schema.Fields() {
		builder := newBuilder(mem, field.Type)
		builder.Reserve(len(rows))

		for _, row := range rows {
			if val, ok := row[field.Name]; ok && val != nil {
				if err := appendValue(builder, val); err != nil {
					return nil, err
				}
			} else {
				builder.AppendNull()
			}
		}

		columns[colIdx] = builder.NewArray()
		returnBuilder(builder)
	}

	return array.NewRecordBatch(schema, columns, int64(len(rows))), nil
}

func newBuilder(mem memory.Allocator, dt arrow.DataType) array.Builder {
	if mem == nil {
		mem = memory.DefaultAllocator
	}

	switch dt.ID() {
	case arrow.INT64:
		return defaultBuilderPool.GetInt64(mem)
	case arrow.INT32:
		return defaultBuilderPool.GetInt32(mem)
	case arrow.FLOAT64:
		return defaultBuilderPool.GetFloat64(mem)
	case arrow.STRING:
		return defaultBuilderPool.GetString(mem)
	default:
		return array.NewBuilder(mem, dt)
	}
}

func returnBuilder(b array.Builder) {
	if b == nil {
		return
	}

	switch b := b.(type) {
	case *array.Int64Builder:
		defaultBuilderPool.ReturnInt64(b)
	case *array.Int32Builder:
		defaultBuilderPool.ReturnInt32(b)
	case *array.Float64Builder:
		defaultBuilderPool.ReturnFloat64(b)
	case *array.StringBuilder:
		defaultBuilderPool.ReturnString(b)
	default:
		b.Release()
	}
}

func appendValue(builder array.Builder, value interface{}) error {
	switch b := builder.(type) {
	case *array.Int64Builder:
		switch v := value.(type) {
		case int64:
			b.Append(v)
		case int:
			b.Append(int64(v))
		default:
			b.AppendNull()
		}
	case *array.Int32Builder:
		switch v := value.(type) {
		case int32:
			b.Append(v)
		case int:
			b.Append(int32(v))
		default:
			b.AppendNull()
		}
	case *array.Int16Builder:
		switch v := value.(type) {
		case int16:
			b.Append(v)
		case int:
			b.Append(int16(v))
		default:
			b.AppendNull()
		}
	case *array.Int8Builder:
		switch v := value.(type) {
		case int8:
			b.Append(v)
		case int:
			b.Append(int8(v))
		default:
			b.AppendNull()
		}
	case *array.Uint64Builder:
		switch v := value.(type) {
		case uint64:
			b.Append(v)
		case uint:
			b.Append(uint64(v))
		default:
			b.AppendNull()
		}
	case *array.Uint32Builder:
		switch v := value.(type) {
		case uint32:
			b.Append(v)
		case uint:
			b.Append(uint32(v))
		default:
			b.AppendNull()
		}
	case *array.Uint16Builder:
		switch v := value.(type) {
		case uint16:
			b.Append(v)
		case uint:
			b.Append(uint16(v))
		default:
			b.AppendNull()
		}
	case *array.Uint8Builder:
		switch v := value.(type) {
		case uint8:
			b.Append(v)
		case uint:
			b.Append(uint8(v))
		default:
			b.AppendNull()
		}
	case *array.Float64Builder:
		switch v := value.(type) {
		case float64:
			b.Append(v)
		case float32:
			b.Append(float64(v))
		default:
			b.AppendNull()
		}
	case *array.Float32Builder:
		switch v := value.(type) {
		case float32:
			b.Append(v)
		case float64:
			b.Append(float32(v))
		default:
			b.AppendNull()
		}
	case *array.StringBuilder:
		switch v := value.(type) {
		case string:
			b.Append(v)
		default:
			b.AppendNull()
		}
	case *array.BooleanBuilder:
		switch v := value.(type) {
		case bool:
			b.Append(v)
		default:
			b.AppendNull()
		}
	default:
		builder.AppendNull()
	}
	return nil
}
