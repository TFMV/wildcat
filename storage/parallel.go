package storage

import (
	"bytes"
	"context"
	"sync"
	"sync/atomic"

	"github.com/TFMV/wildcat/format"
	"github.com/apache/arrow-go/v18/arrow"
	"go.etcd.io/bbolt"
)

type ParallelReader struct {
	engine     *Engine
	numWorkers int
}

func NewParallelReader(engine *Engine, numWorkers int) *ParallelReader {
	if numWorkers <= 0 {
		numWorkers = 4
	}
	return &ParallelReader{
		engine:     engine,
		numWorkers: numWorkers,
	}
}

type ParallelReadResult struct {
	Chunks     []format.Chunk
	Errors     []error
	RowsRead   int64
	ChunksRead int32
}

func (p *ParallelReader) ReadColumn(ctx context.Context, dataset, partition, column string) (*ParallelReadResult, error) {
	result := &ParallelReadResult{
		Chunks: make([]format.Chunk, 0),
		Errors: make([]error, 0),
	}

	chunks, err := p.collectChunkInfo(dataset, partition, column)
	if err != nil {
		return nil, err
	}

	if len(chunks) == 0 {
		return result, nil
	}

	type chunkTask struct {
		idx     int
		chunkID uint32
		data    []byte
	}

	tasks := make(chan chunkTask, len(chunks))
	results := make(chan format.Chunk, len(chunks))
	errors := make(chan error, len(chunks))

	var wg sync.WaitGroup

	for i := 0; i < p.numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for task := range tasks {
				arr, err := format.DeserializeColumn(task.data)
				if err != nil {
					errors <- err
					continue
				}
				results <- format.Chunk{
					ColumnName: column,
					Array:      arr,
					ChunkID:    task.chunkID,
					NumRows:    int64(arr.Len()),
				}
			}
		}()
	}

	go func() {
		for _, c := range chunks {
			tasks <- chunkTask{idx: c.idx, chunkID: c.chunkID, data: c.data}
		}
		close(tasks)
	}()

	go func() {
		wg.Wait()
		close(results)
		close(errors)
	}()

	for r := range results {
		result.Chunks = append(result.Chunks, r)
		atomic.AddInt64(&result.RowsRead, r.NumRows)
		atomic.AddInt32(&result.ChunksRead, 1)
	}

	for e := range errors {
		result.Errors = append(result.Errors, e)
	}

	return result, nil
}

type chunkInfo struct {
	idx     int
	chunkID uint32
	data    []byte
}

func (p *ParallelReader) collectChunkInfo(dataset, partition, column string) ([]chunkInfo, error) {
	var chunks []chunkInfo
	var mu sync.Mutex

	err := p.engine.db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte(BucketData))
		if bucket == nil {
			return nil
		}

		prefix := EncodeKeyPrefix(dataset, partition, column)
		cursor := bucket.Cursor()

		idx := 0
		for k, v := cursor.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, v = cursor.Next() {
			chunkID, err := ExtractChunkID(k)
			if err != nil {
				continue
			}
			mu.Lock()
			chunks = append(chunks, chunkInfo{
				idx:     idx,
				chunkID: chunkID,
				data:    v,
			})
			mu.Unlock()
			idx++
		}
		return nil
	})

	return chunks, err
}

func (p *ParallelReader) ReadAllColumns(ctx context.Context, dataset, partition string) (map[string][]format.Chunk, error) {
	columns, err := p.discoverColumns(dataset, partition)
	if err != nil {
		return nil, err
	}

	type columnResult struct {
		column string
		chunks []format.Chunk
		err    error
	}

	results := make(chan columnResult, len(columns))
	var wg sync.WaitGroup

	for _, col := range columns {
		wg.Add(1)
		go func(c string) {
			defer wg.Done()
			result, err := p.ReadColumn(ctx, dataset, partition, c)
			if err != nil {
				results <- columnResult{column: c, err: err}
				return
			}
			results <- columnResult{column: c, chunks: result.Chunks}
		}(col)
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	columnData := make(map[string][]format.Chunk)
	for r := range results {
		if r.err != nil {
			continue
		}
		columnData[r.column] = r.chunks
	}

	return columnData, nil
}

func (p *ParallelReader) discoverColumns(dataset, partition string) ([]string, error) {
	columns := make([]string, 0)
	seen := make(map[string]bool)

	err := p.engine.db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte(BucketData))
		if bucket == nil {
			return nil
		}

		prefix := EncodeKeyPrefix(dataset, partition, "")
		cursor := bucket.Cursor()

		for k, _ := cursor.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, _ = cursor.Next() {
			_, _, col, _, err := ParseKey(k)
			if err != nil {
				continue
			}
			if !seen[col] {
				seen[col] = true
				columns = append(columns, col)
			}
		}
		return nil
	})

	return columns, err
}

type ParallelIngestor struct {
	ingestor *Ingestor
	workers  int
}

func NewParallelIngestor(engine *Engine, chunkSize, workers int) *ParallelIngestor {
	return &ParallelIngestor{
		ingestor: NewIngestor(engine, chunkSize),
		workers:  workers,
	}
}

func (p *ParallelIngestor) InsertBatches(ctx context.Context, dataset, partition string, batches []arrow.RecordBatch) (*IngestResult, error) {
	if len(batches) == 0 {
		return &IngestResult{}, nil
	}

	type batchTask struct {
		idx    int
		batch  arrow.RecordBatch
		result *IngestResult
		err    error
	}

	batchChan := make(chan batchTask, len(batches))
	resultChan := make(chan batchTask, len(batches))

	var wg sync.WaitGroup

	for i := 0; i < p.workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for task := range batchChan {
				result, err := p.ingestor.Insert(ctx, dataset, partition, task.batch)
				resultChan <- batchTask{
					idx:    task.idx,
					result: result,
					err:    err,
				}
			}
		}()
	}

	go func() {
		for i, batch := range batches {
			batchChan <- batchTask{idx: i, batch: batch}
		}
		close(batchChan)
	}()

	go func() {
		wg.Wait()
		close(resultChan)
	}()

	totalRows := int64(0)
	totalChunks := 0

	for r := range resultChan {
		if r.err != nil {
			continue
		}
		if r.result != nil {
			totalRows += r.result.RowsWritten
			totalChunks += r.result.ChunksCount
		}
	}

	return &IngestResult{
		RowsWritten: totalRows,
		ChunksCount: totalChunks,
		PartitionID: partition,
	}, nil
}
