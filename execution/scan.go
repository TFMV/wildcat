package execution

import (
	"bytes"
	"context"
	"sort"

	"github.com/TFMV/wildcat/format"
	"github.com/TFMV/wildcat/storage"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"go.etcd.io/bbolt"
)

type ScanOperator struct {
	storage   *storage.Engine
	dataset   string
	partition string
	columns   []string
	predicate *storage.Predicate
	mem       memory.Allocator
}

type ScanIterator struct {
	storage   *storage.Engine
	db        *bbolt.DB
	dataset   string
	partition string
	columns   []string
	mem       memory.Allocator

	colIdx   int
	colNames []string
	colData  map[string][]format.Chunk
	done     bool
}

func NewScanOperator(storageEngine *storage.Engine, dataset, partition string, columns []string, mem memory.Allocator) *ScanOperator {
	if mem == nil {
		mem = memory.DefaultAllocator
	}

	if len(columns) == 0 {
		columns = nil
	}

	return &ScanOperator{
		storage:   storageEngine,
		dataset:   dataset,
		partition: partition,
		columns:   columns,
		predicate: nil,
		mem:       mem,
	}
}

func (s *ScanOperator) WithPredicate(pred *storage.Predicate) *ScanOperator {
	s.predicate = pred
	return s
}

func (s *ScanOperator) Execute(ctx context.Context) (*ScanResult, error) {
	result := &ScanResult{
		Columns: make([]string, 0),
		Arrays:  make([]arrow.Array, 0),
	}

	columns, err := s.collectColumnData(ctx)
	if err != nil {
		return nil, err
	}

	for colName, chunks := range columns {
		if len(chunks) == 0 {
			continue
		}

		merged := format.MergeColumnChunks(chunks)
		if merged == nil {
			continue
		}

		result.Columns = append(result.Columns, colName)
		result.Arrays = append(result.Arrays, merged)
	}

	if len(result.Arrays) > 0 {
		fields := make([]arrow.Field, len(result.Arrays))
		for i, arr := range result.Arrays {
			fields[i] = arrow.Field{
				Name: result.Columns[i],
				Type: arr.DataType(),
			}
		}
		schema := arrow.NewSchema(fields, nil)
		result.Record = format.NewRecordBatch(schema, result.Arrays, int64(result.Arrays[0].Len()))
	}

	return result, nil
}

func (s *ScanOperator) collectColumnData(ctx context.Context) (map[string][]format.Chunk, error) {
	colData := make(map[string][]format.Chunk)

	allColumns, err := s.discoverColumns(ctx)
	if err != nil {
		return nil, err
	}

	var candidateChunkIDs []uint32
	if s.predicate != nil {
		predicate := storage.Predicate{
			Column: s.predicate.Column,
			Type:   s.predicate.Type,
			Value:  s.predicate.Value,
		}

		ids := make([]uint32, 0)
		err := s.storage.ScanColumnChunksWithPredicates(ctx, s.dataset, s.partition, predicate.Column, []storage.Predicate{predicate}, func(chunkID uint32, _ []byte) error {
			ids = append(ids, chunkID)
			return nil
		})
		if err != nil {
			return nil, err
		}

		if len(ids) == 0 {
			return colData, nil
		}

		candidateChunkIDs = ids
	}

	for _, col := range allColumns {
		if len(s.columns) > 0 && !containsString(s.columns, col) {
			continue
		}

		var chunks []format.Chunk

		colPredicates := []storage.Predicate{}
		if s.predicate != nil && s.predicate.Column == col {
			colPredicates = []storage.Predicate{{
				Column: s.predicate.Column,
				Type:   storage.PredicateType(s.predicate.Type),
				Value:  s.predicate.Value,
			}}
		}

		baseScanFn := func(chunkID uint32, data []byte) error {
			if len(candidateChunkIDs) > 0 && !containsChunkID(candidateChunkIDs, chunkID) {
				return nil
			}

			arr, err := format.DeserializeColumn(data)
			if err != nil {
				return err
			}

			if len(colPredicates) > 0 {
				execPred := Predicate{
					Column: colPredicates[0].Column,
					Type:   PredicateType(colPredicates[0].Type),
					Value:  colPredicates[0].Value,
				}
				filtered, err := ApplyPredicate(arr, execPred, s.mem)
				arr.Release()
				if err != nil {
					return err
				}

				if filtered != nil {
					chunks = append(chunks, format.Chunk{
						ColumnName: col,
						Array:      filtered,
						ChunkID:    chunkID,
						NumRows:    int64(filtered.Len()),
					})
				}
				return nil
			}

			chunks = append(chunks, format.Chunk{
				ColumnName: col,
				Array:      arr,
				ChunkID:    chunkID,
				NumRows:    int64(arr.Len()),
			})
			return nil
		}

		err := s.storage.ScanColumnChunks(ctx, s.dataset, s.partition, col, baseScanFn)
		if err != nil {
			return nil, err
		}

		colData[col] = chunks
	}

	return colData, nil
}

func (s *ScanOperator) discoverColumns(ctx context.Context) ([]string, error) {
	columns := make([]string, 0)
	seen := make(map[string]bool)

	prefix := storage.EncodeKeyPrefix(s.dataset, s.partition, "")

	err := s.storage.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte(storage.BucketData))
		if bucket == nil {
			return nil
		}

		cursor := bucket.Cursor()
		for k, _ := cursor.Seek(prefix); k != nil; k, _ = cursor.Next() {
			if !bytes.HasPrefix(k, prefix) {
				break
			}

			_, _, col, _, err := storage.ParseKey(k)
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

func (s *ScanOperator) Iterator() (*ScanIterator, error) {
	columns, err := s.discoverColumns(context.Background())
	if err != nil {
		return nil, err
	}

	colData, err := s.collectColumnData(context.Background())
	if err != nil {
		return nil, err
	}

	filteredCols := make([]string, 0)
	for _, col := range columns {
		if len(s.columns) == 0 || containsString(s.columns, col) {
			filteredCols = append(filteredCols, col)
		}
	}

	return &ScanIterator{
		storage:   s.storage,
		db:        nil,
		dataset:   s.dataset,
		partition: s.partition,
		columns:   s.columns,
		mem:       s.mem,
		colNames:  filteredCols,
		colData:   colData,
		colIdx:    0,
		done:      false,
	}, nil
}

func (it *ScanIterator) Next() (format.RecordBatch, error) {
	if it.done {
		return nil, nil
	}

	if it.colIdx >= len(it.colNames) {
		it.done = true
		return nil, nil
	}

	col := it.colNames[it.colIdx]
	it.colIdx++

	chunks := it.colData[col]
	if len(chunks) == 0 {
		return it.Next()
	}

	merged := format.MergeColumnChunks(chunks)
	if merged == nil {
		return nil, nil
	}

	schema := arrow.NewSchema([]arrow.Field{
		{Name: col, Type: merged.DataType()},
	}, nil)

	return format.NewRecordBatch(schema, []arrow.Array{merged}, int64(merged.Len())), nil
}

func (it *ScanIterator) Close() error {
	it.done = true
	return nil
}

func containsString(slice []string, s string) bool {
	for _, v := range slice {
		if v == s {
			return true
		}
	}
	return false
}

func containsChunkID(ids []uint32, target uint32) bool {
	idx := sort.Search(len(ids), func(i int) bool {
		return ids[i] >= target
	})
	return idx < len(ids) && ids[idx] == target
}

type ScanResult struct {
	Columns []string
	Arrays  []arrow.Array
	Record  format.RecordBatch
}

func (r *ScanResult) NumRows() int64 {
	if r.Record != nil {
		return r.Record.NumRows()
	}
	return 0
}

func (r *ScanResult) Release() {
	if r.Record != nil {
		r.Record.Release()
		r.Record = nil
	}
}

type StreamingScanner struct {
	storage   *storage.Engine
	dataset   string
	partition string
	columns   []string
	mem       memory.Allocator
	queue     chan format.RecordBatch
	done      chan struct{}
	closed    bool
}

func NewStreamingScanner(storageEngine *storage.Engine, dataset, partition string, columns []string, mem memory.Allocator) *StreamingScanner {
	if mem == nil {
		mem = memory.DefaultAllocator
	}

	return &StreamingScanner{
		storage:   storageEngine,
		dataset:   dataset,
		partition: partition,
		columns:   columns,
		mem:       mem,
		queue:     make(chan format.RecordBatch, 10),
		done:      make(chan struct{}),
	}
}

func (s *StreamingScanner) Start(ctx context.Context) error {
	go func() {
		defer close(s.queue)

		columns, err := s.collectColumnData(ctx)
		if err != nil {
			return
		}

		for col, chunks := range columns {
			if len(chunks) == 0 {
				continue
			}

			merged := format.MergeColumnChunks(chunks)
			if merged == nil {
				continue
			}

			schema := arrow.NewSchema([]arrow.Field{
				{Name: col, Type: merged.DataType()},
			}, nil)

			rb := format.NewRecordBatch(schema, []arrow.Array{merged}, int64(merged.Len()))
			select {
			case s.queue <- rb:
			case <-ctx.Done():
				return
			case <-s.done:
				return
			}
		}
	}()

	return nil
}

func (s *StreamingScanner) Read() (<-chan format.RecordBatch, error) {
	return s.queue, nil
}

func (s *StreamingScanner) Close() error {
	if s.closed {
		return nil
	}
	s.closed = true
	close(s.done)
	return nil
}

func (s *StreamingScanner) collectColumnData(ctx context.Context) (map[string][]format.Chunk, error) {
	colData := make(map[string][]format.Chunk)

	allColumns, err := s.discoverColumns(ctx)
	if err != nil {
		return nil, err
	}

	for _, col := range allColumns {
		if len(s.columns) > 0 && !containsString(s.columns, col) {
			continue
		}

		var chunks []format.Chunk

		err := s.storage.ScanColumnChunks(ctx, s.dataset, s.partition, col, func(chunkID uint32, data []byte) error {
			arr, err := format.DeserializeColumn(data)
			if err != nil {
				return err
			}

			chunks = append(chunks, format.Chunk{
				ColumnName: col,
				Array:      arr,
				ChunkID:    chunkID,
				NumRows:    int64(arr.Len()),
			})
			return nil
		})
		if err != nil {
			return nil, err
		}

		colData[col] = chunks
	}

	return colData, nil
}

func (s *StreamingScanner) discoverColumns(ctx context.Context) ([]string, error) {
	columns := make([]string, 0)
	seen := make(map[string]bool)

	prefix := storage.EncodeKeyPrefix(s.dataset, s.partition, "")

	err := s.storage.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte(storage.BucketData))
		if bucket == nil {
			return nil
		}

		cursor := bucket.Cursor()
		for k, _ := cursor.Seek(prefix); k != nil; k, _ = cursor.Next() {
			if !bytes.HasPrefix(k, prefix) {
				break
			}

			_, _, col, _, err := storage.ParseKey(k)
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
