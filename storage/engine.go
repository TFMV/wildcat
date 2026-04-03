package storage

import (
	"bytes"
	"context"

	"github.com/apache/arrow-go/v18/arrow"
	"go.etcd.io/bbolt"
)

type Engine struct {
	db *DB
}

func NewEngine(path string) (*Engine, error) {
	db, err := Open(path)
	if err != nil {
		return nil, err
	}
	return &Engine{db: db}, nil
}

func (e *Engine) Close() error {
	return e.db.Close()
}

func (e *Engine) PutColumnChunk(ctx context.Context, dataset, partition, column string, chunkID uint32, data []byte) error {
	return e.db.PutColumnChunk(ctx, dataset, partition, column, chunkID, data)
}

func (e *Engine) GetColumnChunks(ctx context.Context, dataset, partition, column string) *ChunkIterator {
	return e.db.GetColumnChunks(ctx, dataset, partition, column)
}

func (e *Engine) ScanColumnChunks(ctx context.Context, dataset, partition, column string, fn func(chunkID uint32, data []byte) error) error {
	return e.db.ScanColumnChunks(ctx, dataset, partition, column, fn)
}

func (e *Engine) ScanColumnChunksWithPredicates(ctx context.Context, dataset, partition, column string,
	predicates []Predicate, fn func(chunkID uint32, data []byte) error) error {
	return e.db.ScanColumnChunksWithPredicates(ctx, dataset, partition, column, predicates, fn)
}

func (e *Engine) StoreMetadata(ctx context.Context, dataset, partition string, meta *Metadata) error {
	return e.db.StoreMetadata(ctx, dataset, partition, meta)
}

func (e *Engine) LoadMetadata(ctx context.Context, dataset, partition string) (*Metadata, error) {
	return e.db.LoadMetadata(ctx, dataset, partition)
}

func (e *Engine) ListPartitions(ctx context.Context, dataset string) ([]string, error) {
	return e.db.ListPartitions(ctx, dataset)
}

func (e *Engine) ReadPartitionColumns(ctx context.Context, dataset, partition string) (map[string][][]byte, error) {
	columns := make(map[string][][]byte)

	err := e.db.db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte(BucketData))
		if bucket == nil {
			return nil
		}

		prefix := EncodeKeyPrefix(dataset, partition, "")
		cursor := bucket.Cursor()

		for k, v := cursor.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, v = cursor.Next() {
			_, _, col, _, err := ParseKey(k)
			if err != nil {
				continue
			}
			columns[col] = append(columns[col], v)
		}
		return nil
	})

	return columns, err
}

func (e *Engine) View(fn func(tx *bbolt.Tx) error) error {
	return e.db.View(fn)
}

type Dataset struct {
	Name       string
	Schema     *arrow.Schema
	Partitions []Partition
}

type Partition struct {
	ID        string
	Dataset   string
	RowCount  int64
	ChunkSize int64
}
