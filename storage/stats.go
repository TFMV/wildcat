package storage

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"

	"go.etcd.io/bbolt"
)

type ChunkStats struct {
	Dataset   string
	Partition string
	Column    string
	ChunkID   uint32

	RowCount  int64
	NullCount int64
	MinValue  []byte
	MaxValue  []byte

	MinInt64   *int64
	MaxInt64   *int64
	MinFloat64 *float64
	MaxFloat64 *float64
	MinString  *string
	MaxString  *string
}

func (s *ChunkStats) String() string {
	return fmt.Sprintf("ChunkStats(%s/%s/%s[%d]: rows=%d, nulls=%d)",
		s.Dataset, s.Partition, s.Column, s.ChunkID, s.RowCount, s.NullCount)
}

func (s *ChunkStats) Encode() ([]byte, error) {
	buf := new(bytes.Buffer)

	binary.Write(buf, binary.BigEndian, s.RowCount)
	binary.Write(buf, binary.BigEndian, s.NullCount)

	if s.MinValue != nil {
		binary.Write(buf, binary.BigEndian, int32(len(s.MinValue)))
		buf.Write(s.MinValue)
	} else {
		binary.Write(buf, binary.BigEndian, int32(0))
	}

	if s.MaxValue != nil {
		binary.Write(buf, binary.BigEndian, int32(len(s.MaxValue)))
		buf.Write(s.MaxValue)
	} else {
		binary.Write(buf, binary.BigEndian, int32(0))
	}

	return buf.Bytes(), nil
}

func DecodeChunkStats(data []byte) (*ChunkStats, error) {
	if len(data) == 0 {
		return nil, nil
	}

	buf := bytes.NewReader(data)
	stats := &ChunkStats{}

	if err := binary.Read(buf, binary.BigEndian, &stats.RowCount); err != nil {
		return nil, err
	}

	if err := binary.Read(buf, binary.BigEndian, &stats.NullCount); err != nil {
		return nil, err
	}

	var minLen int32
	if err := binary.Read(buf, binary.BigEndian, &minLen); err != nil {
		return nil, err
	}
	if minLen > 0 {
		stats.MinValue = make([]byte, minLen)
		if _, err := buf.Read(stats.MinValue); err != nil {
			return nil, err
		}
	}

	var maxLen int32
	if err := binary.Read(buf, binary.BigEndian, &maxLen); err != nil {
		return nil, err
	}
	if maxLen > 0 {
		stats.MaxValue = make([]byte, maxLen)
		if _, err := buf.Read(stats.MaxValue); err != nil {
			return nil, err
		}
	}

	return stats, nil
}

func (d *DB) WriteChunkStats(ctx context.Context, stats *ChunkStats) error {
	return d.db.Update(func(tx *bbolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists([]byte(BucketMeta))
		if err != nil {
			return err
		}

		key := chunkStatsKey(stats.Dataset, stats.Partition, stats.Column, stats.ChunkID)
		data, err := stats.Encode()
		if err != nil {
			return err
		}

		return bucket.Put(key, data)
	})
}

func (d *DB) ReadChunkStats(ctx context.Context, dataset, partition, column string, chunkID uint32) (*ChunkStats, error) {
	var stats *ChunkStats

	err := d.db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte(BucketMeta))
		if bucket == nil {
			return nil
		}

		key := chunkStatsKey(dataset, partition, column, chunkID)
		data := bucket.Get(key)
		if data == nil {
			return nil
		}

		var err error
		stats, err = DecodeChunkStats(data)
		return err
	})

	return stats, err
}

func (d *DB) ReadColumnStats(ctx context.Context, dataset, partition, column string) ([]ChunkStats, error) {
	var results []ChunkStats

	err := d.db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte(BucketMeta))
		if bucket == nil {
			return nil
		}

		prefix := chunkStatsPrefix(dataset, partition, column)
		cursor := bucket.Cursor()

		for k, v := cursor.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, v = cursor.Next() {
			stats, err := DecodeChunkStats(v)
			if err != nil {
				continue
			}
			results = append(results, *stats)
		}

		return nil
	})

	return results, err
}

func (d *DB) ScanChunkStats(ctx context.Context, dataset, partition, column string, fn func(*ChunkStats) error) error {
	return d.db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte(BucketMeta))
		if bucket == nil {
			return nil
		}

		prefix := chunkStatsPrefix(dataset, partition, column)
		cursor := bucket.Cursor()

		for k, v := cursor.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, v = cursor.Next() {
			stats, err := DecodeChunkStats(v)
			if err != nil {
				continue
			}
			if err := fn(stats); err != nil {
				return err
			}
		}

		return nil
	})
}

func chunkStatsKey(dataset, partition, column string, chunkID uint32) []byte {
	key := fmt.Sprintf("%s/%s/%s/stats/%d", dataset, partition, column, chunkID)
	return []byte(key)
}

func chunkStatsPrefix(dataset, partition, column string) []byte {
	prefix := fmt.Sprintf("%s/%s/%s/stats/", dataset, partition, column)
	return []byte(prefix)
}

func ComputeArrayStats(arr arrow.Array, dataset, partition, column string, chunkID uint32) *ChunkStats {
	if arr == nil {
		return nil
	}

	stats := &ChunkStats{
		Dataset:   dataset,
		Partition: partition,
		Column:    column,
		ChunkID:   chunkID,
		RowCount:  int64(arr.Len()),
		NullCount: int64(arr.Len() - arr.NullN()),
	}

	switch arr := arr.(type) {
	case *array.Int64:
		var min int64
		var max int64
		found := false
		for i := 0; i < arr.Len(); i++ {
			if !arr.IsValid(i) {
				continue
			}
			v := arr.Value(i)
			if !found || v < min {
				min = v
			}
			if !found || v > max {
				max = v
			}
			found = true
		}
		if found {
			stats.MinInt64 = &min
			stats.MaxInt64 = &max
			minBytes := make([]byte, 8)
			binary.BigEndian.PutUint64(minBytes, uint64(min))
			stats.MinValue = minBytes
			maxBytes := make([]byte, 8)
			binary.BigEndian.PutUint64(maxBytes, uint64(max))
			stats.MaxValue = maxBytes
		}

	case *array.Int32:
		var min int64
		var max int64
		found := false
		for i := 0; i < arr.Len(); i++ {
			if !arr.IsValid(i) {
				continue
			}
			v := int64(arr.Value(i))
			if !found || v < min {
				min = v
			}
			if !found || v > max {
				max = v
			}
			found = true
		}
		if found {
			stats.MinInt64 = &min
			stats.MaxInt64 = &max
			minBytes := make([]byte, 8)
			binary.BigEndian.PutUint64(minBytes, uint64(min))
			stats.MinValue = minBytes
			maxBytes := make([]byte, 8)
			binary.BigEndian.PutUint64(maxBytes, uint64(max))
			stats.MaxValue = maxBytes
		}

	case *array.Float64:
		var min float64
		var max float64
		found := false
		for i := 0; i < arr.Len(); i++ {
			if !arr.IsValid(i) {
				continue
			}
			v := arr.Value(i)
			if !found || v < min {
				min = v
			}
			if !found || v > max {
				max = v
			}
			found = true
		}
		if found {
			stats.MinFloat64 = &min
			stats.MaxFloat64 = &max
			minBytes := make([]byte, 8)
			encodeFloat64(minBytes, min)
			stats.MinValue = minBytes
			maxBytes := make([]byte, 8)
			encodeFloat64(maxBytes, max)
			stats.MaxValue = maxBytes
		}

	case *array.String:
		var min string
		var max string
		found := false
		for i := 0; i < arr.Len(); i++ {
			if !arr.IsValid(i) {
				continue
			}
			v := arr.Value(i)
			if !found || v < min {
				min = v
			}
			if !found || v > max {
				max = v
			}
			found = true
		}
		if found {
			stats.MinString = &min
			stats.MaxString = &max
			stats.MinValue = []byte(min)
			stats.MaxValue = []byte(max)
		}
	}

	return stats
}

type ColumnMetadata struct {
	Dataset    string
	Partition  string
	Column     string
	DataType   string
	NumChunks  int64
	TotalRows  int64
	TotalNulls int64
	MinValue   []byte
	MaxValue   []byte
}

func (m *ColumnMetadata) Encode() ([]byte, error) {
	buf := new(bytes.Buffer)

	binary.Write(buf, binary.BigEndian, m.NumChunks)
	binary.Write(buf, binary.BigEndian, m.TotalRows)
	binary.Write(buf, binary.BigEndian, m.TotalNulls)

	if m.MinValue != nil {
		binary.Write(buf, binary.BigEndian, int32(len(m.MinValue)))
		buf.Write(m.MinValue)
	} else {
		binary.Write(buf, binary.BigEndian, int32(0))
	}

	if m.MaxValue != nil {
		binary.Write(buf, binary.BigEndian, int32(len(m.MaxValue)))
		buf.Write(m.MaxValue)
	} else {
		binary.Write(buf, binary.BigEndian, int32(0))
	}

	buf.WriteString(m.DataType)

	return buf.Bytes(), nil
}

func DecodeColumnMetadata(data []byte) (*ColumnMetadata, error) {
	if len(data) == 0 {
		return nil, nil
	}

	buf := bytes.NewReader(data)
	meta := &ColumnMetadata{}

	if err := binary.Read(buf, binary.BigEndian, &meta.NumChunks); err != nil {
		return nil, err
	}
	if err := binary.Read(buf, binary.BigEndian, &meta.TotalRows); err != nil {
		return nil, err
	}
	if err := binary.Read(buf, binary.BigEndian, &meta.TotalNulls); err != nil {
		return nil, err
	}

	var minLen int32
	if err := binary.Read(buf, binary.BigEndian, &minLen); err != nil {
		return nil, err
	}
	if minLen > 0 {
		meta.MinValue = make([]byte, minLen)
		if _, err := buf.Read(meta.MinValue); err != nil {
			return nil, err
		}
	}

	var maxLen int32
	if err := binary.Read(buf, binary.BigEndian, &maxLen); err != nil {
		return nil, err
	}
	if maxLen > 0 {
		meta.MaxValue = make([]byte, maxLen)
		if _, err := buf.Read(meta.MaxValue); err != nil {
			return nil, err
		}
	}

	rest, err := io.ReadAll(buf)
	if err != nil {
		return nil, err
	}
	meta.DataType = string(rest)

	return meta, nil
}

func (d *DB) WriteColumnMetadata(ctx context.Context, meta *ColumnMetadata) error {
	return d.db.Update(func(tx *bbolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists([]byte(BucketMeta))
		if err != nil {
			return err
		}

		key := columnMetaKey(meta.Dataset, meta.Partition, meta.Column)
		data, err := meta.Encode()
		if err != nil {
			return err
		}

		return bucket.Put(key, data)
	})
}

func (d *DB) ReadColumnMetadata(ctx context.Context, dataset, partition, column string) (*ColumnMetadata, error) {
	var meta *ColumnMetadata

	err := d.db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte(BucketMeta))
		if bucket == nil {
			return nil
		}

		key := columnMetaKey(dataset, partition, column)
		data := bucket.Get(key)
		if data == nil {
			return nil
		}

		var err error
		meta, err = DecodeColumnMetadata(data)
		return err
	})

	return meta, err
}

func columnMetaKey(dataset, partition, column string) []byte {
	return []byte(fmt.Sprintf("%s/%s/%s/meta", dataset, partition, column))
}

type StatsWriter struct {
	db *DB
}

func NewStatsWriter(db *DB) *StatsWriter {
	return &StatsWriter{db: db}
}

func (w *StatsWriter) WriteChunkStat(arr arrow.Array, dataset, partition, column string, chunkID uint32) error {
	stats := ComputeArrayStats(arr, dataset, partition, column, chunkID)
	return w.db.WriteChunkStats(context.Background(), stats)
}

func (w *StatsWriter) WriteBatchStats(batch arrow.RecordBatch, dataset, partition string) error {
	for colIdx := 0; colIdx < int(batch.NumCols()); colIdx++ {
		arr := batch.Column(colIdx)
		colName := batch.ColumnName(colIdx)

		chunkStats := ComputeArrayStats(arr, dataset, partition, colName, 0)
		if err := w.db.WriteChunkStats(context.Background(), chunkStats); err != nil {
			return err
		}

		colMeta := &ColumnMetadata{
			Dataset:   dataset,
			Partition: partition,
			Column:    colName,
			DataType:  arr.DataType().Name(),
			NumChunks: 1,
			TotalRows: batch.NumRows(),
			MinValue:  chunkStats.MinValue,
			MaxValue:  chunkStats.MaxValue,
		}

		if err := w.db.WriteColumnMetadata(context.Background(), colMeta); err != nil {
			return err
		}
	}

	return nil
}

func (e *Engine) WriteChunkStats(ctx context.Context, stats *ChunkStats) error {
	return e.db.WriteChunkStats(ctx, stats)
}

func (e *Engine) ReadChunkStats(ctx context.Context, dataset, partition, column string, chunkID uint32) (*ChunkStats, error) {
	return e.db.ReadChunkStats(ctx, dataset, partition, column, chunkID)
}

func (e *Engine) ReadColumnStats(ctx context.Context, dataset, partition, column string) ([]ChunkStats, error) {
	return e.db.ReadColumnStats(ctx, dataset, partition, column)
}

func (e *Engine) ReadColumnMetadata(ctx context.Context, dataset, partition, column string) (*ColumnMetadata, error) {
	return e.db.ReadColumnMetadata(ctx, dataset, partition, column)
}

func (e *Engine) ScanChunkStats(ctx context.Context, dataset, partition, column string, fn func(*ChunkStats) error) error {
	return e.db.ScanChunkStats(ctx, dataset, partition, column, fn)
}
