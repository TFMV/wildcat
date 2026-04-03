package storage

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"

	"go.etcd.io/bbolt"
)

const (
	BucketData = "data"
	BucketMeta = "meta"

	ChunkIDSize     = 4
	KeyPartSep      = 0xFF
	MaxDatasetLen   = 64
	MaxPartitionLen = 64
	MaxColumnLen    = 64
)

var (
	ErrInvalidKey     = errors.New("invalid key format")
	ErrNotFound       = errors.New("key not found")
	ErrInvalidDataset = errors.New("invalid dataset name")
)

type DB struct {
	db *bbolt.DB
}

func Open(path string) (*DB, error) {
	db, err := bbolt.Open(path, 0600, nil)
	if err != nil {
		return nil, err
	}
	return &DB{db: db}, nil
}

func (d *DB) Close() error {
	return d.db.Close()
}

func (d *DB) Update(fn func(tx *bbolt.Tx) error) error {
	return d.db.Update(fn)
}

func (d *DB) View(fn func(tx *bbolt.Tx) error) error {
	return d.db.View(fn)
}

type ChunkIterator struct {
	cursor  *bbolt.Cursor
	prefix  []byte
	current []byte
}

func (i *ChunkIterator) Next() (chunkID uint32, data []byte, err error) {
	k, v := i.cursor.Next()
	if k == nil {
		return 0, nil, nil
	}

	if !bytes.HasPrefix(k, i.prefix) {
		return 0, nil, nil
	}

	chunkID, err = ExtractChunkID(k)
	if err != nil {
		return 0, nil, err
	}

	return chunkID, v, nil
}

func (i *ChunkIterator) Close() {}

func (d *DB) PutColumnChunk(ctx context.Context, dataset, partition, column string, chunkID uint32, data []byte) error {
	return d.db.Update(func(tx *bbolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists([]byte(BucketData))
		if err != nil {
			return err
		}
		key := EncodeKey(dataset, partition, column, chunkID)
		return bucket.Put(key, data)
	})
}

func (d *DB) GetColumnChunks(ctx context.Context, dataset, partition, column string) *ChunkIterator {
	tx, _ := d.db.Begin(false)
	bucket := tx.Bucket([]byte(BucketData))

	prefix := EncodeKeyPrefix(dataset, partition, column)
	cursor := bucket.Cursor()

	it := &ChunkIterator{
		cursor: cursor,
		prefix: prefix,
	}
	it.current, _ = cursor.Seek(prefix)

	return it
}

func (d *DB) ScanColumnChunks(ctx context.Context, dataset, partition, column string, fn func(chunkID uint32, data []byte) error) error {
	return d.db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte(BucketData))
		if bucket == nil {
			return nil
		}

		prefix := EncodeKeyPrefix(dataset, partition, column)
		cursor := bucket.Cursor()

		for k, v := cursor.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, v = cursor.Next() {
			chunkID, err := ExtractChunkID(k)
			if err != nil {
				continue
			}
			if err := fn(chunkID, v); err != nil {
				return err
			}
		}
		return nil
	})
}

func (d *DB) ScanColumnChunksWithPredicates(ctx context.Context, dataset, partition, column string,
	predicates []Predicate, fn func(chunkID uint32, data []byte) error) error {

	return d.db.View(func(tx *bbolt.Tx) error {
		dataBucket := tx.Bucket([]byte(BucketData))
		if dataBucket == nil {
			return nil
		}

		metaBucket := tx.Bucket([]byte(BucketMeta))

		prefix := EncodeKeyPrefix(dataset, partition, column)
		statsPrefix := chunkStatsPrefix(dataset, partition, column)

		statsMap := make(map[uint32]*ChunkStats)

		if metaBucket != nil && len(predicates) > 0 {
			statsCursor := metaBucket.Cursor()
			for k, v := statsCursor.Seek(statsPrefix); k != nil && bytes.HasPrefix(k, statsPrefix); k, v = statsCursor.Next() {
				if stats, err := DecodeChunkStats(v); err == nil && stats != nil {
					statsMap[stats.ChunkID] = stats
				}
			}
		}

		cursor := dataBucket.Cursor()
		for k, v := cursor.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, v = cursor.Next() {
			chunkID, err := ExtractChunkID(k)
			if err != nil {
				continue
			}

			if len(predicates) > 0 {
				if stats, ok := statsMap[chunkID]; ok {
					shouldSkip := false
					for _, pred := range predicates {
						if ShouldSkipChunk(stats.MinValue, stats.MaxValue, pred) {
							shouldSkip = true
							break
						}
					}
					if shouldSkip {
						continue
					}
				}
			}

			if err := fn(chunkID, v); err != nil {
				return err
			}
		}
		return nil
	})
}

type Metadata struct {
	RowCount   int64
	ChunkSize  int64
	NumChunks  int
	MinValues  map[string]interface{}
	MaxValues  map[string]interface{}
	NullCounts map[string]int64
}

func (d *DB) StoreMetadata(ctx context.Context, dataset, partition string, meta *Metadata) error {
	return d.db.Update(func(tx *bbolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists([]byte(BucketMeta))
		if err != nil {
			return err
		}

		key := metadataKey(dataset, partition)
		data, err := encodeMetadata(meta)
		if err != nil {
			return err
		}

		return bucket.Put(key, data)
	})
}

func (d *DB) LoadMetadata(ctx context.Context, dataset, partition string) (*Metadata, error) {
	var meta *Metadata
	err := d.db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte(BucketMeta))
		if bucket == nil {
			return nil
		}

		key := metadataKey(dataset, partition)
		data := bucket.Get(key)
		if data == nil {
			return nil
		}

		var err error
		meta, err = decodeMetadata(data)
		return err
	})
	return meta, err
}

func (d *DB) ListPartitions(ctx context.Context, dataset string) ([]string, error) {
	var partitions []string
	err := d.db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte(BucketMeta))
		if bucket == nil {
			return nil
		}

		prefix := []byte(dataset + "/")
		cursor := bucket.Cursor()

		for k, _ := cursor.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, _ = cursor.Next() {
			parts := bytes.Split(k, []byte("/"))
			if len(parts) >= 2 {
				part := string(parts[1])
				if !contains(partitions, part) {
					partitions = append(partitions, part)
				}
			}
		}
		return nil
	})
	return partitions, err
}

func contains(slice []string, s string) bool {
	for _, v := range slice {
		if v == s {
			return true
		}
	}
	return false
}

func EncodeKeyTo(buf *bytes.Buffer, dataset, partition, column string, chunkID uint32) {
	buf.WriteString(dataset)
	buf.WriteByte(0)
	buf.WriteString(partition)
	buf.WriteByte(0)
	buf.WriteString(column)
	buf.WriteByte(0)

	var chunkBytes [ChunkIDSize]byte
	binary.BigEndian.PutUint32(chunkBytes[:], chunkID)
	buf.Write(chunkBytes[:])
}

func EncodeKey(dataset, partition, column string, chunkID uint32) []byte {
	var buf bytes.Buffer
	buf.Grow(len(dataset) + 1 + len(partition) + 1 + len(column) + 1 + ChunkIDSize)
	EncodeKeyTo(&buf, dataset, partition, column, chunkID)
	return append([]byte(nil), buf.Bytes()...)
}

func EncodeKeyPrefixTo(buf *bytes.Buffer, dataset, partition, column string) {
	buf.WriteString(dataset)
	buf.WriteByte(0)
	buf.WriteString(partition)
	buf.WriteByte(0)
	buf.WriteString(column)
	buf.WriteByte(0)
}

func EncodeKeyPrefix(dataset, partition, column string) []byte {
	var buf bytes.Buffer
	buf.Grow(len(dataset) + 1 + len(partition) + 1 + len(column) + 1)
	EncodeKeyPrefixTo(&buf, dataset, partition, column)
	return append([]byte(nil), buf.Bytes()...)
}

func ExtractChunkID(key []byte) (uint32, error) {
	// Key format: dataset\0partition\0column\0[4 bytes chunkID]
	// Find last null byte, then read next 4 bytes

	if len(key) < ChunkIDSize+3 {
		return 0, ErrInvalidKey
	}

	lastSep := bytes.LastIndex(key, []byte{0})
	if lastSep < 0 || len(key) < lastSep+1+ChunkIDSize {
		return 0, ErrInvalidKey
	}

	chunkBytes := key[lastSep+1 : lastSep+1+ChunkIDSize]
	return binary.BigEndian.Uint32(chunkBytes), nil
}

func ParseKey(key []byte) (dataset, partition, column string, chunkID uint32, err error) {
	parts := bytes.Split(key, []byte{0})
	if len(parts) != 5 {
		return "", "", "", 0, ErrInvalidKey
	}

	dataset = string(parts[0])
	partition = string(parts[1])
	column = string(parts[2])
	chunkID = binary.BigEndian.Uint32(parts[3])

	return dataset, partition, column, chunkID, nil
}

func metadataKey(dataset, partition string) []byte {
	return []byte(dataset + "/" + partition + "/meta")
}

func encodeMetadata(meta *Metadata) ([]byte, error) {
	// Simple binary encoding for metadata
	buf := new(bytes.Buffer)

	binary.Write(buf, binary.BigEndian, meta.RowCount)
	binary.Write(buf, binary.BigEndian, meta.ChunkSize)
	binary.Write(buf, binary.BigEndian, int32(meta.NumChunks))

	// Store min/max/null count maps as simpleKV pairs
	if meta.MinValues != nil {
		binary.Write(buf, binary.BigEndian, int32(len(meta.MinValues)))
		for k, v := range meta.MinValues {
			buf.WriteString(k)
			fmt.Fprintf(buf, "%v", v)
		}
	} else {
		binary.Write(buf, binary.BigEndian, int32(0))
	}

	return buf.Bytes(), nil
}

func decodeMetadata(data []byte) (*Metadata, error) {
	buf := bytes.NewReader(data)

	meta := &Metadata{}

	if err := binary.Read(buf, binary.BigEndian, &meta.RowCount); err != nil {
		return nil, err
	}
	if err := binary.Read(buf, binary.BigEndian, &meta.ChunkSize); err != nil {
		return nil, err
	}
	var numChunks int32
	if err := binary.Read(buf, binary.BigEndian, &numChunks); err != nil {
		return nil, err
	}
	meta.NumChunks = int(numChunks)

	return meta, nil
}
