package format

import (
	"bytes"
	"io"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

type Record = arrow.Record
type RecordBatch = arrow.RecordBatch
type Array = arrow.Array
type Schema = arrow.Schema

const (
	DefaultChunkSize = 65536
)

type Chunk struct {
	ColumnName string
	Array      arrow.Array
	ChunkID    uint32
	RowOffset  int64
	NumRows    int64
}

type ColumnChunks struct {
	Column string
	Chunks []Chunk
}

func SplitRecordBatch(batch arrow.RecordBatch, chunkSize int) []ColumnChunks {
	if batch == nil {
		return nil
	}

	if chunkSize <= 0 {
		chunkSize = DefaultChunkSize
	}

	numRows := int(batch.NumRows())
	if numRows == 0 {
		return nil
	}

	result := make([]ColumnChunks, 0, int(batch.NumCols()))

	for colIdx := 0; colIdx < int(batch.NumCols()); colIdx++ {
		colName := batch.ColumnName(colIdx)
		col := batch.Column(colIdx)

		cc := ColumnChunks{
			Column: colName,
			Chunks: make([]Chunk, 0, (numRows+chunkSize-1)/chunkSize),
		}

		var chunkID uint32
		for start := 0; start < numRows; start += chunkSize {
			end := start + chunkSize
			if end > numRows {
				end = numRows
			}

			chunkArr := array.NewSlice(col, int64(start), int64(end))

			cc.Chunks = append(cc.Chunks, Chunk{
				ColumnName: colName,
				Array:      chunkArr,
				ChunkID:    chunkID,
				RowOffset:  int64(start),
				NumRows:    int64(end - start),
			})

			chunkID++
		}

		result = append(result, cc)
	}

	return result
}

func SerializeColumn(arr arrow.Array) ([]byte, error) {
	if arr == nil {
		return nil, nil
	}

	fields := []arrow.Field{
		{Name: "col", Type: arr.DataType()},
	}
	schema := arrow.NewSchema(fields, nil)

	var buf bytes.Buffer
	w := ipc.NewWriter(&buf, ipc.WithSchema(schema))
	defer w.Close()

	batch := array.NewRecordBatch(schema, []arrow.Array{arr}, int64(arr.Len()))
	defer batch.Release()

	if err := w.Write(batch); err != nil {
		return nil, err
	}

	if err := w.Close(); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func DeserializeColumn(data []byte) (arrow.Array, error) {
	if len(data) == 0 {
		return nil, nil
	}

	reader, err := ipc.NewReader(bytes.NewReader(data), ipc.WithAllocator(memory.NewGoAllocator()))
	if err != nil {
		return nil, err
	}
	defer reader.Release()

	batch, err := reader.Read()
	if err != nil {
		return nil, err
	}
	defer batch.Release()

	if batch.NumCols() == 0 {
		return nil, nil
	}

	return batch.Column(0), nil
}

func SerializeColumnChunk(chunk Chunk) ([]byte, error) {
	return SerializeColumn(chunk.Array)
}

func DeserializeColumnChunk(data []byte) (Chunk, error) {
	arr, err := DeserializeColumn(data)
	if err != nil {
		return Chunk{}, err
	}
	return Chunk{Array: arr}, nil
}

func MergeColumnChunks(chunks []Chunk) arrow.Array {
	if len(chunks) == 0 {
		return nil
	}

	if len(chunks) == 1 {
		return chunks[0].Array
	}

	first := chunks[0].Array
	mem := memory.DefaultAllocator

	switch arr := first.(type) {
	case *array.Int64:
		return mergeInt64Array(arr, chunks, mem)
	case *array.Int32:
		return mergeInt32Array(arr, chunks, mem)
	case *array.Int16:
		return mergeInt16Array(arr, chunks, mem)
	case *array.Int8:
		return mergeInt8Array(arr, chunks, mem)
	case *array.Uint64:
		return mergeUint64Array(arr, chunks, mem)
	case *array.Uint32:
		return mergeUint32Array(arr, chunks, mem)
	case *array.Uint16:
		return mergeUint16Array(arr, chunks, mem)
	case *array.Uint8:
		return mergeUint8Array(arr, chunks, mem)
	case *array.Float64:
		return mergeFloat64Array(arr, chunks, mem)
	case *array.Float32:
		return mergeFloat32Array(arr, chunks, mem)
	case *array.String:
		return mergeStringArray(arr, chunks, mem)
	case *array.Boolean:
		return mergeBooleanArray(arr, chunks, mem)
	default:
		return mergeGenericArray(first, chunks, mem)
	}
}

func mergeInt64Array(first *array.Int64, chunks []Chunk, mem memory.Allocator) arrow.Array {
	totalRows := 0
	for _, c := range chunks {
		totalRows += int(c.NumRows)
	}

	b := array.NewInt64Builder(mem)
	b.Reserve(totalRows)

	for _, c := range chunks {
		arr := c.Array.(*array.Int64)
		for i := 0; i < arr.Len(); i++ {
			if arr.IsValid(i) {
				b.Append(arr.Value(i))
			} else {
				b.AppendNull()
			}
		}
	}

	return b.NewArray()
}

func mergeInt32Array(first *array.Int32, chunks []Chunk, mem memory.Allocator) arrow.Array {
	totalRows := 0
	for _, c := range chunks {
		totalRows += int(c.NumRows)
	}

	b := array.NewInt32Builder(mem)
	b.Reserve(totalRows)

	for _, c := range chunks {
		arr := c.Array.(*array.Int32)
		for i := 0; i < arr.Len(); i++ {
			if arr.IsValid(i) {
				b.Append(arr.Value(i))
			} else {
				b.AppendNull()
			}
		}
	}

	return b.NewArray()
}

func mergeInt16Array(first *array.Int16, chunks []Chunk, mem memory.Allocator) arrow.Array {
	totalRows := 0
	for _, c := range chunks {
		totalRows += int(c.NumRows)
	}

	b := array.NewInt16Builder(mem)
	b.Reserve(totalRows)

	for _, c := range chunks {
		arr := c.Array.(*array.Int16)
		for i := 0; i < arr.Len(); i++ {
			if arr.IsValid(i) {
				b.Append(arr.Value(i))
			} else {
				b.AppendNull()
			}
		}
	}

	return b.NewArray()
}

func mergeInt8Array(first *array.Int8, chunks []Chunk, mem memory.Allocator) arrow.Array {
	totalRows := 0
	for _, c := range chunks {
		totalRows += int(c.NumRows)
	}

	b := array.NewInt8Builder(mem)
	b.Reserve(totalRows)

	for _, c := range chunks {
		arr := c.Array.(*array.Int8)
		for i := 0; i < arr.Len(); i++ {
			if arr.IsValid(i) {
				b.Append(arr.Value(i))
			} else {
				b.AppendNull()
			}
		}
	}

	return b.NewArray()
}

func mergeUint64Array(first *array.Uint64, chunks []Chunk, mem memory.Allocator) arrow.Array {
	totalRows := 0
	for _, c := range chunks {
		totalRows += int(c.NumRows)
	}

	b := array.NewUint64Builder(mem)
	b.Reserve(totalRows)

	for _, c := range chunks {
		arr := c.Array.(*array.Uint64)
		for i := 0; i < arr.Len(); i++ {
			if arr.IsValid(i) {
				b.Append(arr.Value(i))
			} else {
				b.AppendNull()
			}
		}
	}

	return b.NewArray()
}

func mergeUint32Array(first *array.Uint32, chunks []Chunk, mem memory.Allocator) arrow.Array {
	totalRows := 0
	for _, c := range chunks {
		totalRows += int(c.NumRows)
	}

	b := array.NewUint32Builder(mem)
	b.Reserve(totalRows)

	for _, c := range chunks {
		arr := c.Array.(*array.Uint32)
		for i := 0; i < arr.Len(); i++ {
			if arr.IsValid(i) {
				b.Append(arr.Value(i))
			} else {
				b.AppendNull()
			}
		}
	}

	return b.NewArray()
}

func mergeUint16Array(first *array.Uint16, chunks []Chunk, mem memory.Allocator) arrow.Array {
	totalRows := 0
	for _, c := range chunks {
		totalRows += int(c.NumRows)
	}

	b := array.NewUint16Builder(mem)
	b.Reserve(totalRows)

	for _, c := range chunks {
		arr := c.Array.(*array.Uint16)
		for i := 0; i < arr.Len(); i++ {
			if arr.IsValid(i) {
				b.Append(arr.Value(i))
			} else {
				b.AppendNull()
			}
		}
	}

	return b.NewArray()
}

func mergeUint8Array(first *array.Uint8, chunks []Chunk, mem memory.Allocator) arrow.Array {
	totalRows := 0
	for _, c := range chunks {
		totalRows += int(c.NumRows)
	}

	b := array.NewUint8Builder(mem)
	b.Reserve(totalRows)

	for _, c := range chunks {
		arr := c.Array.(*array.Uint8)
		for i := 0; i < arr.Len(); i++ {
			if arr.IsValid(i) {
				b.Append(arr.Value(i))
			} else {
				b.AppendNull()
			}
		}
	}

	return b.NewArray()
}

func mergeFloat64Array(first *array.Float64, chunks []Chunk, mem memory.Allocator) arrow.Array {
	totalRows := 0
	for _, c := range chunks {
		totalRows += int(c.NumRows)
	}

	b := array.NewFloat64Builder(mem)
	b.Reserve(totalRows)

	for _, c := range chunks {
		arr := c.Array.(*array.Float64)
		for i := 0; i < arr.Len(); i++ {
			if arr.IsValid(i) {
				b.Append(arr.Value(i))
			} else {
				b.AppendNull()
			}
		}
	}

	return b.NewArray()
}

func mergeFloat32Array(first *array.Float32, chunks []Chunk, mem memory.Allocator) arrow.Array {
	totalRows := 0
	for _, c := range chunks {
		totalRows += int(c.NumRows)
	}

	b := array.NewFloat32Builder(mem)
	b.Reserve(totalRows)

	for _, c := range chunks {
		arr := c.Array.(*array.Float32)
		for i := 0; i < arr.Len(); i++ {
			if arr.IsValid(i) {
				b.Append(arr.Value(i))
			} else {
				b.AppendNull()
			}
		}
	}

	return b.NewArray()
}

func mergeStringArray(first *array.String, chunks []Chunk, mem memory.Allocator) arrow.Array {
	totalRows := 0
	for _, c := range chunks {
		totalRows += int(c.NumRows)
	}

	b := array.NewStringBuilder(mem)
	b.Reserve(totalRows)

	for _, c := range chunks {
		arr := c.Array.(*array.String)
		for i := 0; i < arr.Len(); i++ {
			if arr.IsValid(i) {
				b.Append(arr.Value(i))
			} else {
				b.AppendNull()
			}
		}
	}

	return b.NewArray()
}

func mergeBooleanArray(first *array.Boolean, chunks []Chunk, mem memory.Allocator) arrow.Array {
	totalRows := 0
	for _, c := range chunks {
		totalRows += int(c.NumRows)
	}

	b := array.NewBooleanBuilder(mem)
	b.Reserve(totalRows)

	for _, c := range chunks {
		arr := c.Array.(*array.Boolean)
		for i := 0; i < arr.Len(); i++ {
			if arr.IsValid(i) {
				b.Append(arr.Value(i))
			} else {
				b.AppendNull()
			}
		}
	}

	return b.NewArray()
}

func mergeGenericArray(first arrow.Array, chunks []Chunk, mem memory.Allocator) arrow.Array {
	totalRows := 0
	for _, c := range chunks {
		totalRows += int(c.NumRows)
	}

	switch first.(type) {
	case *array.Binary:
		b := array.NewBinaryBuilder(mem, arrow.BinaryTypes.Binary)
		b.Reserve(totalRows)
		for _, c := range chunks {
			arr := c.Array.(*array.Binary)
			for i := 0; i < arr.Len(); i++ {
				if arr.IsValid(i) {
					b.Append(arr.Value(i))
				} else {
					b.AppendNull()
				}
			}
		}
		return b.NewArray()
	case *array.LargeString:
		b := array.NewLargeStringBuilder(mem)
		b.Reserve(totalRows)
		for _, c := range chunks {
			arr := c.Array.(*array.LargeString)
			for i := 0; i < arr.Len(); i++ {
				if arr.IsValid(i) {
					b.Append(arr.Value(i))
				} else {
					b.AppendNull()
				}
			}
		}
		return b.NewArray()
	default:
		return nil
	}
}

func EncodeBatch(batch arrow.RecordBatch) ([]byte, error) {
	var buf bytes.Buffer
	w := ipc.NewWriter(&buf, ipc.WithSchema(batch.Schema()))
	defer w.Close()

	if err := w.Write(batch); err != nil {
		return nil, err
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func DecodeBatch(data []byte) (arrow.RecordBatch, error) {
	reader, err := ipc.NewReader(bytes.NewReader(data), ipc.WithAllocator(memory.NewGoAllocator()))
	if err != nil {
		return nil, err
	}
	defer reader.Release()

	return reader.Read()
}

func NewRecordBatch(schema *arrow.Schema, columns []arrow.Array, numRows int64) arrow.RecordBatch {
	return array.NewRecordBatch(schema, columns, numRows)
}

type Encoder struct {
	w *ipc.Writer
}

func NewEncoder(w io.Writer) (*Encoder, error) {
	enc := &Encoder{
		w: ipc.NewWriter(w, ipc.WithAllocator(memory.NewGoAllocator())),
	}
	return enc, nil
}

func (e *Encoder) WriteBatch(batch arrow.RecordBatch) error {
	return e.w.Write(batch)
}

func (e *Encoder) Close() error {
	return e.w.Close()
}

type Decoder struct {
	r *ipc.Reader
}

func NewDecoder(r io.Reader) (*Decoder, error) {
	reader, err := ipc.NewReader(r, ipc.WithAllocator(memory.NewGoAllocator()))
	if err != nil {
		return nil, err
	}
	dec := &Decoder{r: reader}
	return dec, nil
}

func (d *Decoder) ReadBatch() (arrow.RecordBatch, error) {
	return d.r.Read()
}

func (d *Decoder) Schema() *arrow.Schema {
	return d.r.Schema()
}

func (d *Decoder) Close() error {
	d.r.Release()
	return nil
}
