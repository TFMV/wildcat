package storage

import (
	"math"
	"sync"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

type Predicate struct {
	Column string
	Type   PredicateType
	Value  interface{}
}

type PredicateType string

const (
	PredicateEq  PredicateType = "="
	PredicateNeq PredicateType = "!="
	PredicateLt  PredicateType = "<"
	PredicateLte PredicateType = "<="
	PredicateGt  PredicateType = ">"
	PredicateGte PredicateType = ">="
)

func ShouldSkipChunk(minBytes, maxBytes []byte, pred Predicate) bool {
	if minBytes == nil || maxBytes == nil {
		return false
	}

	switch pred.Type {
	case PredicateEq:
		return !valueInRange(minBytes, maxBytes, pred.Value)
	case PredicateNeq:
		return false
	case PredicateLt:
		return !canBeLessThan(maxBytes, pred.Value)
	case PredicateLte:
		return !canBeLessThanOrEqual(maxBytes, pred.Value)
	case PredicateGt:
		return !canBeGreaterThan(minBytes, pred.Value)
	case PredicateGte:
		return !canBeGreaterThanOrEqual(minBytes, pred.Value)
	}
	return false
}

func valueInRange(minBytes, maxBytes []byte, value interface{}) bool {
	valBytes, ok := valueToBytes(value)
	if !ok {
		return true
	}
	return bytesCompare(valBytes, minBytes) >= 0 && bytesCompare(valBytes, maxBytes) <= 0
}

func canBeLessThan(maxBytes []byte, value interface{}) bool {
	valBytes, ok := valueToBytes(value)
	if !ok {
		return true
	}
	return bytesCompare(maxBytes, valBytes) > 0
}

func canBeLessThanOrEqual(maxBytes []byte, value interface{}) bool {
	valBytes, ok := valueToBytes(value)
	if !ok {
		return true
	}
	return bytesCompare(maxBytes, valBytes) >= 0
}

func canBeGreaterThan(minBytes []byte, value interface{}) bool {
	valBytes, ok := valueToBytes(value)
	if !ok {
		return true
	}
	return bytesCompare(minBytes, valBytes) < 0
}

func canBeGreaterThanOrEqual(minBytes []byte, value interface{}) bool {
	valBytes, ok := valueToBytes(value)
	if !ok {
		return true
	}
	return bytesCompare(minBytes, valBytes) <= 0
}

func valueToBytes(value interface{}) ([]byte, bool) {
	switch v := value.(type) {
	case int64:
		b := make([]byte, 8)
		encodeBigEndian(b, uint64(v))
		return b, true
	case int:
		b := make([]byte, 8)
		encodeBigEndian(b, uint64(v))
		return b, true
	case float64:
		b := make([]byte, 8)
		encodeFloat64(b, v)
		return b, true
	case string:
		return []byte(v), true
	default:
		return nil, false
	}
}

func encodeBigEndian(b []byte, v uint64) {
	b[0] = byte(v >> 56)
	b[1] = byte(v >> 48)
	b[2] = byte(v >> 40)
	b[3] = byte(v >> 32)
	b[4] = byte(v >> 24)
	b[5] = byte(v >> 16)
	b[6] = byte(v >> 8)
	b[7] = byte(v)
}

func encodeFloat64(b []byte, v float64) {
	bits := math.Float64bits(v)
	encodeBigEndian(b, bits)
}

func bytesCompare(a, b []byte) int {
	if len(a) != len(b) {
		if len(a) < len(b) {
			return -1
		}
		return 1
	}
	for i := 0; i < len(a); i++ {
		if a[i] < b[i] {
			return -1
		}
		if a[i] > b[i] {
			return 1
		}
	}
	return 0
}

type BuilderPool struct {
	int64   *array.Int64Builder
	int32   *array.Int32Builder
	float64 *array.Float64Builder
	string  *array.StringBuilder
	mu      sync.Mutex
}

func NewBuilderPool() *BuilderPool {
	return &BuilderPool{}
}

func (p *BuilderPool) GetInt64(mem memory.Allocator) *array.Int64Builder {
	p.mu.Lock()
	defer p.mu.Unlock()
	if b := p.int64; b != nil {
		p.int64 = nil
		return b
	}
	return array.NewInt64Builder(mem)
}

func (p *BuilderPool) ReturnInt64(b *array.Int64Builder) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.int64 = b
}

func (p *BuilderPool) GetInt32(mem memory.Allocator) *array.Int32Builder {
	p.mu.Lock()
	defer p.mu.Unlock()
	if b := p.int32; b != nil {
		p.int32 = nil
		return b
	}
	return array.NewInt32Builder(mem)
}

func (p *BuilderPool) ReturnInt32(b *array.Int32Builder) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.int32 = b
}

func (p *BuilderPool) GetFloat64(mem memory.Allocator) *array.Float64Builder {
	p.mu.Lock()
	defer p.mu.Unlock()
	if b := p.float64; b != nil {
		p.float64 = nil
		return b
	}
	return array.NewFloat64Builder(mem)
}

func (p *BuilderPool) ReturnFloat64(b *array.Float64Builder) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.float64 = b
}

func (p *BuilderPool) GetString(mem memory.Allocator) *array.StringBuilder {
	p.mu.Lock()
	defer p.mu.Unlock()
	if b := p.string; b != nil {
		p.string = nil
		return b
	}
	return array.NewStringBuilder(mem)
}

func (p *BuilderPool) ReturnString(b *array.StringBuilder) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.string = b
}

var defaultBuilderPool = NewBuilderPool()
