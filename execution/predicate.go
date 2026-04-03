package execution

import (
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func (p *Predicate) String() string {
	return fmt.Sprintf("%s %s %v", p.Column, p.Type, p.Value)
}

func toStringPred(v interface{}) string {
	switch v := v.(type) {
	case string:
		return v
	case int64:
		return string(rune(v))
	case float64:
		return string(rune(int(v)))
	default:
		return ""
	}
}

func ApplyPredicate(arr arrow.Array, pred Predicate, mem memory.Allocator) (arrow.Array, error) {
	if arr == nil {
		return nil, nil
	}

	if mem == nil {
		mem = memory.DefaultAllocator
	}

	switch pred.Type {
	case PredicateEq:
		return filterEqual(arr, pred.Value, mem)
	case PredicateNeq:
		return filterNotEqual(arr, pred.Value, mem)
	case PredicateLt:
		return filterLess(arr, pred.Value, mem)
	case PredicateLte:
		return filterLessEqual(arr, pred.Value, mem)
	case PredicateGt:
		return filterGreater(arr, pred.Value, mem)
	case PredicateGte:
		return filterGreaterEqual(arr, pred.Value, mem)
	default:
		return arr, nil
	}
}

func ShouldSkipChunk(minVal, maxVal interface{}, pred Predicate) bool {
	if minVal == nil || maxVal == nil {
		return false
	}

	switch pred.Type {
	case PredicateEq:
		return !containsValue(minVal, maxVal, pred.Value)
	case PredicateNeq:
		return false
	case PredicateLt:
		return !canContainLessThan(maxVal, pred.Value)
	case PredicateLte:
		return !canContainLessThanOrEqual(maxVal, pred.Value)
	case PredicateGt:
		return !canContainGreaterThan(minVal, pred.Value)
	case PredicateGte:
		return !canContainGreaterThanOrEqual(minVal, pred.Value)
	}
	return false
}

func filterEqual(arr arrow.Array, value interface{}, mem memory.Allocator) (arrow.Array, error) {
	switch arr := arr.(type) {
	case *array.Int64:
		return filterInt64Equal(arr, value, mem)
	case *array.Int32:
		return filterInt32Equal(arr, value, mem)
	case *array.Int16:
		return filterInt16Equal(arr, value, mem)
	case *array.Int8:
		return filterInt8Equal(arr, value, mem)
	case *array.Uint64:
		return filterUint64Equal(arr, value, mem)
	case *array.Uint32:
		return filterUint32Equal(arr, value, mem)
	case *array.Uint16:
		return filterUint16Equal(arr, value, mem)
	case *array.Uint8:
		return filterUint8Equal(arr, value, mem)
	case *array.Float64:
		return filterFloat64Equal(arr, value, mem)
	case *array.Float32:
		return filterFloat32Equal(arr, value, mem)
	case *array.String:
		return filterStringEqual(arr, value, mem)
	case *array.Boolean:
		return filterBooleanEqual(arr, value, mem)
	default:
		return filterGeneric(arr, value, mem)
	}
}

func filterNotEqual(arr arrow.Array, value interface{}, mem memory.Allocator) (arrow.Array, error) {
	eq, err := filterEqual(arr, value, mem)
	if err != nil || eq == nil {
		return eq, err
	}

	return invertFilter(eq, arr.Len(), mem)
}

func filterLess(arr arrow.Array, value interface{}, mem memory.Allocator) (arrow.Array, error) {
	switch arr := arr.(type) {
	case *array.Int64:
		return filterInt64Less(arr, value, mem)
	case *array.Int32:
		return filterInt32Less(arr, value, mem)
	case *array.Float64:
		return filterFloat64Less(arr, value, mem)
	case *array.Float32:
		return filterFloat32Less(arr, value, mem)
	case *array.String:
		return filterStringLess(arr, value, mem)
	default:
		return filterGenericLess(arr, value, mem)
	}
}

func filterLessEqual(arr arrow.Array, value interface{}, mem memory.Allocator) (arrow.Array, error) {
	switch arr := arr.(type) {
	case *array.Int64:
		return filterInt64LessEqual(arr, value, mem)
	case *array.Int32:
		return filterInt32LessEqual(arr, value, mem)
	case *array.Float64:
		return filterFloat64LessEqual(arr, value, mem)
	case *array.Float32:
		return filterFloat32LessEqual(arr, value, mem)
	default:
		return filterGenericLess(arr, value, mem)
	}
}

func filterGreater(arr arrow.Array, value interface{}, mem memory.Allocator) (arrow.Array, error) {
	switch arr := arr.(type) {
	case *array.Int64:
		return filterInt64Greater(arr, value, mem)
	case *array.Int32:
		return filterInt32Greater(arr, value, mem)
	case *array.Float64:
		return filterFloat64Greater(arr, value, mem)
	case *array.Float32:
		return filterFloat32Greater(arr, value, mem)
	default:
		return filterGenericGreater(arr, value, mem)
	}
}

func filterGreaterEqual(arr arrow.Array, value interface{}, mem memory.Allocator) (arrow.Array, error) {
	switch arr := arr.(type) {
	case *array.Int64:
		return filterInt64GreaterEqual(arr, value, mem)
	case *array.Int32:
		return filterInt32GreaterEqual(arr, value, mem)
	case *array.Float64:
		return filterFloat64GreaterEqual(arr, value, mem)
	case *array.Float32:
		return filterFloat32GreaterEqual(arr, value, mem)
	default:
		return filterGenericGreater(arr, value, mem)
	}
}

func filterInt64Equal(arr *array.Int64, value interface{}, mem memory.Allocator) (arrow.Array, error) {
	target, ok := toInt64(value)
	if !ok {
		return arr, nil
	}

	b := array.NewInt64Builder(mem)
	b.Reserve(arr.Len())

	for i := 0; i < arr.Len(); i++ {
		if arr.IsValid(i) && arr.Value(i) == target {
			b.Append(arr.Value(i))
		} else if !arr.IsValid(i) {
			b.AppendNull()
		}
	}

	return b.NewArray(), nil
}

func filterInt32Equal(arr *array.Int32, value interface{}, mem memory.Allocator) (arrow.Array, error) {
	target, ok := toInt32(value)
	if !ok {
		return arr, nil
	}

	b := array.NewInt32Builder(mem)
	b.Reserve(arr.Len())

	for i := 0; i < arr.Len(); i++ {
		if arr.IsValid(i) && arr.Value(i) == target {
			b.Append(arr.Value(i))
		} else if !arr.IsValid(i) {
			b.AppendNull()
		}
	}

	return b.NewArray(), nil
}

func filterInt16Equal(arr *array.Int16, value interface{}, mem memory.Allocator) (arrow.Array, error) {
	target, ok := toInt16(value)
	if !ok {
		return arr, nil
	}

	b := array.NewInt16Builder(mem)
	b.Reserve(arr.Len())

	for i := 0; i < arr.Len(); i++ {
		if arr.IsValid(i) && arr.Value(i) == target {
			b.Append(arr.Value(i))
		} else if !arr.IsValid(i) {
			b.AppendNull()
		}
	}

	return b.NewArray(), nil
}

func filterInt8Equal(arr *array.Int8, value interface{}, mem memory.Allocator) (arrow.Array, error) {
	target, ok := toInt8(value)
	if !ok {
		return arr, nil
	}

	b := array.NewInt8Builder(mem)
	b.Reserve(arr.Len())

	for i := 0; i < arr.Len(); i++ {
		if arr.IsValid(i) && arr.Value(i) == target {
			b.Append(arr.Value(i))
		} else if !arr.IsValid(i) {
			b.AppendNull()
		}
	}

	return b.NewArray(), nil
}

func filterUint64Equal(arr *array.Uint64, value interface{}, mem memory.Allocator) (arrow.Array, error) {
	target, ok := toUint64(value)
	if !ok {
		return arr, nil
	}

	b := array.NewUint64Builder(mem)
	b.Reserve(arr.Len())

	for i := 0; i < arr.Len(); i++ {
		if arr.IsValid(i) && arr.Value(i) == target {
			b.Append(arr.Value(i))
		} else if !arr.IsValid(i) {
			b.AppendNull()
		}
	}

	return b.NewArray(), nil
}

func filterUint32Equal(arr *array.Uint32, value interface{}, mem memory.Allocator) (arrow.Array, error) {
	target, ok := toUint32(value)
	if !ok {
		return arr, nil
	}

	b := array.NewUint32Builder(mem)
	b.Reserve(arr.Len())

	for i := 0; i < arr.Len(); i++ {
		if arr.IsValid(i) && arr.Value(i) == target {
			b.Append(arr.Value(i))
		} else if !arr.IsValid(i) {
			b.AppendNull()
		}
	}

	return b.NewArray(), nil
}

func filterUint16Equal(arr *array.Uint16, value interface{}, mem memory.Allocator) (arrow.Array, error) {
	target, ok := toUint16(value)
	if !ok {
		return arr, nil
	}

	b := array.NewUint16Builder(mem)
	b.Reserve(arr.Len())

	for i := 0; i < arr.Len(); i++ {
		if arr.IsValid(i) && arr.Value(i) == target {
			b.Append(arr.Value(i))
		} else if !arr.IsValid(i) {
			b.AppendNull()
		}
	}

	return b.NewArray(), nil
}

func filterUint8Equal(arr *array.Uint8, value interface{}, mem memory.Allocator) (arrow.Array, error) {
	target, ok := toUint8(value)
	if !ok {
		return arr, nil
	}

	b := array.NewUint8Builder(mem)
	b.Reserve(arr.Len())

	for i := 0; i < arr.Len(); i++ {
		if arr.IsValid(i) && arr.Value(i) == target {
			b.Append(arr.Value(i))
		} else if !arr.IsValid(i) {
			b.AppendNull()
		}
	}

	return b.NewArray(), nil
}

func filterFloat64Equal(arr *array.Float64, value interface{}, mem memory.Allocator) (arrow.Array, error) {
	target, ok := toFloat64(value)
	if !ok {
		return arr, nil
	}

	b := array.NewFloat64Builder(mem)
	b.Reserve(arr.Len())

	for i := 0; i < arr.Len(); i++ {
		if arr.IsValid(i) && arr.Value(i) == target {
			b.Append(arr.Value(i))
		} else if !arr.IsValid(i) {
			b.AppendNull()
		}
	}

	return b.NewArray(), nil
}

func filterFloat32Equal(arr *array.Float32, value interface{}, mem memory.Allocator) (arrow.Array, error) {
	target, ok := toFloat32(value)
	if !ok {
		return arr, nil
	}

	b := array.NewFloat32Builder(mem)
	b.Reserve(arr.Len())

	for i := 0; i < arr.Len(); i++ {
		if arr.IsValid(i) && arr.Value(i) == target {
			b.Append(arr.Value(i))
		} else if !arr.IsValid(i) {
			b.AppendNull()
		}
	}

	return b.NewArray(), nil
}

func filterStringEqual(arr *array.String, value interface{}, mem memory.Allocator) (arrow.Array, error) {
	target, ok := toStringValue(value)
	if !ok {
		return arr, nil
	}

	b := array.NewStringBuilder(mem)
	b.Reserve(arr.Len())

	for i := 0; i < arr.Len(); i++ {
		if arr.IsValid(i) && arr.Value(i) == target {
			b.Append(arr.Value(i))
		} else if !arr.IsValid(i) {
			b.AppendNull()
		}
	}

	return b.NewArray(), nil
}

func filterBooleanEqual(arr *array.Boolean, value interface{}, mem memory.Allocator) (arrow.Array, error) {
	target, ok := toBool(value)
	if !ok {
		return arr, nil
	}

	b := array.NewBooleanBuilder(mem)
	b.Reserve(arr.Len())

	for i := 0; i < arr.Len(); i++ {
		if arr.IsValid(i) && arr.Value(i) == target {
			b.Append(arr.Value(i))
		} else if !arr.IsValid(i) {
			b.AppendNull()
		}
	}

	return b.NewArray(), nil
}

func filterInt64Less(arr *array.Int64, value interface{}, mem memory.Allocator) (arrow.Array, error) {
	target, ok := toInt64(value)
	if !ok {
		return arr, nil
	}

	b := array.NewInt64Builder(mem)
	b.Reserve(arr.Len())

	for i := 0; i < arr.Len(); i++ {
		if arr.IsValid(i) && arr.Value(i) < target {
			b.Append(arr.Value(i))
		} else if !arr.IsValid(i) {
			b.AppendNull()
		}
	}

	return b.NewArray(), nil
}

func filterInt32Less(arr *array.Int32, value interface{}, mem memory.Allocator) (arrow.Array, error) {
	target, ok := toInt32(value)
	if !ok {
		return arr, nil
	}

	b := array.NewInt32Builder(mem)
	b.Reserve(arr.Len())

	for i := 0; i < arr.Len(); i++ {
		if arr.IsValid(i) && arr.Value(i) < target {
			b.Append(arr.Value(i))
		} else if !arr.IsValid(i) {
			b.AppendNull()
		}
	}

	return b.NewArray(), nil
}

func filterFloat64Less(arr *array.Float64, value interface{}, mem memory.Allocator) (arrow.Array, error) {
	target, ok := toFloat64(value)
	if !ok {
		return arr, nil
	}

	b := array.NewFloat64Builder(mem)
	b.Reserve(arr.Len())

	for i := 0; i < arr.Len(); i++ {
		if arr.IsValid(i) && arr.Value(i) < target {
			b.Append(arr.Value(i))
		} else if !arr.IsValid(i) {
			b.AppendNull()
		}
	}

	return b.NewArray(), nil
}

func filterFloat32Less(arr *array.Float32, value interface{}, mem memory.Allocator) (arrow.Array, error) {
	target, ok := toFloat32(value)
	if !ok {
		return arr, nil
	}

	b := array.NewFloat32Builder(mem)
	b.Reserve(arr.Len())

	for i := 0; i < arr.Len(); i++ {
		if arr.IsValid(i) && arr.Value(i) < target {
			b.Append(arr.Value(i))
		} else if !arr.IsValid(i) {
			b.AppendNull()
		}
	}

	return b.NewArray(), nil
}

func filterStringLess(arr *array.String, value interface{}, mem memory.Allocator) (arrow.Array, error) {
	target, ok := toStringValue(value)
	if !ok {
		return arr, nil
	}

	b := array.NewStringBuilder(mem)
	b.Reserve(arr.Len())

	for i := 0; i < arr.Len(); i++ {
		if arr.IsValid(i) && arr.Value(i) < target {
			b.Append(arr.Value(i))
		} else if !arr.IsValid(i) {
			b.AppendNull()
		}
	}

	return b.NewArray(), nil
}

func filterInt64LessEqual(arr *array.Int64, value interface{}, mem memory.Allocator) (arrow.Array, error) {
	target, ok := toInt64(value)
	if !ok {
		return arr, nil
	}

	b := array.NewInt64Builder(mem)
	b.Reserve(arr.Len())

	for i := 0; i < arr.Len(); i++ {
		if arr.IsValid(i) && arr.Value(i) <= target {
			b.Append(arr.Value(i))
		} else if !arr.IsValid(i) {
			b.AppendNull()
		}
	}

	return b.NewArray(), nil
}

func filterInt32LessEqual(arr *array.Int32, value interface{}, mem memory.Allocator) (arrow.Array, error) {
	target, ok := toInt32(value)
	if !ok {
		return arr, nil
	}

	b := array.NewInt32Builder(mem)
	b.Reserve(arr.Len())

	for i := 0; i < arr.Len(); i++ {
		if arr.IsValid(i) && arr.Value(i) <= target {
			b.Append(arr.Value(i))
		} else if !arr.IsValid(i) {
			b.AppendNull()
		}
	}

	return b.NewArray(), nil
}

func filterFloat64LessEqual(arr *array.Float64, value interface{}, mem memory.Allocator) (arrow.Array, error) {
	target, ok := toFloat64(value)
	if !ok {
		return arr, nil
	}

	b := array.NewFloat64Builder(mem)
	b.Reserve(arr.Len())

	for i := 0; i < arr.Len(); i++ {
		if arr.IsValid(i) && arr.Value(i) <= target {
			b.Append(arr.Value(i))
		} else if !arr.IsValid(i) {
			b.AppendNull()
		}
	}

	return b.NewArray(), nil
}

func filterFloat32LessEqual(arr *array.Float32, value interface{}, mem memory.Allocator) (arrow.Array, error) {
	target, ok := toFloat32(value)
	if !ok {
		return arr, nil
	}

	b := array.NewFloat32Builder(mem)
	b.Reserve(arr.Len())

	for i := 0; i < arr.Len(); i++ {
		if arr.IsValid(i) && arr.Value(i) <= target {
			b.Append(arr.Value(i))
		} else if !arr.IsValid(i) {
			b.AppendNull()
		}
	}

	return b.NewArray(), nil
}

func filterInt64Greater(arr *array.Int64, value interface{}, mem memory.Allocator) (arrow.Array, error) {
	target, ok := toInt64(value)
	if !ok {
		return arr, nil
	}

	b := array.NewInt64Builder(mem)
	b.Reserve(arr.Len())

	for i := 0; i < arr.Len(); i++ {
		if arr.IsValid(i) && arr.Value(i) > target {
			b.Append(arr.Value(i))
		} else if !arr.IsValid(i) {
			b.AppendNull()
		}
	}

	return b.NewArray(), nil
}

func filterInt32Greater(arr *array.Int32, value interface{}, mem memory.Allocator) (arrow.Array, error) {
	target, ok := toInt32(value)
	if !ok {
		return arr, nil
	}

	b := array.NewInt32Builder(mem)
	b.Reserve(arr.Len())

	for i := 0; i < arr.Len(); i++ {
		if arr.IsValid(i) && arr.Value(i) > target {
			b.Append(arr.Value(i))
		} else if !arr.IsValid(i) {
			b.AppendNull()
		}
	}

	return b.NewArray(), nil
}

func filterFloat64Greater(arr *array.Float64, value interface{}, mem memory.Allocator) (arrow.Array, error) {
	target, ok := toFloat64(value)
	if !ok {
		return arr, nil
	}

	b := array.NewFloat64Builder(mem)
	b.Reserve(arr.Len())

	for i := 0; i < arr.Len(); i++ {
		if arr.IsValid(i) && arr.Value(i) > target {
			b.Append(arr.Value(i))
		} else if !arr.IsValid(i) {
			b.AppendNull()
		}
	}

	return b.NewArray(), nil
}

func filterFloat32Greater(arr *array.Float32, value interface{}, mem memory.Allocator) (arrow.Array, error) {
	target, ok := toFloat32(value)
	if !ok {
		return arr, nil
	}

	b := array.NewFloat32Builder(mem)
	b.Reserve(arr.Len())

	for i := 0; i < arr.Len(); i++ {
		if arr.IsValid(i) && arr.Value(i) > target {
			b.Append(arr.Value(i))
		} else if !arr.IsValid(i) {
			b.AppendNull()
		}
	}

	return b.NewArray(), nil
}

func filterInt64GreaterEqual(arr *array.Int64, value interface{}, mem memory.Allocator) (arrow.Array, error) {
	target, ok := toInt64(value)
	if !ok {
		return arr, nil
	}

	b := array.NewInt64Builder(mem)
	b.Reserve(arr.Len())

	for i := 0; i < arr.Len(); i++ {
		if arr.IsValid(i) && arr.Value(i) >= target {
			b.Append(arr.Value(i))
		} else if !arr.IsValid(i) {
			b.AppendNull()
		}
	}

	return b.NewArray(), nil
}

func filterInt32GreaterEqual(arr *array.Int32, value interface{}, mem memory.Allocator) (arrow.Array, error) {
	target, ok := toInt32(value)
	if !ok {
		return arr, nil
	}

	b := array.NewInt32Builder(mem)
	b.Reserve(arr.Len())

	for i := 0; i < arr.Len(); i++ {
		if arr.IsValid(i) && arr.Value(i) >= target {
			b.Append(arr.Value(i))
		} else if !arr.IsValid(i) {
			b.AppendNull()
		}
	}

	return b.NewArray(), nil
}

func filterFloat64GreaterEqual(arr *array.Float64, value interface{}, mem memory.Allocator) (arrow.Array, error) {
	target, ok := toFloat64(value)
	if !ok {
		return arr, nil
	}

	b := array.NewFloat64Builder(mem)
	b.Reserve(arr.Len())

	for i := 0; i < arr.Len(); i++ {
		if arr.IsValid(i) && arr.Value(i) >= target {
			b.Append(arr.Value(i))
		} else if !arr.IsValid(i) {
			b.AppendNull()
		}
	}

	return b.NewArray(), nil
}

func filterFloat32GreaterEqual(arr *array.Float32, value interface{}, mem memory.Allocator) (arrow.Array, error) {
	target, ok := toFloat32(value)
	if !ok {
		return arr, nil
	}

	b := array.NewFloat32Builder(mem)
	b.Reserve(arr.Len())

	for i := 0; i < arr.Len(); i++ {
		if arr.IsValid(i) && arr.Value(i) >= target {
			b.Append(arr.Value(i))
		} else if !arr.IsValid(i) {
			b.AppendNull()
		}
	}

	return b.NewArray(), nil
}

func filterGeneric(arr arrow.Array, value interface{}, mem memory.Allocator) (arrow.Array, error) {
	b := array.NewBuilder(mem, arr.DataType())
	b.Reserve(arr.Len())

	for i := 0; i < arr.Len(); i++ {
		if arr.IsValid(i) {
			b.AppendNull()
		} else {
			b.AppendNull()
		}
	}

	return b.NewArray(), nil
}

func filterGenericLess(arr arrow.Array, value interface{}, mem memory.Allocator) (arrow.Array, error) {
	return filterGeneric(arr, value, mem)
}

func filterGenericGreater(arr arrow.Array, value interface{}, mem memory.Allocator) (arrow.Array, error) {
	return filterGeneric(arr, value, mem)
}

func invertFilter(filter arrow.Array, totalLen int, mem memory.Allocator) (arrow.Array, error) {
	switch f := filter.(type) {
	case *array.Int64:
		b := array.NewInt64Builder(mem)
		b.Reserve(totalLen)
		for i := 0; i < totalLen; i++ {
			if !f.IsValid(i) || f.Value(i) != 0 {
				b.AppendNull()
			} else {
				b.Append(1)
			}
		}
		return b.NewArray(), nil
	default:
		return filter, nil
	}
}

func toInt64(v interface{}) (int64, bool) {
	switch n := v.(type) {
	case int64:
		return n, true
	case int:
		return int64(n), true
	case int32:
		return int64(n), true
	case int16:
		return int64(n), true
	case int8:
		return int64(n), true
	default:
		return 0, false
	}
}

func toInt32(v interface{}) (int32, bool) {
	switch n := v.(type) {
	case int64:
		return int32(n), true
	case int:
		return int32(n), true
	case int32:
		return n, true
	default:
		return 0, false
	}
}

func toInt16(v interface{}) (int16, bool) {
	switch n := v.(type) {
	case int64:
		return int16(n), true
	case int:
		return int16(n), true
	case int16:
		return n, true
	default:
		return 0, false
	}
}

func toInt8(v interface{}) (int8, bool) {
	switch n := v.(type) {
	case int64:
		return int8(n), true
	case int:
		return int8(n), true
	case int8:
		return n, true
	default:
		return 0, false
	}
}

func toUint64(v interface{}) (uint64, bool) {
	switch n := v.(type) {
	case uint64:
		return n, true
	case uint:
		return uint64(n), true
	case uint32:
		return uint64(n), true
	case uint16:
		return uint64(n), true
	case uint8:
		return uint64(n), true
	default:
		return 0, false
	}
}

func toUint32(v interface{}) (uint32, bool) {
	switch n := v.(type) {
	case uint64:
		return uint32(n), true
	case uint:
		return uint32(n), true
	case uint32:
		return n, true
	default:
		return 0, false
	}
}

func toUint16(v interface{}) (uint16, bool) {
	switch n := v.(type) {
	case uint64:
		return uint16(n), true
	case uint:
		return uint16(n), true
	case uint16:
		return n, true
	default:
		return 0, false
	}
}

func toUint8(v interface{}) (uint8, bool) {
	switch n := v.(type) {
	case uint64:
		return uint8(n), true
	case uint:
		return uint8(n), true
	case uint8:
		return n, true
	default:
		return 0, false
	}
}

func toFloat64(v interface{}) (float64, bool) {
	switch n := v.(type) {
	case float64:
		return n, true
	case float32:
		return float64(n), true
	case int64:
		return float64(n), true
	case int:
		return float64(n), true
	default:
		return 0, false
	}
}

func toFloat32(v interface{}) (float32, bool) {
	switch n := v.(type) {
	case float64:
		return float32(n), true
	case float32:
		return n, true
	default:
		return 0, false
	}
}

func toStringValue(v interface{}) (string, bool) {
	switch s := v.(type) {
	case string:
		return s, true
	default:
		return "", false
	}
}

func toBool(v interface{}) (bool, bool) {
	switch b := v.(type) {
	case bool:
		return b, true
	default:
		return false, false
	}
}

func containsValue(min, max, val interface{}) bool {
	minCmp := compareValues(min, val)
	maxCmp := compareValues(max, val)
	return (minCmp <= 0 && maxCmp >= 0)
}

func canContainLessThan(max, val interface{}) bool {
	return compareValues(max, val) > 0
}

func canContainLessThanOrEqual(max, val interface{}) bool {
	return compareValues(max, val) >= 0
}

func canContainGreaterThan(min, val interface{}) bool {
	return compareValues(min, val) < 0
}

func canContainGreaterThanOrEqual(min, val interface{}) bool {
	return compareValues(min, val) <= 0
}

func compareValues(a, b interface{}) int {
	av, ok := toInt64(a)
	if !ok {
		avf, ok := toFloat64(a)
		if !ok {
			return 0
		}
		bvf, ok := toFloat64(b)
		if !ok {
			return 0
		}
		if avf < bvf {
			return -1
		}
		if avf > bvf {
			return 1
		}
		return 0
	}

	bv, ok := toInt64(b)
	if !ok {
		return 0
	}

	if av < bv {
		return -1
	}
	if av > bv {
		return 1
	}
	return 0
}

type ChunkStats struct {
	Min       interface{}
	Max       interface{}
	NullCount int64
	RowCount  int64
}

func ComputeChunkStats(arr arrow.Array) *ChunkStats {
	if arr == nil {
		return nil
	}

	stats := &ChunkStats{
		RowCount:  int64(arr.Len()),
		NullCount: int64(arr.Len() - arr.NullN()),
	}

	switch arr := arr.(type) {
	case *array.Int64:
		if arr.Len() > 0 {
			stats.Min = arr.Value(0)
			stats.Max = arr.Value(arr.Len() - 1)
		}
	case *array.Int32:
		if arr.Len() > 0 {
			stats.Min = arr.Value(0)
			stats.Max = arr.Value(arr.Len() - 1)
		}
	case *array.Float64:
		if arr.Len() > 0 {
			stats.Min = arr.Value(0)
			stats.Max = arr.Value(arr.Len() - 1)
		}
	case *array.Float32:
		if arr.Len() > 0 {
			stats.Min = arr.Value(0)
			stats.Max = arr.Value(arr.Len() - 1)
		}
	case *array.String:
		if arr.Len() > 0 {
			stats.Min = arr.Value(0)
			stats.Max = arr.Value(arr.Len() - 1)
		}
	}

	return stats
}
