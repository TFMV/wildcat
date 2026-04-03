package execution

import (
	"math"

	"github.com/TFMV/wildcat/format"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

type AggregationType string

const (
	AggCount AggregationType = "count"
	AggSum   AggregationType = "sum"
	AggAvg   AggregationType = "avg"
	AggMin   AggregationType = "min"
	AggMax   AggregationType = "max"
)

type AggregationSpec struct {
	Column string
	Type   AggregationType
}

type AggregationResult struct {
	Values map[string]interface{}
}

func (r *AggregationResult) GetString(col string) string {
	if v, ok := r.Values[col].(string); ok {
		return v
	}
	return ""
}

func (r *AggregationResult) GetInt(col string) int64 {
	if v, ok := r.Values[col].(int64); ok {
		return v
	}
	return 0
}

func (r *AggregationResult) GetFloat(col string) float64 {
	if v, ok := r.Values[col].(float64); ok {
		return v
	}
	return 0
}

func ComputeAggregation(chunks []format.Chunk, spec AggregationSpec, mem memory.Allocator) (*AggregationResult, error) {
	if len(chunks) == 0 {
		return &AggregationResult{Values: make(map[string]interface{})}, nil
	}

	if mem == nil {
		mem = memory.DefaultAllocator
	}

	result := &AggregationResult{
		Values: make(map[string]interface{}),
	}

	switch spec.Type {
	case AggCount:
		result.Values[spec.Column] = countRows(chunks)
	case AggSum:
		result.Values[spec.Column] = sumColumn(chunks, mem)
	case AggAvg:
		result.Values[spec.Column] = avgColumn(chunks, mem)
	case AggMin:
		result.Values[spec.Column] = minColumn(chunks, mem)
	case AggMax:
		result.Values[spec.Column] = maxColumn(chunks, mem)
	}

	return result, nil
}

func countRows(chunks []format.Chunk) int64 {
	var total int64
	for _, chunk := range chunks {
		total += chunk.NumRows
	}
	return total
}

func sumColumn(chunks []format.Chunk, mem memory.Allocator) interface{} {
	if len(chunks) == 0 {
		return int64(0)
	}

	first := chunks[0].Array

	switch first.(type) {
	case *array.Int64:
		var sum int64
		for _, chunk := range chunks {
			a := chunk.Array.(*array.Int64)
			for i := 0; i < a.Len(); i++ {
				if a.IsValid(i) {
					sum += a.Value(i)
				}
			}
		}
		return sum

	case *array.Int32:
		var sum int64
		for _, chunk := range chunks {
			a := chunk.Array.(*array.Int32)
			for i := 0; i < a.Len(); i++ {
				if a.IsValid(i) {
					sum += int64(a.Value(i))
				}
			}
		}
		return sum

	case *array.Int16:
		var sum int64
		for _, chunk := range chunks {
			a := chunk.Array.(*array.Int16)
			for i := 0; i < a.Len(); i++ {
				if a.IsValid(i) {
					sum += int64(a.Value(i))
				}
			}
		}
		return sum

	case *array.Int8:
		var sum int64
		for _, chunk := range chunks {
			a := chunk.Array.(*array.Int8)
			for i := 0; i < a.Len(); i++ {
				if a.IsValid(i) {
					sum += int64(a.Value(i))
				}
			}
		}
		return sum

	case *array.Uint64:
		var sum uint64
		for _, chunk := range chunks {
			a := chunk.Array.(*array.Uint64)
			for i := 0; i < a.Len(); i++ {
				if a.IsValid(i) {
					sum += a.Value(i)
				}
			}
		}
		return int64(sum)

	case *array.Uint32:
		var sum uint64
		for _, chunk := range chunks {
			a := chunk.Array.(*array.Uint32)
			for i := 0; i < a.Len(); i++ {
				if a.IsValid(i) {
					sum += uint64(a.Value(i))
				}
			}
		}
		return int64(sum)

	case *array.Float64:
		var sum float64
		for _, chunk := range chunks {
			a := chunk.Array.(*array.Float64)
			for i := 0; i < a.Len(); i++ {
				if a.IsValid(i) {
					sum += a.Value(i)
				}
			}
		}
		return sum

	case *array.Float32:
		var sum float64
		for _, chunk := range chunks {
			a := chunk.Array.(*array.Float32)
			for i := 0; i < a.Len(); i++ {
				if a.IsValid(i) {
					sum += float64(a.Value(i))
				}
			}
		}
		return sum
	}

	return int64(0)
}

func avgColumn(chunks []format.Chunk, mem memory.Allocator) interface{} {
	if len(chunks) == 0 {
		return float64(0)
	}

	first := chunks[0].Array

	switch first.(type) {
	case *array.Int64:
		var sum int64
		var count int64
		for _, chunk := range chunks {
			a := chunk.Array.(*array.Int64)
			for i := 0; i < a.Len(); i++ {
				if a.IsValid(i) {
					sum += a.Value(i)
					count++
				}
			}
		}
		if count == 0 {
			return float64(0)
		}
		return float64(sum) / float64(count)

	case *array.Int32:
		var sum int64
		var count int64
		for _, chunk := range chunks {
			a := chunk.Array.(*array.Int32)
			for i := 0; i < a.Len(); i++ {
				if a.IsValid(i) {
					sum += int64(a.Value(i))
					count++
				}
			}
		}
		if count == 0 {
			return float64(0)
		}
		return float64(sum) / float64(count)

	case *array.Float64:
		var sum float64
		var count int64
		for _, chunk := range chunks {
			a := chunk.Array.(*array.Float64)
			for i := 0; i < a.Len(); i++ {
				if a.IsValid(i) {
					sum += a.Value(i)
					count++
				}
			}
		}
		if count == 0 {
			return float64(0)
		}
		return sum / float64(count)

	case *array.Float32:
		var sum float64
		var count int64
		for _, chunk := range chunks {
			a := chunk.Array.(*array.Float32)
			for i := 0; i < a.Len(); i++ {
				if a.IsValid(i) {
					sum += float64(a.Value(i))
					count++
				}
			}
		}
		if count == 0 {
			return float64(0)
		}
		return sum / float64(count)
	}

	return float64(0)
}

func minColumn(chunks []format.Chunk, mem memory.Allocator) interface{} {
	if len(chunks) == 0 {
		return nil
	}

	first := chunks[0].Array

	switch first.(type) {
	case *array.Int64:
		min := int64(math.MaxInt64)
		hasValue := false
		for _, chunk := range chunks {
			a := chunk.Array.(*array.Int64)
			for i := 0; i < a.Len(); i++ {
				if a.IsValid(i) {
					if !hasValue || a.Value(i) < min {
						min = a.Value(i)
						hasValue = true
					}
				}
			}
		}
		if !hasValue {
			return nil
		}
		return min

	case *array.Int32:
		min := int32(math.MaxInt32)
		hasValue := false
		for _, chunk := range chunks {
			a := chunk.Array.(*array.Int32)
			for i := 0; i < a.Len(); i++ {
				if a.IsValid(i) {
					if !hasValue || a.Value(i) < min {
						min = a.Value(i)
						hasValue = true
					}
				}
			}
		}
		if !hasValue {
			return nil
		}
		return int64(min)

	case *array.Float64:
		min := math.MaxFloat64
		hasValue := false
		for _, chunk := range chunks {
			a := chunk.Array.(*array.Float64)
			for i := 0; i < a.Len(); i++ {
				if a.IsValid(i) {
					if !hasValue || a.Value(i) < min {
						min = a.Value(i)
						hasValue = true
					}
				}
			}
		}
		if !hasValue {
			return nil
		}
		return min

	case *array.String:
		min := ""
		hasValue := false
		for _, chunk := range chunks {
			a := chunk.Array.(*array.String)
			for i := 0; i < a.Len(); i++ {
				if a.IsValid(i) {
					if !hasValue || a.Value(i) < min {
						min = a.Value(i)
						hasValue = true
					}
				}
			}
		}
		if !hasValue {
			return nil
		}
		return min
	}

	return nil
}

func maxColumn(chunks []format.Chunk, mem memory.Allocator) interface{} {
	if len(chunks) == 0 {
		return nil
	}

	first := chunks[0].Array

	switch first.(type) {
	case *array.Int64:
		max := int64(math.MinInt64)
		hasValue := false
		for _, chunk := range chunks {
			a := chunk.Array.(*array.Int64)
			for i := 0; i < a.Len(); i++ {
				if a.IsValid(i) {
					if !hasValue || a.Value(i) > max {
						max = a.Value(i)
						hasValue = true
					}
				}
			}
		}
		if !hasValue {
			return nil
		}
		return max

	case *array.Int32:
		max := int32(math.MinInt32)
		hasValue := false
		for _, chunk := range chunks {
			a := chunk.Array.(*array.Int32)
			for i := 0; i < a.Len(); i++ {
				if a.IsValid(i) {
					if !hasValue || a.Value(i) > max {
						max = a.Value(i)
						hasValue = true
					}
				}
			}
		}
		if !hasValue {
			return nil
		}
		return int64(max)

	case *array.Float64:
		max := -math.MaxFloat64
		hasValue := false
		for _, chunk := range chunks {
			a := chunk.Array.(*array.Float64)
			for i := 0; i < a.Len(); i++ {
				if a.IsValid(i) {
					if !hasValue || a.Value(i) > max {
						max = a.Value(i)
						hasValue = true
					}
				}
			}
		}
		if !hasValue {
			return nil
		}
		return max

	case *array.String:
		max := ""
		hasValue := false
		for _, chunk := range chunks {
			a := chunk.Array.(*array.String)
			for i := 0; i < a.Len(); i++ {
				if a.IsValid(i) {
					if !hasValue || a.Value(i) > max {
						max = a.Value(i)
						hasValue = true
					}
				}
			}
		}
		if !hasValue {
			return nil
		}
		return max
	}

	return nil
}

type StreamingAggregator struct {
	spec        AggregationSpec
	mem         memory.Allocator
	sum         interface{}
	count       int64
	minVal      interface{}
	maxVal      interface{}
	initialized bool
}

func NewStreamingAggregator(spec AggregationSpec, mem memory.Allocator) *StreamingAggregator {
	if mem == nil {
		mem = memory.DefaultAllocator
	}
	return &StreamingAggregator{
		spec: spec,
		mem:  mem,
	}
}

func (a *StreamingAggregator) Update(chunk format.Chunk) error {
	arr := chunk.Array
	if arr == nil || arr.Len() == 0 {
		return nil
	}

	if !a.initialized {
		a.initializeFromArray(arr)
		a.initialized = true
	}

	switch a.spec.Type {
	case AggCount:
		a.count += chunk.NumRows

	case AggSum, AggAvg:
		a.accumulateSum(arr)

	case AggMin:
		a.accumulateMin(arr)

	case AggMax:
		a.accumulateMax(arr)
	}

	return nil
}

func (a *StreamingAggregator) Result() interface{} {
	switch a.spec.Type {
	case AggCount:
		return a.count

	case AggSum:
		return a.sum

	case AggAvg:
		if a.count == 0 {
			return float64(0)
		}
		switch s := a.sum.(type) {
		case int64:
			return float64(s) / float64(a.count)
		case float64:
			return s / float64(a.count)
		default:
			return float64(0)
		}

	case AggMin:
		return a.minVal

	case AggMax:
		return a.maxVal

	default:
		return nil
	}
}

func (a *StreamingAggregator) Reset() {
	a.sum = nil
	a.count = 0
	a.minVal = nil
	a.maxVal = nil
	a.initialized = false
}

func (a *StreamingAggregator) initializeFromArray(arr arrow.Array) {
	switch a.spec.Type {
	case AggSum, AggAvg:
		switch arr.(type) {
		case *array.Int64:
			a.sum = int64(0)
		case *array.Int32:
			a.sum = int64(0)
		case *array.Int16:
			a.sum = int64(0)
		case *array.Int8:
			a.sum = int64(0)
		case *array.Uint64:
			a.sum = uint64(0)
		case *array.Uint32:
			a.sum = uint64(0)
		case *array.Float64:
			a.sum = float64(0)
		case *array.Float32:
			a.sum = float64(0)
		}

	case AggMin:
		a.minVal = nil

	case AggMax:
		a.maxVal = nil
	}
}

func (a *StreamingAggregator) accumulateSum(arr arrow.Array) {
	switch s := a.sum.(type) {
	case int64:
		switch v := arr.(type) {
		case *array.Int64:
			for i := 0; i < v.Len(); i++ {
				if v.IsValid(i) {
					s += v.Value(i)
				}
			}
			a.sum = s
		case *array.Int32:
			for i := 0; i < v.Len(); i++ {
				if v.IsValid(i) {
					s += int64(v.Value(i))
				}
			}
			a.sum = s
		}

	case uint64:
		switch v := arr.(type) {
		case *array.Uint64:
			for i := 0; i < v.Len(); i++ {
				if v.IsValid(i) {
					s += v.Value(i)
				}
			}
			a.sum = s
		}

	case float64:
		switch v := arr.(type) {
		case *array.Float64:
			for i := 0; i < v.Len(); i++ {
				if v.IsValid(i) {
					s += v.Value(i)
				}
			}
			a.sum = s
		case *array.Float32:
			for i := 0; i < v.Len(); i++ {
				if v.IsValid(i) {
					s += float64(v.Value(i))
				}
			}
			a.sum = s
		}
	}
}

func (a *StreamingAggregator) accumulateMin(arr arrow.Array) {
	switch v := arr.(type) {
	case *array.Int64:
		for i := 0; i < v.Len(); i++ {
			if v.IsValid(i) {
				if a.minVal == nil || v.Value(i) < a.minVal.(int64) {
					a.minVal = v.Value(i)
				}
			}
		}
	case *array.Int32:
		for i := 0; i < v.Len(); i++ {
			if v.IsValid(i) {
				if a.minVal == nil || int64(v.Value(i)) < a.minVal.(int64) {
					a.minVal = int64(v.Value(i))
				}
			}
		}
	case *array.Float64:
		for i := 0; i < v.Len(); i++ {
			if v.IsValid(i) {
				if a.minVal == nil || v.Value(i) < a.minVal.(float64) {
					a.minVal = v.Value(i)
				}
			}
		}
	}
}

func (a *StreamingAggregator) accumulateMax(arr arrow.Array) {
	switch v := arr.(type) {
	case *array.Int64:
		for i := 0; i < v.Len(); i++ {
			if v.IsValid(i) {
				if a.maxVal == nil || v.Value(i) > a.maxVal.(int64) {
					a.maxVal = v.Value(i)
				}
			}
		}
	case *array.Int32:
		for i := 0; i < v.Len(); i++ {
			if v.IsValid(i) {
				if a.maxVal == nil || int64(v.Value(i)) > a.maxVal.(int64) {
					a.maxVal = int64(v.Value(i))
				}
			}
		}
	case *array.Float64:
		for i := 0; i < v.Len(); i++ {
			if v.IsValid(i) {
				if a.maxVal == nil || v.Value(i) > a.maxVal.(float64) {
					a.maxVal = v.Value(i)
				}
			}
		}
	}
}

func GroupedAggregate(chunks []format.Chunk, spec AggregationSpec, groupKey string, mem memory.Allocator) (map[string]*AggregationResult, error) {
	if len(chunks) == 0 {
		return make(map[string]*AggregationResult), nil
	}

	results := make(map[string]*AggregationResult)

	for _, chunk := range chunks {
		arr := chunk.Array
		groupArr := findGroupColumn(chunks, groupKey)
		if groupArr == nil {
			continue
		}

		groupVals := extractGroupValues(groupArr)
		for i := 0; i < arr.Len(); i++ {
			key := groupVals[i]
			if _, ok := results[key]; !ok {
				results[key] = &AggregationResult{Values: make(map[string]interface{})}
			}

			switch spec.Type {
			case AggCount:
				results[key].Values[spec.Column] = results[key].GetInt(spec.Column) + 1

			case AggSum:
				val := extractNumericValue(arr, i)
				results[key].Values[spec.Column] = results[key].GetInt(spec.Column) + val
			}
		}
	}

	return results, nil
}

func findGroupColumn(chunks []format.Chunk, name string) arrow.Array {
	for _, chunk := range chunks {
		if chunk.ColumnName == name {
			return chunk.Array
		}
	}
	return nil
}

func extractGroupValues(arr arrow.Array) []string {
	if strArr, ok := arr.(*array.String); ok {
		vals := make([]string, strArr.Len())
		for i := 0; i < strArr.Len(); i++ {
			if strArr.IsValid(i) {
				vals[i] = strArr.Value(i)
			}
		}
		return vals
	}
	return nil
}

func extractNumericValue(arr arrow.Array, idx int) int64 {
	switch v := arr.(type) {
	case *array.Int64:
		if v.IsValid(idx) {
			return v.Value(idx)
		}
	case *array.Int32:
		if v.IsValid(idx) {
			return int64(v.Value(idx))
		}
	}
	return 0
}
