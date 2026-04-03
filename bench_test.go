package wildcat

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/TFMV/wildcat/format"
	"github.com/TFMV/wildcat/storage"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

var testEngine *Engine
var testMem memory.Allocator

func TestMain(m *testing.M) {
	testMem = memory.NewGoAllocator()

	engine, err := NewEngine(&EngineConfig{
		StoragePath: "/tmp/wildcat_bench.db",
		Memory:      testMem,
	})
	if err != nil {
		fmt.Printf("Failed to create test engine: %v\n", err)
		os.Exit(1)
	}
	testEngine = engine

	code := m.Run()

	testEngine.Close()
	os.Exit(code)
}

func BenchmarkIngest(b *testing.B) {
	ctx := context.Background()
	dataset := "bench_ingest"

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		batch := generateTestBatch(10000)
		_, err := testEngine.Ingest(ctx, dataset, batch)
		if err != nil {
			b.Fatalf("Ingest failed: %v", err)
		}
	}
}

func BenchmarkScanNoPredicate(b *testing.B) {
	ctx := context.Background()
	dataset := "bench_scan"

	setupTestData(ctx, dataset, 100000)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		result, err := testEngine.Query(ctx, "SELECT * FROM "+dataset)
		if err != nil {
			b.Fatalf("Query failed: %v", err)
		}
		if result != nil {
			result.Release()
		}
	}
}

func BenchmarkScanWithPredicate(b *testing.B) {
	ctx := context.Background()
	dataset := "bench_predicate"

	setupTestData(ctx, dataset, 100000)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		result, err := testEngine.Query(ctx, "SELECT * FROM "+dataset+" WHERE value > 5000")
		if err != nil {
			b.Fatalf("Query failed: %v", err)
		}
		if result != nil {
			result.Release()
		}
	}
}

func BenchmarkAggregationCount(b *testing.B) {
	ctx := context.Background()
	dataset := "bench_agg"

	setupTestData(ctx, dataset, 100000)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		result, err := testEngine.Query(ctx, "SELECT COUNT(*) FROM "+dataset)
		if err != nil {
			b.Fatalf("Query failed: %v", err)
		}
		if result != nil {
			result.Release()
		}
	}
}

func BenchmarkAggregationSum(b *testing.B) {
	ctx := context.Background()
	dataset := "bench_sum"

	setupTestData(ctx, dataset, 100000)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		result, err := testEngine.Query(ctx, "SELECT SUM(value) FROM "+dataset)
		if err != nil {
			b.Fatalf("Query failed: %v", err)
		}
		if result != nil {
			result.Release()
		}
	}
}

func BenchmarkAggregationGroupBy(b *testing.B) {
	ctx := context.Background()
	dataset := "bench_group"

	setupTestDataWithGroup(ctx, dataset, 100000)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		result, err := testEngine.Query(ctx, "SELECT category, SUM(value) FROM "+dataset+" GROUP BY category")
		if err != nil {
			b.Fatalf("Query failed: %v", err)
		}
		if result != nil {
			result.Release()
		}
	}
}

func BenchmarkPredicatePushdown(b *testing.B) {
	ctx := context.Background()
	dataset := "bench_pushdown"

	setupTestData(ctx, dataset, 100000)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		result, err := testEngine.Query(ctx, "SELECT * FROM "+dataset+" WHERE value > 90000")
		if err != nil {
			b.Fatalf("Query failed: %v", err)
		}
		if result != nil {
			result.Release()
		}
	}
}

func BenchmarkParallelRead(b *testing.B) {
	ctx := context.Background()
	dataset := "bench_parallel"

	setupTestData(ctx, dataset, 100000)

	reader := storage.NewParallelReader(testEngine.storage, 4)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		result, err := reader.ReadColumn(ctx, dataset, "default", "value")
		if err != nil {
			b.Fatalf("Read failed: %v", err)
		}
		for _, c := range result.Chunks {
			if c.Array != nil {
				c.Array.Release()
			}
		}
	}
}

func BenchmarkChunkSize(b *testing.B) {
	ctx := context.Background()

	sizes := []int{1024, 4096, 16384, 65536}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("chunk_%d", size), func(b *testing.B) {
			dataset := fmt.Sprintf("bench_chunk_%d", size)

			ingestor := storage.NewIngestor(testEngine.storage, size)

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				batch := generateTestBatch(50000)
				_, err := ingestor.Insert(ctx, dataset, "default", batch)
				if err != nil {
					b.Fatalf("Ingest failed: %v", err)
				}
			}
		})
	}
}

func BenchmarkExplain(b *testing.B) {
	ctx := context.Background()
	dataset := "bench_explain"

	setupTestData(ctx, dataset, 10000)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := testEngine.Explain("SELECT * FROM " + dataset + " WHERE value > 5000")
		if err != nil {
			b.Fatalf("Explain failed: %v", err)
		}
	}
}

func setupTestData(ctx context.Context, dataset string, numRows int) {
	batch := generateTestBatch(numRows)
	testEngine.Ingest(ctx, dataset, batch)
}

func setupTestDataWithGroup(ctx context.Context, dataset string, numRows int) {
	catBuilder := array.NewStringBuilder(testMem)
	valBuilder := array.NewInt64Builder(testMem)

	for i := 0; i < numRows; i++ {
		catBuilder.Append(fmt.Sprintf("cat_%d", i%10))
		valBuilder.Append(int64(i % 1000))
	}

	catArr := catBuilder.NewArray()
	valArr := valBuilder.NewArray()
	defer func() {
		catArr.Release()
		valArr.Release()
	}()

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "category", Type: &arrow.StringType{}},
		{Name: "value", Type: &arrow.Int64Type{}},
	}, nil)

	batch := format.NewRecordBatch(schema, []arrow.Array{catArr, valArr}, int64(numRows))
	defer batch.Release()

	testEngine.Ingest(ctx, dataset, batch)
}

func generateTestBatch(numRows int) arrow.RecordBatch {
	idBuilder := array.NewInt64Builder(testMem)
	valBuilder := array.NewInt64Builder(testMem)

	for i := 0; i < numRows; i++ {
		idBuilder.Append(int64(i))
		valBuilder.Append(int64(i % 100000))
	}

	idArr := idBuilder.NewArray()
	valArr := valBuilder.NewArray()
	defer func() {
		idArr.Release()
		valArr.Release()
	}()

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "value", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	return format.NewRecordBatch(schema, []arrow.Array{idArr, valArr}, int64(numRows))
}

func generateTestData(numRows int, chunkSize int) []arrow.RecordBatch {
	batches := make([]arrow.RecordBatch, 0)

	for i := 0; i < numRows; i += chunkSize {
		remaining := numRows - i
		if remaining > chunkSize {
			remaining = chunkSize
		}
		batches = append(batches, generateTestBatch(remaining))
	}

	return batches
}

type BenchmarkResult struct {
	Name       string
	Operations int
	TotalTime  time.Duration
	AvgTime    time.Duration
	Throughput float64
	AllocBytes int64
	Allocs     int64
}

func (r *BenchmarkResult) String() string {
	return fmt.Sprintf("%s: %d ops in %v (%.2f ops/sec, %d bytes, %d allocs)",
		r.Name, r.Operations, r.TotalTime, r.Throughput, r.AllocBytes, r.Allocs)
}

func runBenchmark(name string, ops int, fn func()) BenchmarkResult {
	start := time.Now()
	for i := 0; i < ops; i++ {
		fn()
	}
	elapsed := time.Since(start)

	return BenchmarkResult{
		Name:       name,
		Operations: ops,
		TotalTime:  elapsed,
		AvgTime:    elapsed / time.Duration(ops),
		Throughput: float64(ops) / elapsed.Seconds(),
		AllocBytes: 0,
		Allocs:     0,
	}
}

func printBenchmarkResults(results []BenchmarkResult) {
	fmt.Println("\n=== Benchmark Results ===")
	for _, r := range results {
		fmt.Println(r.String())
	}
}
