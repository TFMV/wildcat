# Wildcat - Embedded Analytics Engine

A high-performance embedded analytics engine built in Go, using bbolt for storage and Apache Arrow for in-memory data processing.

## Features

- **Column-Oriented Storage**: Data stored column-by-column in bbolt
- **Arrow IPC Serialization**: Efficient binary format for Arrow arrays
- **Vectorized Execution**: Processing entire columns at once
- **SQL-like Query Support**: SELECT, WHERE, GROUP BY, aggregation functions
- **Predicate Pushdown**: Filters applied at storage layer using chunk statistics
- **Chunk-Based Organization**: Configurable chunk sizes for optimal performance
- **Parallel Reading**: Multi-worker parallel chunk loading

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                         Wildcat Engine                      │
├─────────────────────────────────────────────────────────────┤
│  Query Layer                                                │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐  │
│  │   Parser    │→ │   Planner   │→ │    Optimizer        │  │
│  │   (SQL AST) │  │  (Physical) │  │  (Stats-based)      │  │
│  └─────────────┘  └─────────────┘  └─────────────────────┘  │
├─────────────────────────────────────────────────────────────┤
│  Execution Layer                                            │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐  │
│  │    Scan     │  │   Filter    │  │    Aggregate        │  │
│  └─────────────┘  └─────────────┘  └─────────────────────┘  │
├─────────────────────────────────────────────────────────────┤
│  Format Layer                                               │
│  ┌─────────────────────────────────────────────────────────┐│
│  │            Apache Arrow IPC Encoding/Decoding           ││
│  └─────────────────────────────────────────────────────────┘│
├─────────────────────────────────────────────────────────────┤
│  Storage Layer                                              │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐  │
│  │   bbolt DB  │  │   Chunks    │  │   Statistics        │  │
│  └─────────────┘  └─────────────┘  └─────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
```

## Quick Start

```go
package main

import (
    "context"
    "fmt"
    "github.com/TFMV/wildcat"
)

func main() {
    engine, _ := wildcat.NewEngine(&wildcat.EngineConfig{
        StoragePath: "data.db",
    })
    defer engine.Close()

    ctx := context.Background()

    // Ingest data (using Arrow)
    // ... create batch ...
    engine.Ingest(ctx, "sales", batch)

    // Query
    result, _ := engine.Query(ctx, "SELECT SUM(revenue), category FROM sales WHERE date > '2025-01-01' GROUP BY category")
    fmt.Printf("Rows: %d\n", result.NumRows())
}
```

## Project Structure

```
wildcat/
├── engine.go           # Main engine entry point
├── query/             # Query processing
│   ├── parser.go      # SQL parsing → AST
│   ├── planner.go    # AST → physical plan
│   ├── optimizer.go  # Stats-based optimization
│   └── executor.go   # Plan execution
├── execution/         # Query execution engine
│   ├── engine.go     # Execution engine
│   ├── scan.go       # Scan operator
│   ├── predicate.go  # Predicate evaluation
│   └── aggregate.go  # Aggregation functions
├── format/            # Arrow serialization
│   └── arrow.go      # IPC encode/decode, chunking
├── storage/           # bbolt storage layer
│   ├── engine.go     # Storage engine
│   ├── bbolt.go      # Low-level bbolt operations
│   ├── ingest.go     # Data ingestion
│   ├── stats.go      # Chunk statistics
│   └── parallel.go   # Parallel I/O
└── bench_test.go     # Performance benchmarks
```

## Supported SQL Syntax

```sql
SELECT <columns> [, <function>(<column>)]
FROM <dataset>
WHERE <predicate>
GROUP BY <column>
```

### Supported Operations

- **Predicates**: `=`, `!=`, `<`, `<=`, `>`, `>=`
- **Aggregations**: `COUNT`, `SUM`, `AVG`, `MIN`, `MAX`
- **Grouping**: Single column GROUP BY

## Storage Format

### Key Structure
```
<dataset>\0<partition>\0<column>\0[<big-endian chunkID>]
```

### Buckets
- **data**: Column chunk data
- **meta**: Metadata and statistics

## Benchmarks

Run benchmarks with:
```bash
go test -bench=. -benchmem ./...
```

### Benchmark Results (Apple M4)

| Benchmark | Operations | Time per Op | Memory per Op | Allocs per Op |
|-----------|------------|-------------|---------------|---------------|
| **Ingest** | 64 | 18.16 ms | 1.92 MB | 452 |
| **Scan (No Predicate)** | 1,740,811 | 662 ns | 1.30 KB | 28 |
| **Scan (With Predicate)** | 1,376,605 | 874 ns | 1.63 KB | 29 |
| **Aggregation Count** | 1,401,103 | 859 ns | 2.22 KB | 36 |
| **Aggregation Sum** | 1,245,460 | 965 ns | 2.29 KB | 42 |
| **Aggregation GroupBy** | 809,755 | 1.35 µs | 2.74 KB | 52 |
| **Predicate Pushdown** | 1,339,988 | 906 ns | 1.66 KB | 32 |
| **Parallel Read** | 2,691,224 | 495 ns | 696 B | 28 |
| **Explain** | 2,081,295 | 562 ns | 1.17 KB | 19 |

### Chunk Size Comparison

| Chunk Size | Time per Op | Memory per Op | Allocs per Op |
|------------|-------------|---------------|---------------|
| 1,024 | 20.80 ms | 7.79 MB | 10,289 |
| 4,096 | 21.24 ms | 6.87 MB | 3,071 |
| 16,384 | 22.32 ms | 6.76 MB | 1,231 |
| 65,536 | 21.63 ms | 7.96 MB | 586 |

### Analysis

- **Parallel Read** is fastest (495ns) due to multi-worker design
- **Scan** operations achieve ~1.5M ops/sec with minimal allocations
- **Predicate pushdown** adds ~200ns overhead but reduces data transfer
- **Larger chunk sizes** reduce allocation count significantly (10K→586)
- **Ingestion** is the bottleneck at ~55 ops/sec (18ms per batch)

### Available Benchmarks

- `BenchmarkIngest` - Write throughput
- `BenchmarkScanNoPredicate` - Full table scan
- `BenchmarkScanWithPredicate` - Scan with filter
- `BenchmarkAggregationCount` - COUNT aggregation
- `BenchmarkAggregationSum` - SUM aggregation
- `BenchmarkAggregationGroupBy` - GROUP BY performance
- `BenchmarkPredicatePushdown` - Pushdown effectiveness
- `BenchmarkParallelRead` - Parallel I/O performance
- `BenchmarkChunkSize` - Different chunk sizes

## Configuration

```go
type EngineConfig struct {
    StoragePath string          // Database file path
    Memory      memory.Allocator // Arrow memory allocator
}
```

## Testing

```bash
# Run all tests
go test ./...

# Run with coverage
go test -cover ./...

# Run benchmarks
go test -bench=. -benchmem ./...
```

## Dependencies

- `go.etcd.io/bbolt` - Embedded key-value store
- `github.com/apache/arrow-go/v18` - Arrow implementation

## License

MIT