package query

import (
	"bytes"
	"strings"

	"github.com/TFMV/wildcat/execution"
	"github.com/TFMV/wildcat/storage"
	"go.etcd.io/bbolt"
)

type Optimizer struct {
	engine *storage.Engine
}

func NewOptimizer(engine *storage.Engine) *Optimizer {
	return &Optimizer{engine: engine}
}

func (o *Optimizer) Optimize(plan LogicalPlan) (LogicalPlan, error) {
	switch node := plan.(type) {
	case *LogicalScan:
		return o.optimizeScan(node)
	case *LogicalFilter:
		child, err := o.Optimize(node.Input)
		if err != nil {
			return nil, err
		}
		node.Input = child
		return o.optimizeFilter(node)
	default:
		return plan, nil
	}
}

func (o *Optimizer) optimizeScan(scan *LogicalScan) (LogicalPlan, error) {
	stats, err := o.collectPartitionStats(scan.Dataset, scan.Partition)
	if err != nil || len(stats) == 0 {
		return scan, nil
	}

	var filtered []storage.ChunkStats
	for _, colStats := range stats {
		for _, s := range colStats {
			if shouldIncludeChunk(s, scan.Predicate) {
				filtered = append(filtered, s)
			}
		}
	}

	if len(filtered) == 0 {
		return &LogicalEmptyScan{Dataset: scan.Dataset}, nil
	}

	return scan, nil
}

func (o *Optimizer) optimizeFilter(filter *LogicalFilter) (LogicalPlan, error) {
	pred := filter.Predicate

	stats, err := o.collectPredicateStats(filter.Input, pred.Column)
	if err != nil {
		return filter, nil
	}

	if len(stats) == 0 {
		return filter, nil
	}

	totalChunks := len(stats)
	prunedChunks := 0

	for _, s := range stats {
		if shouldSkipChunkBasedOnStats(s, pred) {
			prunedChunks++
		}
	}

	if prunedChunks == totalChunks {
		return &LogicalEmptyScan{Dataset: ""}, nil
	}

	return filter, nil
}

func (o *Optimizer) collectPartitionStats(dataset, partition string) (map[string][]storage.ChunkStats, error) {
	result := make(map[string][]storage.ChunkStats)

	cols, err := o.getColumnsInPartition(dataset, partition)
	if err != nil {
		return nil, err
	}

	for _, col := range cols {
		stats, err := o.engine.ReadColumnStats(nil, dataset, partition, col)
		if err != nil {
			continue
		}
		result[col] = stats
	}

	return result, nil
}

func (o *Optimizer) collectPredicateStats(input LogicalPlan, column string) ([]storage.ChunkStats, error) {
	scan, ok := input.(*LogicalScan)
	if !ok {
		return nil, nil
	}

	return o.engine.ReadColumnStats(nil, scan.Dataset, scan.Partition, column)
}

func (o *Optimizer) getColumnsInPartition(dataset, partition string) ([]string, error) {
	var columns []string

	err := o.engine.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte(storage.BucketMeta))
		if bucket == nil {
			return nil
		}

		prefix := []byte(dataset + "/" + partition + "/")
		cursor := bucket.Cursor()

		for k, _ := cursor.Seek(prefix); k != nil; k, _ = cursor.Next() {
			if len(k) > len(prefix) {
				rest := k[len(prefix):]
				if idx := bytes.Index(rest, []byte("/meta")); idx > 0 {
					col := string(rest[:idx])
					if !contains(columns, col) {
						columns = append(columns, col)
					}
				}
			}
		}
		return nil
	})

	return columns, err
}

func shouldIncludeChunk(stats storage.ChunkStats, pred execution.Predicate) bool {
	if pred.Column == "" {
		return true
	}

	if stats.Column != pred.Column {
		return true
	}

	return !shouldSkipChunkBasedOnStats(stats, pred)
}

func shouldSkipChunkBasedOnStats(stats storage.ChunkStats, pred execution.Predicate) bool {
	switch pred.Type {
	case execution.PredicateEq:
		return !valueInRange(stats, pred.Value)
	case execution.PredicateNeq:
		return false
	case execution.PredicateLt:
		return !canBeLessThan(stats, pred.Value)
	case execution.PredicateLte:
		return !canBeLessThanOrEqual(stats, pred.Value)
	case execution.PredicateGt:
		return !canBeGreaterThan(stats, pred.Value)
	case execution.PredicateGte:
		return !canBeGreaterThanOrEqual(stats, pred.Value)
	}
	return false
}

func valueInRange(stats storage.ChunkStats, value interface{}) bool {
	if stats.MinValue == nil || stats.MaxValue == nil {
		return true
	}

	minBytes := stats.MinValue
	maxBytes := stats.MaxValue
	valBytes, ok := valueToBytes(value)
	if !ok {
		return true
	}

	return bytesCompare(valBytes, minBytes) >= 0 && bytesCompare(valBytes, maxBytes) <= 0
}

func canBeLessThan(stats storage.ChunkStats, value interface{}) bool {
	if stats.MaxValue == nil {
		return true
	}

	valBytes, ok := valueToBytes(value)
	if !ok {
		return true
	}

	return bytesCompare(stats.MaxValue, valBytes) > 0
}

func canBeLessThanOrEqual(stats storage.ChunkStats, value interface{}) bool {
	if stats.MaxValue == nil {
		return true
	}

	valBytes, ok := valueToBytes(value)
	if !ok {
		return true
	}

	return bytesCompare(stats.MaxValue, valBytes) >= 0
}

func canBeGreaterThan(stats storage.ChunkStats, value interface{}) bool {
	if stats.MinValue == nil {
		return true
	}

	valBytes, ok := valueToBytes(value)
	if !ok {
		return true
	}

	return bytesCompare(stats.MinValue, valBytes) < 0
}

func canBeGreaterThanOrEqual(stats storage.ChunkStats, value interface{}) bool {
	if stats.MinValue == nil {
		return true
	}

	valBytes, ok := valueToBytes(value)
	if !ok {
		return true
	}

	return bytesCompare(stats.MinValue, valBytes) <= 0
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
	bits := int64ToBytes(int64(v))
	encodeBigEndian(b, uint64(bits))
}

func int64ToBytes(i int64) uint64 {
	return uint64(i)
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

func contains(slice []string, s string) bool {
	for _, v := range slice {
		if v == s {
			return true
		}
	}
	return false
}

type LogicalEmptyScan struct {
	Dataset string
}

func (s *LogicalEmptyScan) String() string {
	return "EmptyScan"
}

type StatsBasedPlanner struct {
	planner   *Planner
	optimizer *Optimizer
}

func NewStatsBasedPlanner(engine *storage.Engine) *StatsBasedPlanner {
	return &StatsBasedPlanner{
		planner:   NewPlanner(engine),
		optimizer: NewOptimizer(engine),
	}
}

func (p *StatsBasedPlanner) Plan(ast *AST) (ExecutionPlan, error) {
	if err := ast.Validate(); err != nil {
		return nil, err
	}

	logicalPlan := p.planner.createLogicalPlan(ast)

	optimizedPlan, err := p.optimizer.Optimize(logicalPlan)
	if err != nil {
		return nil, err
	}

	physicalPlan := p.planner.optimizeLogicalPlan(optimizedPlan)

	return physicalPlan, nil
}

func (p *StatsBasedPlanner) Explain(ast *AST) (string, error) {
	logicalPlan := p.planner.createLogicalPlan(ast)

	optimizedPlan, err := p.optimizer.Optimize(logicalPlan)
	if err != nil {
		return "", err
	}

	var b strings.Builder
	b.WriteString("Logical Plan:\n")
	b.WriteString(p.planner.explainLogical(ast))

	b.WriteString("\n\nOptimized Plan:\n")
	b.WriteString(optimizedPlan.String())

	b.WriteString("\n\nStats-Based Optimizations Applied:\n")
	b.WriteString(p.explainOptimizations(ast))

	physicalPlan, _ := p.planner.Plan(ast)
	b.WriteString("\n\nPhysical Plan:\n")
	b.WriteString(physicalPlan.String())

	return b.String(), nil
}

func (p *StatsBasedPlanner) explainOptimizations(ast *AST) string {
	var b strings.Builder

	if ast.Where != nil {
		b.WriteString("- Predicate pushdown to storage layer\n")
		b.WriteString("- Chunk pruning based on min/max statistics\n")
		b.WriteString("- Column elimination for unused columns\n")
	}

	if len(ast.GroupBy) > 0 {
		b.WriteString("- Group aggregation optimization\n")
	}

	if b.Len() == 0 {
		b.WriteString("No optimizations applicable\n")
	}

	return b.String()
}
