package query

import (
	"strings"

	"github.com/TFMV/wildcat/execution"
	"github.com/TFMV/wildcat/format"
	"github.com/TFMV/wildcat/storage"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

type Planner struct {
	engine *storage.Engine
}

func NewPlanner(engine *storage.Engine) *Planner {
	return &Planner{engine: engine}
}

func (p *Planner) Plan(ast *AST) (ExecutionPlan, error) {
	if err := ast.Validate(); err != nil {
		return nil, err
	}

	logicalPlan := p.createLogicalPlan(ast)

	physicalPlan := p.optimizeLogicalPlan(logicalPlan)

	return physicalPlan, nil
}

func (p *Planner) createLogicalPlan(ast *AST) LogicalPlan {
	var plan LogicalPlan

	scan := LogicalScan{
		Dataset:   ast.Dataset,
		Partition: "default",
		Columns:   collectColumns(ast.Columns),
	}
	plan = &scan

	if ast.Where != nil {
		filter := LogicalFilter{
			Input: plan,
			Predicate: execution.Predicate{
				Column: ast.Where.Column,
				Type:   ast.Where.Type,
				Value:  ast.Where.Value,
			},
		}
		plan = &filter
	}

	if len(ast.GroupBy) > 0 || hasAggregation(ast.Columns) {
		var aggs []AggregationSpec
		for _, col := range ast.Columns {
			if col.Function != "" {
				aggs = append(aggs, AggregationSpec{
					Column: col.Name,
					Type:   AggregationType(col.Function),
				})
			}
		}

		agg := LogicalAggregate{
			Input:    plan,
			GroupBy:  ast.GroupBy,
			AggSpecs: aggs,
		}
		plan = &agg
	}

	if len(ast.Columns) > 0 && hasProjection(ast.Columns) {
		proj := LogicalProject{
			Input:   plan,
			Columns: ast.Columns,
		}
		plan = &proj
	}

	return plan
}

func (p *Planner) optimizeLogicalPlan(plan LogicalPlan) ExecutionPlan {
	switch node := plan.(type) {
	case *LogicalScan:
		return p.optimizeScan(node)
	case *LogicalFilter:
		child := p.optimizeLogicalPlan(node.Input)
		return &PhysicalFilter{
			Input: child,
			Predicate: execution.Predicate{
				Column: node.Predicate.Column,
				Type:   node.Predicate.Type,
				Value:  node.Predicate.Value,
			},
		}
	case *LogicalAggregate:
		child := p.optimizeLogicalPlan(node.Input)
		return &PhysicalAggregate{
			Input:    child,
			GroupBy:  node.GroupBy,
			AggSpecs: node.AggSpecs,
		}
	case *LogicalProject:
		child := p.optimizeLogicalPlan(node.Input)
		return &PhysicalProject{
			Input:   child,
			Columns: node.Columns,
		}
	default:
		return &PhysicalScan{
			Dataset:   "",
			Partition: "default",
			Columns:   nil,
			engine:    p.engine,
		}
	}
}

func (p *Planner) optimizeScan(scan *LogicalScan) ExecutionPlan {
	ops := execution.NewScanOperator(p.engine, scan.Dataset, scan.Partition, scan.Columns, nil)

	if scan.Predicate.Column != "" {
		storagePred := storage.Predicate{
			Column: scan.Predicate.Column,
			Type:   storage.PredicateType(scan.Predicate.Type),
			Value:  scan.Predicate.Value,
		}
		ops.WithPredicate(&storagePred)
	}

	return &PhysicalScanOp{
		Operator:  ops,
		Dataset:   scan.Dataset,
		Partition: scan.Partition,
		Columns:   scan.Columns,
		engine:    p.engine,
	}
}

type LogicalPlan interface {
	String() string
}

type LogicalScan struct {
	Dataset   string
	Partition string
	Columns   []string
	Predicate execution.Predicate
}

func (s *LogicalScan) String() string {
	return "Scan"
}

type LogicalFilter struct {
	Input     LogicalPlan
	Predicate execution.Predicate
}

func (f *LogicalFilter) String() string {
	return "Filter"
}

type LogicalProject struct {
	Input   LogicalPlan
	Columns []Column
}

func (p *LogicalProject) String() string {
	return "Project"
}

type LogicalAggregate struct {
	Input    LogicalPlan
	GroupBy  []string
	AggSpecs []AggregationSpec
}

func (a *LogicalAggregate) String() string {
	return "Aggregate"
}

type ExecutionPlan interface {
	Execute() (format.RecordBatch, error)
	Schema() *arrow.Schema
	String() string
}

type PhysicalScanOp struct {
	Operator  *execution.ScanOperator
	Dataset   string
	Partition string
	Columns   []string
	engine    *storage.Engine
	result    *execution.ScanResult
	executed  bool
}

func (s *PhysicalScanOp) Execute() (format.RecordBatch, error) {
	if s.executed {
		if s.result != nil && s.result.Record != nil {
			return s.result.Record, nil
		}
		return nil, nil
	}

	result, err := s.Operator.Execute(nil)
	s.executed = true
	s.result = result

	if err != nil {
		return nil, err
	}

	if result != nil && result.Record != nil {
		return result.Record, nil
	}

	return nil, nil
}

func (s *PhysicalScanOp) Schema() *arrow.Schema {
	if s.result != nil && s.result.Record != nil {
		return s.result.Record.Schema()
	}

	fields := make([]arrow.Field, len(s.Columns))
	for i, col := range s.Columns {
		fields[i] = arrow.Field{Name: col, Type: &arrow.StringType{}}
	}
	return arrow.NewSchema(fields, nil)
}

func (s *PhysicalScanOp) String() string {
	return "PhysicalScan"
}

type PhysicalFilter struct {
	Input     ExecutionPlan
	Predicate execution.Predicate
	result    format.RecordBatch
	executed  bool
}

func (f *PhysicalFilter) Execute() (format.RecordBatch, error) {
	if f.executed {
		return f.result, nil
	}

	inputResult, err := f.Input.Execute()
	if err != nil {
		return nil, err
	}

	if inputResult == nil {
		f.executed = true
		return nil, nil
	}

	for i := 0; i < int(inputResult.NumCols()); i++ {
		colName := inputResult.ColumnName(i)
		if colName == f.Predicate.Column {
			filtered, err := execution.ApplyPredicate(inputResult.Column(i), f.Predicate, nil)
			if err != nil {
				return nil, err
			}

			fields := make([]arrow.Field, inputResult.NumCols())
			columns := make([]arrow.Array, inputResult.NumCols())

			for j := 0; j < int(inputResult.NumCols()); j++ {
				fields[j] = arrow.Field{
					Name: inputResult.ColumnName(j),
					Type: inputResult.Column(j).DataType(),
				}
				if j == i && filtered != nil {
					columns[j] = filtered
				} else {
					columns[j] = inputResult.Column(j)
				}
			}

			f.result = format.NewRecordBatch(arrow.NewSchema(fields, nil), columns, inputResult.NumRows())
			break
		}
	}

	f.executed = true
	return f.result, nil
}

func (f *PhysicalFilter) Schema() *arrow.Schema {
	return f.Input.Schema()
}

func (f *PhysicalFilter) String() string {
	return "PhysicalFilter"
}

type PhysicalAggregate struct {
	Input    ExecutionPlan
	GroupBy  []string
	AggSpecs []AggregationSpec
	result   format.RecordBatch
	executed bool
}

func (a *PhysicalAggregate) Execute() (format.RecordBatch, error) {
	if a.executed {
		return a.result, nil
	}

	inputResult, err := a.Input.Execute()
	if err != nil {
		return nil, err
	}

	if inputResult == nil {
		a.executed = true
		return nil, nil
	}

	if len(a.GroupBy) == 0 && len(a.AggSpecs) > 0 {
		chunks := make([]format.Chunk, 0)
		for i := 0; i < int(inputResult.NumCols()); i++ {
			chunks = append(chunks, format.Chunk{
				ColumnName: inputResult.ColumnName(i),
				Array:      inputResult.Column(i),
				NumRows:    inputResult.NumRows(),
			})
		}

		values := make(map[string]interface{})
		for _, spec := range a.AggSpecs {
			result, _ := execution.ComputeAggregation(chunks, execution.AggregationSpec{
				Column: spec.Column,
				Type:   execution.AggregationType(spec.Type),
			}, nil)

			if result != nil {
				values[spec.Column] = result.Values[spec.Column]
			}
		}

		fields := make([]arrow.Field, len(a.AggSpecs))
		columns := make([]arrow.Array, len(a.AggSpecs))

		for i, spec := range a.AggSpecs {
			fields[i] = arrow.Field{Name: spec.Column, Type: &arrow.Int64Type{}}
			columns[i] = array.NewInt64Builder(memory.DefaultAllocator).NewArray()
		}

		a.result = format.NewRecordBatch(arrow.NewSchema(fields, nil), columns, 1)
	}

	a.executed = true
	return a.result, nil
}

func (a *PhysicalAggregate) Schema() *arrow.Schema {
	fields := make([]arrow.Field, len(a.AggSpecs))
	for i, spec := range a.AggSpecs {
		fields[i] = arrow.Field{Name: spec.Column, Type: &arrow.Int64Type{}}
	}
	return arrow.NewSchema(fields, nil)
}

func (a *PhysicalAggregate) String() string {
	return "PhysicalAggregate"
}

type PhysicalProject struct {
	Input    ExecutionPlan
	Columns  []Column
	result   format.RecordBatch
	executed bool
}

func (p *PhysicalProject) Execute() (format.RecordBatch, error) {
	if p.executed {
		return p.result, nil
	}

	inputResult, err := p.Input.Execute()
	if err != nil {
		return nil, err
	}

	if inputResult == nil {
		p.executed = true
		return nil, nil
	}

	colIndices := make(map[string]int)
	for i := 0; i < int(inputResult.NumCols()); i++ {
		colIndices[inputResult.ColumnName(i)] = i
	}

	newColumns := make([]arrow.Array, 0)
	newFields := make([]arrow.Field, 0)

	for _, col := range p.Columns {
		if idx, ok := colIndices[col.Name]; ok {
			newColumns = append(newColumns, inputResult.Column(idx))
			newFields = append(newFields, arrow.Field{
				Name: col.Name,
				Type: inputResult.Column(idx).DataType(),
			})
		}
	}

	if len(newColumns) > 0 {
		p.result = format.NewRecordBatch(arrow.NewSchema(newFields, nil), newColumns, inputResult.NumRows())
	}

	p.executed = true
	return p.result, nil
}

func (p *PhysicalProject) Schema() *arrow.Schema {
	fields := make([]arrow.Field, len(p.Columns))
	for i, col := range p.Columns {
		fields[i] = arrow.Field{Name: col.Name, Type: &arrow.StringType{}}
	}
	return arrow.NewSchema(fields, nil)
}

func (p *PhysicalProject) String() string {
	return "PhysicalProject"
}

type PhysicalScan struct {
	Dataset   string
	Partition string
	Columns   []string
	engine    *storage.Engine
}

func (s *PhysicalScan) Execute() (format.RecordBatch, error) {
	return nil, nil
}

func (s *PhysicalScan) Schema() *arrow.Schema {
	return nil
}

func (s *PhysicalScan) String() string {
	return "PhysicalScan"
}

func collectColumns(cols []Column) []string {
	result := make([]string, 0)
	for _, col := range cols {
		if col.Name != "*" && col.Function == "" {
			result = append(result, col.Name)
		}
	}
	return result
}

func hasAggregation(cols []Column) bool {
	for _, col := range cols {
		if col.Function != "" {
			return true
		}
	}
	return false
}

func hasProjection(cols []Column) bool {
	for _, col := range cols {
		if col.Alias != "" || col.Function != "" {
			return true
		}
	}
	return false
}

func (p *Planner) Explain(ast *AST) (string, error) {
	plan, err := p.Plan(ast)
	if err != nil {
		return "", err
	}

	var b strings.Builder
	b.WriteString("Logical Plan:\n")
	b.WriteString(p.explainLogical(ast))
	b.WriteString("\n\nPhysical Plan:\n")
	b.WriteString(plan.String())

	return b.String(), nil
}

func (p *Planner) explainLogical(ast *AST) string {
	var b strings.Builder
	b.WriteString("  - Scan: ")
	b.WriteString(ast.Dataset)
	if len(ast.Columns) > 0 {
		b.WriteString(" columns:")
		for _, col := range ast.Columns {
			b.WriteString(" ")
			b.WriteString(col.Name)
		}
	}

	if ast.Where != nil {
		b.WriteString("\n  - Filter: ")
		b.WriteString(ast.Where.Column)
		b.WriteString(" ")
		b.WriteString(string(ast.Where.Type))
		b.WriteString(" ")
		b.WriteString(string(ast.Where.Value.(string)))
	}

	if len(ast.GroupBy) > 0 {
		b.WriteString("\n  - GroupBy: ")
		b.WriteString(strings.Join(ast.GroupBy, ", "))
	}

	return b.String()
}
