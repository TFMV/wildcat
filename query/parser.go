package query

import (
	"fmt"
	"strings"

	"github.com/TFMV/wildcat/execution"
)

type Parser struct{}

func NewParser() *Parser {
	return &Parser{}
}

func (p *Parser) Parse(sql string) (*AST, error) {
	sql = strings.TrimSpace(sql)

	if !strings.HasPrefix(strings.ToUpper(sql), "SELECT") {
		return nil, fmt.Errorf("only SELECT queries supported")
	}

	ast, err := p.parseSelect(sql)
	if err != nil {
		return nil, err
	}

	if err := ast.Validate(); err != nil {
		return nil, err
	}

	return ast, nil
}

func (p *Parser) parseSelect(sql string) (*AST, error) {
	ast := &AST{}

	sqlUpper := strings.ToUpper(sql)

	selectIdx := strings.Index(sqlUpper, "SELECT")
	fromIdx := strings.Index(sqlUpper, "FROM")

	if selectIdx < 0 || fromIdx < 0 {
		return nil, fmt.Errorf("invalid SELECT syntax")
	}

	cols := strings.TrimSpace(sql[selectIdx+6 : fromIdx])
	ast.Columns = p.parseColumns(cols)

	rest := sql[fromIdx+4:]

	whereIdx := strings.Index(strings.ToUpper(rest), "WHERE")
	groupIdx := strings.Index(strings.ToUpper(rest), "GROUP BY")

	var dataset, whereClause, groupClause string

	if whereIdx > 0 && (groupIdx < 0 || whereIdx < groupIdx) {
		dataset = strings.TrimSpace(rest[:whereIdx])
		if groupIdx > 0 {
			whereClause = strings.TrimSpace(rest[whereIdx+5 : groupIdx])
			groupClause = strings.TrimSpace(rest[groupIdx+8:])
		} else {
			whereClause = strings.TrimSpace(rest[whereIdx+5:])
		}
	} else if groupIdx > 0 {
		dataset = strings.TrimSpace(rest[:groupIdx])
		groupClause = strings.TrimSpace(rest[groupIdx+8:])
	} else {
		dataset = strings.TrimSpace(rest)
	}

	ast.Dataset = strings.TrimSpace(dataset)

	if whereClause != "" {
		ast.Where = p.parsePredicate(whereClause)
	}

	if groupClause != "" {
		ast.GroupBy = p.parseGroupBy(groupClause)
	}

	return ast, nil
}

func (p *Parser) parseColumns(cols string) []Column {
	if strings.TrimSpace(cols) == "*" {
		return nil
	}

	parts := strings.Split(cols, ",")
	result := make([]Column, 0, len(parts))

	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}

		col := Column{Name: part}

		upper := strings.ToUpper(part)
		if strings.HasSuffix(upper, " AS ") {
			asIdx := strings.LastIndex(part, " AS ")
			col.Name = strings.TrimSpace(part[:asIdx])
			col.Alias = strings.TrimSpace(part[asIdx+4:])
		} else if idx := strings.Index(part, " "); idx > 0 {
			col.Alias = strings.TrimSpace(part[idx+1:])
			col.Name = strings.TrimSpace(part[:idx])
		}

		if strings.HasPrefix(strings.ToUpper(col.Name), "COUNT(") {
			col.Function = "count"
			col.Name = strings.Trim(strings.Trim(col.Name, "COUNT("), ")")
		} else if strings.HasPrefix(strings.ToUpper(col.Name), "SUM(") {
			col.Function = "sum"
			col.Name = strings.Trim(strings.Trim(col.Name, "SUM("), ")")
		} else if strings.HasPrefix(strings.ToUpper(col.Name), "AVG(") {
			col.Function = "avg"
			col.Name = strings.Trim(strings.Trim(col.Name, "AVG("), ")")
		} else if strings.HasPrefix(strings.ToUpper(col.Name), "MIN(") {
			col.Function = "min"
			col.Name = strings.Trim(strings.Trim(col.Name, "MIN("), ")")
		} else if strings.HasPrefix(strings.ToUpper(col.Name), "MAX(") {
			col.Function = "max"
			col.Name = strings.Trim(strings.Trim(col.Name, "MAX("), ")")
		}

		result = append(result, col)
	}

	return result
}

func (p *Parser) parsePredicate(where string) *Predicate {
	where = strings.TrimSpace(where)

	ops := []string{"!=", ">=", "<=", "=", ">", "<"}
	var op string
	var opIdx int

	for _, o := range ops {
		if idx := strings.Index(where, o); idx > 0 {
			op = o
			opIdx = idx
			break
		}
	}

	if op == "" {
		return nil
	}

	col := strings.TrimSpace(where[:opIdx])
	val := strings.TrimSpace(where[opIdx+len(op):])
	val = strings.Trim(val, "'")

	var predType execution.PredicateType
	switch op {
	case "=":
		predType = execution.PredicateEq
	case "!=":
		predType = execution.PredicateNeq
	case "<":
		predType = execution.PredicateLt
	case "<=":
		predType = execution.PredicateLte
	case ">":
		predType = execution.PredicateGt
	case ">=":
		predType = execution.PredicateGte
	default:
		predType = execution.PredicateEq
	}

	return &Predicate{
		Column: col,
		Type:   predType,
		Value:  val,
	}
}

func (p *Parser) parseGroupBy(group string) []string {
	parts := strings.Split(group, ",")
	result := make([]string, 0, len(parts))

	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			result = append(result, part)
		}
	}

	return result
}

type AST struct {
	Columns []Column
	Dataset string
	Where   *Predicate
	GroupBy []string
}

func (a *AST) Validate() error {
	if a.Dataset == "" {
		return fmt.Errorf("dataset is required")
	}

	for _, col := range a.Columns {
		if col.Name == "" {
			return fmt.Errorf("column name cannot be empty")
		}
	}

	for _, col := range a.GroupBy {
		if col == "" {
			return fmt.Errorf("group by column cannot be empty")
		}
	}

	return nil
}

func (a *AST) ToPlan() *execution.Plan {
	plan := &execution.Plan{}

	columns := make([]string, 0)
	var aggSpec *AggregationSpec
	var groupCol string

	for _, col := range a.Columns {
		if col.Function != "" {
			if aggSpec == nil {
				groupCol = a.GroupBy[0]
			}
			aggSpec = &AggregationSpec{
				Column: col.Name,
				Type:   AggregationType(col.Function),
			}
		} else if col.Name != "*" {
			columns = append(columns, col.Name)
		}
	}

	scan := &execution.Scan{
		Dataset:   a.Dataset,
		Partition: "default",
		Columns:   columns,
	}

	var root execution.Operator = scan

	if a.Where != nil {
		root = &execution.Filter{
			Input: root,
			Predicate: execution.Predicate{
				Column: a.Where.Column,
				Type:   a.Where.Type,
				Value:  a.Where.Value,
			},
		}
	}

	if aggSpec != nil {
		agg := &execution.Aggregate{
			Input:   root,
			AggType: string(aggSpec.Type),
			Column:  aggSpec.Column,
			GroupBy: groupCol,
		}
		root = agg
	}

	plan.Root = root
	return plan
}

type Column struct {
	Name     string
	Alias    string
	Function string
}

type Predicate struct {
	Column string
	Type   execution.PredicateType
	Value  interface{}
}

type AggregationSpec struct {
	Column string
	Type   AggregationType
}

type AggregationType string

func (a *AST) String() string {
	var b strings.Builder

	b.WriteString("SELECT ")
	if len(a.Columns) == 0 {
		b.WriteString("*")
	} else {
		for i, col := range a.Columns {
			if i > 0 {
				b.WriteString(", ")
			}
			if col.Function != "" {
				b.WriteString(strings.ToUpper(col.Function))
				b.WriteString("(")
				b.WriteString(col.Name)
				b.WriteString(")")
			} else {
				b.WriteString(col.Name)
			}
			if col.Alias != "" {
				b.WriteString(" AS ")
				b.WriteString(col.Alias)
			}
		}
	}

	b.WriteString(" FROM ")
	b.WriteString(a.Dataset)

	if a.Where != nil {
		b.WriteString(" WHERE ")
		b.WriteString(a.Where.Column)
		b.WriteString(" ")
		b.WriteString(string(a.Where.Type))
		b.WriteString(" ")
		b.WriteString(fmt.Sprintf("%v", a.Where.Value))
	}

	if len(a.GroupBy) > 0 {
		b.WriteString(" GROUP BY ")
		b.WriteString(strings.Join(a.GroupBy, ", "))
	}

	return b.String()
}
