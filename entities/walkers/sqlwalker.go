package walkers

import (
	"fmt"
	"strings"
	"time"

	"github.com/verticalgmbh/database-go/entities/models"
	"github.com/verticalgmbh/database-go/interfaces"

	"github.com/verticalgmbh/database-go/connection"
	"github.com/verticalgmbh/database-go/xpr"
)

// SqlWalker used to convert expressions to sql strings
type SqlWalker struct {
	connectioninfo connection.IConnectionInfo
	builder        *strings.Builder
}

// NewSqlWalker creates a new SqlWalker
//
// **Parameters**
//   - connectioninfo: driver specific connection info
//   - builder       : command builder to fill
//
// **Returns**
//   - *SqlWalker: created sql walker
func NewSqlWalker(connectioninfo connection.IConnectionInfo, builder *strings.Builder) *SqlWalker {
	return &SqlWalker{
		connectioninfo: connectioninfo,
		builder:        builder}
}

// Visit creates an sql representation of a given expression tree
//
// **Parameters**
//   - tree: expression to evaluate
func (walker *SqlWalker) Visit(tree interface{}) error {
	if pstat, ok := tree.(interfaces.IPreparedOperation); ok {
		walker.visitStatement(pstat)
		return nil
	}

	switch v := tree.(type) {
	default:
		walker.visitValue(tree)
	case *xpr.UnaryNode:
		walker.visitUnary(v)
	case xpr.UnaryNode:
		walker.visitUnary(&v)
	case *xpr.BinaryNode:
		walker.visitBinary(v)
	case xpr.BinaryNode:
		walker.visitBinary(&v)
	case xpr.ParameterNode:
		walker.visitParameter(&v)
	case *xpr.ParameterNode:
		walker.visitParameter(v)
	case xpr.FieldNode:
		walker.visitField(&v)
	case *xpr.FieldNode:
		walker.visitField(v)
	case xpr.ColumnNode:
		walker.visitColumn(&v)
	case *xpr.ColumnNode:
		walker.visitColumn(v)
	case xpr.AliasNode:
		walker.visitAlias(&v)
	case *xpr.AliasNode:
		walker.visitAlias(v)
	case xpr.FunctionNode:
		return walker.visitFunction(&v)
	case *xpr.FunctionNode:
		return walker.visitFunction(v)
	case *models.ColumnDescriptor:
		walker.builder.WriteString(v.Name())
	case models.ColumnDescriptor:
		walker.builder.WriteString(v.Name())
	case *xpr.InCollectionNode:
		walker.visitIn(v)
	case xpr.InCollectionNode:
		walker.visitIn(&v)
	case *xpr.StatementNode:
		walker.visitStatement(v.Statement)
	case xpr.StatementNode:
		walker.visitStatement(v.Statement)
	case *xpr.TableNode:
		walker.builder.WriteString(v.Name)
	case xpr.TableNode:
		walker.builder.WriteString(v.Name)
	}

	return nil
}

func (walker *SqlWalker) visitStatement(statement interfaces.IPreparedOperation) {
	walker.builder.WriteRune('(')
	walker.builder.WriteString(statement.Command())
	walker.builder.WriteRune(')')
}

func (walker *SqlWalker) visitIn(node *xpr.InCollectionNode) {
	walker.Visit(node.Item())
	walker.builder.WriteString(" IN (")
	for index, item := range node.Collection() {
		if index > 0 {
			walker.builder.WriteRune(',')
		}
		walker.Visit(item)
	}
	walker.builder.WriteRune(')')
}

func (walker *SqlWalker) visitFunction(node *xpr.FunctionNode) error {
	return walker.connectioninfo.EvaluateFunction(node, walker.builder, walker.Visit)
}

func (walker *SqlWalker) visitParameter(node *xpr.ParameterNode) {
	walker.connectioninfo.EvaluateParameter(node, walker.builder)
}

func (walker *SqlWalker) visitAlias(node *xpr.AliasNode) {
	walker.builder.WriteString(node.Alias)
	walker.builder.WriteRune('.')
	walker.Visit(node.Field)
}

func (walker *SqlWalker) visitField(node *xpr.FieldNode) {
	column := node.Model().ColumnFromField(node.Name())
	walker.builder.WriteString(walker.connectioninfo.MaskColumn(column.Name()))
}

func (walker *SqlWalker) visitColumn(node *xpr.ColumnNode) {
	walker.builder.WriteString(walker.connectioninfo.MaskColumn(node.Name))
}

func (walker *SqlWalker) visitUnary(node *xpr.UnaryNode) {
	switch node.Operator() {
	case xpr.Not:
		walker.builder.WriteRune('!')
	case xpr.Complement:
		walker.builder.WriteRune('~')
	case xpr.Negate:
		walker.builder.WriteRune('-')
	}

	walker.Visit(node.Value())
}

func (walker *SqlWalker) visitBinary(node *xpr.BinaryNode) {
	walker.Visit(node.Lhs())

	switch node.Operator() {
	case xpr.BinaryAnd:
		walker.builder.WriteString(" AND ")
	case xpr.BinaryOr:
		walker.builder.WriteString(" OR ")
	case xpr.BinaryEquals:
		if node.Rhs() == nil {
			walker.builder.WriteString(" IS NULL")
			return
		}

		walker.builder.WriteString(" = ")
	case xpr.BinaryNotEqual:
		if node.Rhs() == nil {
			walker.builder.WriteString(" IS NOT NULL")
			return
		}

		walker.builder.WriteString(" <> ")
	case xpr.BinaryAssign:
		walker.builder.WriteString(" = ")
	case xpr.BinaryGreater:
		walker.builder.WriteString(" > ")
	case xpr.BinaryGreaterEqual:
		walker.builder.WriteString(" >= ")
	case xpr.BinaryLess:
		walker.builder.WriteString(" < ")
	case xpr.BinaryLessEqual:
		walker.builder.WriteString(" <= ")
	case xpr.BinaryAdd:
		walker.builder.WriteString(" + ")
	case xpr.BinarySub:
		walker.builder.WriteString(" - ")
	case xpr.BinaryDiv:
		walker.builder.WriteString(" / ")
	case xpr.BinaryMul:
		walker.builder.WriteString(" * ")
	case xpr.BinaryMod:
		walker.builder.WriteString(" % ")
	case xpr.BinaryShl:
		walker.builder.WriteString(" << ")
	case xpr.BinaryShr:
		walker.builder.WriteString(" >> ")
	case xpr.BinaryBitAnd:
		walker.builder.WriteString(" & ")
	case xpr.BinaryBitOr:
		walker.builder.WriteString(" | ")
	case xpr.BinaryBitXor:
		walker.builder.WriteString(" ^ ")
	}

	walker.Visit(node.Rhs())
}

func (walker *SqlWalker) visitValue(value interface{}) {
	if value == nil {
		walker.builder.WriteString("NULL")
		return
	}

	switch v := value.(type) {
	case string:
		walker.builder.WriteRune('\'')
		for _, character := range v {
			switch character {
			case '\'', '%', '_', '\\', '\n', '\r', '\t':
				walker.builder.WriteRune('\\')
			}
			walker.builder.WriteRune(character)
		}
		walker.builder.WriteRune('\'')
	case time.Time:
		walker.builder.WriteString(fmt.Sprintf("'%04d-%02d-%02d %02d:%02d:%02d'", v.Year(), v.Month(), v.Day(), v.Hour(), v.Minute(), v.Second()))
	default:
		walker.builder.WriteString(fmt.Sprintf("%v", value))
	}
}
