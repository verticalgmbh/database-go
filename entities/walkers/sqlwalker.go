package walkers

import (
	"fmt"
	"strings"

	"github.com/verticalgmbh/database-go/entities/models"

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
func (walker *SqlWalker) Visit(tree interface{}) {
	switch tree.(type) {
	default:
		walker.visitValue(tree)
	case *xpr.UnaryNode:
		unary, _ := tree.(*xpr.UnaryNode)
		walker.visitUnary(unary)
	case xpr.UnaryNode:
		unary, _ := tree.(xpr.UnaryNode)
		walker.visitUnary(&unary)
	case *xpr.BinaryNode:
		binary, _ := tree.(*xpr.BinaryNode)
		walker.visitBinary(binary)
	case xpr.BinaryNode:
		binary, _ := tree.(xpr.BinaryNode)
		walker.visitBinary(&binary)
	case xpr.ParameterNode:
		parameter, _ := tree.(xpr.ParameterNode)
		walker.visitParameter(&parameter)
	case *xpr.ParameterNode:
		parameter, _ := tree.(*xpr.ParameterNode)
		walker.visitParameter(parameter)
	case xpr.FieldNode:
		field, _ := tree.(xpr.FieldNode)
		walker.visitField(&field)
	case *xpr.FieldNode:
		field, _ := tree.(*xpr.FieldNode)
		walker.visitField(field)
	case xpr.FunctionNode:
		function, _ := tree.(xpr.FunctionNode)
		walker.visitFunction(&function)
	case *xpr.FunctionNode:
		function, _ := tree.(*xpr.FunctionNode)
		walker.visitFunction(function)
	case *models.ColumnDescriptor:
		column, _ := tree.(*models.ColumnDescriptor)
		walker.builder.WriteString(column.Name())
	case models.ColumnDescriptor:
		column, _ := tree.(models.ColumnDescriptor)
		walker.builder.WriteString(column.Name())
	}
}

func (walker *SqlWalker) visitFunction(node *xpr.FunctionNode) {
	walker.connectioninfo.EvaluateFunction(node, walker.builder)
}

func (walker *SqlWalker) visitParameter(node *xpr.ParameterNode) {
	walker.connectioninfo.EvaluateParameter(node, walker.builder)
}

func (walker *SqlWalker) visitField(node *xpr.FieldNode) {
	column := node.Model().ColumnFromField(node.Name())
	walker.builder.WriteString(walker.connectioninfo.MaskColumn(column.Name()))
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

	switch value.(type) {
	case string:
		stringvalue, _ := value.(string)
		walker.builder.WriteRune('\'')
		for _, character := range stringvalue {
			switch character {
			case '\'', '%', '_', '\\', '\n', '\r', '\t':
				walker.builder.WriteRune('\\')
			}
			walker.builder.WriteRune(character)
		}
		walker.builder.WriteRune('\'')
	default:
		walker.builder.WriteString(fmt.Sprintf("%v", value))
	}
}
