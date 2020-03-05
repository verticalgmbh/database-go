package xpr

import (
	"github.com/verticalgmbh/database-go/entities/models"
	"github.com/verticalgmbh/database-go/interfaces"
)

var count *FunctionNode = &FunctionNode{function: FunctionCount}

// And creates a binary node for an AND operation
//
// **Parameters**
//   - lhs: left hand side operand
//   - rhs: right hand side operand
// **Returns**
//   - *BinaryNode: node to use in expression trees
func And(lhs interface{}, rhs interface{}) *BinaryNode {
	return Binary(lhs, BinaryAnd, rhs)
}

// Or creates a binary node for an OR operation
//
// **Parameters**
//   - lhs: left hand side operand
//   - rhs: right hand side operand
// **Returns**
//   - *BinaryNode: node to use in expression trees
func Or(lhs interface{}, rhs interface{}) *BinaryNode {
	return Binary(lhs, BinaryOr, rhs)
}

// Equals creates a binary node for an Equals operation
//
// **Parameters**
//   - lhs: left hand side operand
//   - rhs: right hand side operand
// **Returns**
//   - *BinaryNode: node to use in expression trees
func Equals(lhs interface{}, rhs interface{}) *BinaryNode {
	return Binary(lhs, BinaryEquals, rhs)
}

// EqualsNot creates a binary node for an Not Equal operation
//
// **Parameters**
//   - lhs: left hand side operand
//   - rhs: right hand side operand
// **Returns**
//   - *BinaryNode: node to use in expression trees
func EqualsNot(lhs interface{}, rhs interface{}) *BinaryNode {
	return Binary(lhs, BinaryNotEqual, rhs)
}

// Assign creates a binary node for an Assign operation
//
// **Parameters**
//   - lhs: left hand side operand
//   - rhs: right hand side operand
// **Returns**
//   - *BinaryNode: node to use in expression trees
func Assign(lhs interface{}, rhs interface{}) *BinaryNode {
	return Binary(lhs, BinaryAssign, rhs)
}

// Add creates a binary node used to add two values
//
// **Parameters**
//   - lhs: left hand side operand
//   - rhs: right hand side operand
// **Returns**
//   - *BinaryNode: node to use in expression trees
func Add(lhs interface{}, rhs interface{}) *BinaryNode {
	return Binary(lhs, BinaryAdd, rhs)
}

// Sub creates a binary node used to subtract a value from another
//
// **Parameters**
//   - lhs: left hand side operand
//   - rhs: right hand side operand
// **Returns**
//   - *BinaryNode: node to use in expression trees
func Sub(lhs interface{}, rhs interface{}) *BinaryNode {
	return Binary(lhs, BinarySub, rhs)
}

// Div creates a binary node used to divide a value by another
//
// **Parameters**
//   - lhs: left hand side operand
//   - rhs: right hand side operand
// **Returns**
//   - *BinaryNode: node to use in expression trees
func Div(lhs interface{}, rhs interface{}) *BinaryNode {
	return Binary(lhs, BinaryDiv, rhs)
}

// Mul creates a binary node used to multiply a value with another
//
// **Parameters**
//   - lhs: left hand side operand
//   - rhs: right hand side operand
// **Returns**
//   - *BinaryNode: node to use in expression trees
func Mul(lhs interface{}, rhs interface{}) *BinaryNode {
	return Binary(lhs, BinaryMul, rhs)
}

// Les creates a binary node used to compare whether a value is less than another
//
// **Parameters**
//   - lhs: left hand side operand
//   - rhs: right hand side operand
// **Returns**
//   - *BinaryNode: node to use in expression trees
func Les(lhs interface{}, rhs interface{}) *BinaryNode {
	return Binary(lhs, BinaryLess, rhs)
}

// Leq creates a binary node used to compare whether a value is less or equal to another
//
// **Parameters**
//   - lhs: left hand side operand
//   - rhs: right hand side operand
// **Returns**
//   - *BinaryNode: node to use in expression trees
func Leq(lhs interface{}, rhs interface{}) *BinaryNode {
	return Binary(lhs, BinaryLessEqual, rhs)
}

// Grt creates a binary node used to compare whether a value is greater than another
//
// **Parameters**
//   - lhs: left hand side operand
//   - rhs: right hand side operand
//
// **Returns**
//   - *BinaryNode: node to use in expression trees
func Grt(lhs interface{}, rhs interface{}) *BinaryNode {
	return Binary(lhs, BinaryGreater, rhs)
}

// Max get maximum value of a series of values
//
// **Parameters**
//   - param: expression which specifies values of which to get maximum
//
// **Returns**
//   - *FunctionNode: node to use in expression
func Max(param interface{}) *FunctionNode {
	return &FunctionNode{
		function:   FunctionMax,
		parameters: []interface{}{param}}
}

// Min get minimum value of a series of values
//
// **Parameters**
//   - param: expression which specifies values of which to get minimum
//
// **Returns**
//   - *FunctionNode: node to use in expression
func Min(param interface{}) *FunctionNode {
	return &FunctionNode{
		function:   FunctionMin,
		parameters: []interface{}{param}}
}

// Geq creates a binary node used to compare whether a value is greater or equal to another
//
// **Parameters**
//   - lhs: left hand side operand
//   - rhs: right hand side operand
//
// **Returns**
//   - *BinaryNode: node to use in expression trees
func Geq(lhs interface{}, rhs interface{}) *BinaryNode {
	return Binary(lhs, BinaryGreaterEqual, rhs)
}

// Binary creates a new binary node to be used in an expression tree
//
// **Parameters**
//   - lhs: left hand side operand
//   - rhs: right hand side operand
//
// **Returns**
//   - *BinaryNode: node to use in expression trees
func Binary(lhs interface{}, operator BinaryOperation, rhs interface{}) *BinaryNode {
	return &BinaryNode{
		lhs:      lhs,
		operator: operator,
		rhs:      rhs}
}

// AliasField specify a field of an entity using a table alias
func AliasField(alias string, model *models.EntityModel, fieldname string) *AliasNode {
	return &AliasNode{
		Alias: alias,
		Field: Field(model, fieldname)}
}

// AliasColumn specifies a column of a table using a table alias
func AliasColumn(alias string, columnname string) *AliasNode {
	return &AliasNode{
		Alias: alias,
		Field: &ColumnNode{
			Name: columnname}}
}

// Field - creates a new node representing an entity field
func Field(model *models.EntityModel, name string) *FieldNode {
	return &FieldNode{
		model: model,
		name:  name}
}

// Parameter - creates a new node representing a positional statement parameter
func Parameter() *ParameterNode {
	return &ParameterNode{}
}

// NamedParameter - creates a new node representing a named statement parameter
func NamedParameter(name string) *ParameterNode {
	return &ParameterNode{name: name}
}

// Count used to count number of rows returned
func Count() *FunctionNode {
	return count
}

// In checks for existence of an item in a collection
func In(item interface{}, collection ...interface{}) *InCollectionNode {
	return &InCollectionNode{
		item:       item,
		collection: collection}
}

// Average computes the average of a series of values
func Average(field interface{}) *FunctionNode {
	return &FunctionNode{
		function: FunctionAverage,
		parameters: []interface{}{
			field}}
}

// Statement includes a sub statement in an expression
func Statement(statement interfaces.IPreparedOperation) *StatementNode {
	return &StatementNode{
		Statement: statement}
}
