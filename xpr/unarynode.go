package xpr

// UnaryOperation - type of unary expression node
type UnaryOperation int

const (
	// Not - logical Not
	Not UnaryOperation = iota

	// Negate - negate connected node
	Negate

	// Complement - compute complement of node
	Complement
)

// UnaryNode - unary operator to a node
type UnaryNode struct {
	operator UnaryOperation
	value    interface{}
}

// Operator - type of unary operation
func (node *UnaryNode) Operator() UnaryOperation {
	return node.operator
}

// Value - operation value
func (node *UnaryNode) Value() interface{} {
	return node.value
}
