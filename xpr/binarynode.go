package xpr

type BinaryOperation int

const (
	BinaryAnd BinaryOperation = iota
	BinaryOr
	BinaryEquals
	BinaryNotEqual
	BinaryGreater
	BinaryGreaterEqual
	BinaryLess
	BinaryLessEqual
	BinaryAdd
	BinarySub
	BinaryDiv
	BinaryMul
	BinaryMod
	BinaryShl
	BinaryShr
	BinaryBitAnd
	BinaryBitOr
	BinaryBitXor
	BinaryAssign
	BinaryLike
)

// BinaryNode - node which combines two operands using an operator
type BinaryNode struct {
	operator BinaryOperation
	lhs      interface{}
	rhs      interface{}
}

func (node *BinaryNode) Operator() BinaryOperation {
	return node.operator
}

func (node *BinaryNode) Lhs() interface{} {
	return node.lhs
}

func (node *BinaryNode) Rhs() interface{} {
	return node.rhs
}
