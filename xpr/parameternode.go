package xpr

// ParameterNode node representing a parameter in a statement
type ParameterNode struct {
	index int
}

// Index parameter index
func (node *ParameterNode) Index() int {
	return node.index
}
