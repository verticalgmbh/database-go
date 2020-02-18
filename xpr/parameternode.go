package xpr

// ParameterNode node representing a parameter in a statement
type ParameterNode struct {
	name string
}

// Name name or index of parameter
func (node *ParameterNode) Name() string {
	return node.name
}
