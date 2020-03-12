package xpr

// FunctionType type of database function
type FunctionType int

const (
	// FunctionCount counts rows in a result set
	FunctionCount FunctionType = iota

	// FunctionRandom get a random number
	FunctionRandom

	// FunctionAverage average of a set of values
	FunctionAverage

	// FunctionSum of a set of values
	FunctionSum

	// FunctionMax maximum of a set of values
	FunctionMax

	// FunctionMin minimum of a set of values
	FunctionMin

	// FunctionCoalesce returns first value from list which is not null, null if all values are null
	FunctionCoalesce
)

// FunctionNode node in an expression tree representing a database function
type FunctionNode struct {
	function   FunctionType
	parameters []interface{}
}

// Function function type
func (node *FunctionNode) Function() FunctionType {
	return node.function
}

// Parameters parameters for function to call
func (node *FunctionNode) Parameters() []interface{} {
	return node.parameters
}
