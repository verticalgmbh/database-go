package xpr

// AliasNode node used to specify a field or column using an table alias reference
type AliasNode struct {
	Alias string
	Field interface{}
}
