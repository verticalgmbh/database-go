package statements

// JoinType specified the type of join to apply
type JoinType int8

const (
	// JoinTypeInner specifies an INNER JOIN operation
	JoinTypeInner JoinType = iota
)

// Join applies a join to a load operation
type join struct {
	jointype  JoinType
	table     string
	alias     string
	predicate interface{}
}
