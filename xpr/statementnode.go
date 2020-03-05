package xpr

import "github.com/verticalgmbh/database-go/interfaces"

// StatementNode node used to include a statement in an expression
type StatementNode struct {
	Statement interfaces.IPreparedOperation
}
