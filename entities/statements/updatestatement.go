package statements

import (
	"database/sql"
	"strings"

	"nightlycode.de/database/connection"
	"nightlycode.de/database/entities/models"
	"nightlycode.de/database/entities/walkers"
)

// UpdateStatement statement used to update data in a database
type UpdateStatement struct {
	connection     *sql.DB
	connectioninfo connection.IConnectionInfo
	model          *models.EntityModel

	updates []interface{}
	where   interface{}
}

// NewUpdateStatement creates a statement used to update entities of a database
//
// **Parameters**
//   - model: model of entity of which to update rows
//   - connection: connection used to send sql statements
//   - connectioninfo: driver specific database information
//
// **Returns**
//   - UpdateStatement: statement to use to prepare operation
func NewUpdateStatement(model *models.EntityModel, connection *sql.DB, connectioninfo connection.IConnectionInfo) *UpdateStatement {
	return &UpdateStatement{
		connection:     connection,
		connectioninfo: connectioninfo,
		model:          model}
}

// Where adds a predicate used to filter for data to update
//
// **Parameters**
//   - predicate: predicate expression specifying filter for data to update
//
// **Returns**
//   - UpdateStatement: current statement for fluent behavior
func (statement *UpdateStatement) Where(predicate interface{}) *UpdateStatement {
	statement.where = predicate
	return statement
}

// Set specifies update operations for statement
//
// **Parameters**
//   - operations: expressions specifying update operations
//
// **Returns**
//   - UpdateStatement: current statement for fluent behavior
func (statement *UpdateStatement) Set(operations ...interface{}) *UpdateStatement {
	statement.updates = operations
	return statement
}

func (statement *UpdateStatement) buildCommandText() string {
	var command strings.Builder
	sqlwalker := walkers.NewSqlWalker(statement.connectioninfo, &command)

	command.WriteString("UPDATE ")
	command.WriteString(statement.model.Table)

	command.WriteString(" SET ")
	for index, operation := range statement.updates {
		if index > 0 {
			command.WriteRune(',')
		}
		sqlwalker.Visit(operation)
	}

	if statement.where != nil {
		command.WriteString(" WHERE ")
		sqlwalker.Visit(statement.where)
	}

	return command.String()
}

// Prepare prepares the statement for execution
//
// **Returns**
//   - PreparedStatement: statement used to execute command
func (statement *UpdateStatement) Prepare() *PreparedStatement {
	return &PreparedStatement{
		connection: statement.connection,
		command:    statement.buildCommandText()}
}
