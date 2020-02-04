package statements

import (
	"database/sql"
	"strings"

	"nightlycode.de/database/connection"
	"nightlycode.de/database/entities/models"
	"nightlycode.de/database/entities/walkers"
)

// DeleteStatement statement used to delete entity data from the database
type DeleteStatement struct {
	connection     *sql.DB
	connectioninfo connection.IConnectionInfo
	model          *models.EntityModel
	where          interface{}
}

// NewDeleteStatement creates a statement used to delete entities from a database
//
// **Parameters**
//   - model: model of entity of which to delete rows
//   - connection: connection used to send sql statements
//   - connectioninfo: driver specific database information
//
// **Returns**
//   - DeleteStatement: statement to use to prepare operation
func NewDeleteStatement(model *models.EntityModel, connection *sql.DB, connectioninfo connection.IConnectionInfo) *DeleteStatement {
	return &DeleteStatement{
		connection:     connection,
		connectioninfo: connectioninfo,
		model:          model}
}

// Where adds a predicate used to filter for data to delete
//
// **Parameters**
//   - predicate: predicate expression specifying data to delete
//
// **Returns**
//   - DeleteStatement: current statement for fluent behavior
func (statement *DeleteStatement) Where(predicate interface{}) *DeleteStatement {
	statement.where = predicate
	return statement
}

func (statement *DeleteStatement) buildCommandText() string {
	var command strings.Builder

	command.WriteString("DELETE FROM ")
	command.WriteString(statement.model.Table)

	if statement.where != nil {
		command.WriteString(" WHERE ")

		sqlwalker := walkers.NewSqlWalker(statement.connectioninfo, &command)
		sqlwalker.Visit(statement.where)
	}

	return command.String()
}

// Prepare prepares the statement for execution
//
// **Returns**
//   - PreparedStatement: statement used to execute command
func (statement *DeleteStatement) Prepare() *PreparedStatement {
	return &PreparedStatement{
		connection: statement.connection,
		command:    statement.buildCommandText()}
}
