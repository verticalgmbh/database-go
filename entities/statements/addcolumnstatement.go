package statements

import (
	"database/sql"
	"strings"

	"github.com/verticalgmbh/database-go/connection"
	"github.com/verticalgmbh/database-go/entities/models"
)

// AddColumnStatement statement used to add a column to a table
type AddColumnStatement struct {
	connection     *sql.DB
	connectioninfo connection.IConnectionInfo
	model          *models.EntityModel
	column         *models.ColumnDescriptor
}

// NewAddColumnStatement creates a new AddColumnStatement
//
// **Parameters**
//   - connection:     connection used to execute sql statement
//   - connectioninfo: driver specific connection info
//   - model:          model for which to create index
//   - column:         column to add to the table
//
// **Returns**
//   - *AddColumnStatement: statement used to prepare operation
func NewAddColumnStatement(connection *sql.DB, connectioninfo connection.IConnectionInfo, model *models.EntityModel, column *models.ColumnDescriptor) *AddColumnStatement {
	return &AddColumnStatement{
		connection:     connection,
		connectioninfo: connectioninfo,
		model:          model,
		column:         column}
}

func (statement *AddColumnStatement) buildCommandText() string {
	var command strings.Builder

	command.WriteString("ALTER TABLE ")
	command.WriteString(statement.model.Table)
	command.WriteString(" ADD COLUMN ")
	statement.connectioninfo.CreateColumn(statement.column, &command)

	return command.String()
}

// Prepare prepares the statement for execution
//
// **Returns**
//   - *PreparedStatement: operation used to execute statement
func (statement *AddColumnStatement) Prepare() *PreparedStatement {
	return &PreparedStatement{
		connection: statement.connection,
		command:    statement.buildCommandText()}
}
