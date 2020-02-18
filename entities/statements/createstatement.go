package statements

import (
	"database/sql"
	"strings"

	"github.com/verticalgmbh/database-go/connection"
	"github.com/verticalgmbh/database-go/entities/models"
)

// CreateStatement statement used to create tables for models in database
type CreateStatement struct {
	connection     *sql.DB
	connectioninfo connection.IConnectionInfo
	model          *models.EntityModel
}

// NewCreateStatement creates a new create statement
//
// **Parameters**
//   - model:          model representing entity for which to create a new table
//   - connection:     connection to database
//   - connectioninfo: driver specific connection info
// **Returns**
//   - CreateStatement: statement used to prepare create operation
func NewCreateStatement(model *models.EntityModel, connection *sql.DB, connectioninfo connection.IConnectionInfo) *CreateStatement {
	return &CreateStatement{
		connection:     connection,
		connectioninfo: connectioninfo,
		model:          model}
}

func (statement *CreateStatement) buildCommandText() string {
	var command strings.Builder

	command.WriteString("CREATE TABLE ")
	command.WriteString(statement.model.Table)
	command.WriteString(" (")

	for index, column := range statement.model.Columns() {
		if index > 0 {
			command.WriteRune(',')
		}

		statement.connectioninfo.CreateColumn(column, &command)
	}

	uniques := statement.model.Uniques()
	if len(uniques) > 0 {
		for _, unique := range uniques {
			command.WriteString(", UNIQUE(")
			for index, column := range unique.Columns() {
				if index > 0 {
					command.WriteRune(',')
				}
				command.WriteString(statement.connectioninfo.MaskColumn(column))
			}
			command.WriteRune(')')
		}
	}

	command.WriteString(")")

	return command.String()
}

// Prepare prepares the statement for execution
//
// **Returns**
//   - PreparedStatement: statement to use to create table
func (statement *CreateStatement) Prepare() *PreparedStatement {
	return &PreparedStatement{
		connection: statement.connection,
		command:    statement.buildCommandText()}
}
