package statements

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/verticalgmbh/database-go/connection"
	"github.com/verticalgmbh/database-go/entities/models"
)

// InsertStatement - statement used to insert data into a database table
type InsertStatement struct {
	connection     *sql.DB
	connectioninfo connection.IConnectionInfo
	model          *models.EntityModel
	fields         []string
	statement      *LoadStatement
}

// NewInsertStatement - creates a new statement used to insert data to a database table
func NewInsertStatement(model *models.EntityModel, connection *sql.DB, connectioninfo connection.IConnectionInfo) *InsertStatement {
	return &InsertStatement{model: model, connection: connection, connectioninfo: connectioninfo}
}

// Columns specify columns to fill
//
// **Parameters**
//   - `fieldnames`: names of field to insert. Remember that you have to specify at least all fields which don't map to columns having an autoincrement or default value
//
// **Returns**
//   - `InsertStatement`: this statement for fluent behavior
func (statement *InsertStatement) Columns(fieldnames ...string) *InsertStatement {
	statement.fields = fieldnames
	return statement
}

// Prepare prepares the insert statement for execution
//
// **Returns**
// - `PreparedStatement`: Statement to execute
func (statement *InsertStatement) Prepare() *PreparedStatement {
	var command strings.Builder

	command.WriteString("INSERT INTO ")
	command.WriteString(statement.model.Table)
	command.WriteString(" (")

	for index, field := range statement.fields {
		if index > 0 {
			command.WriteRune(',')
		}

		column := statement.model.ColumnFromField(field)
		if column == nil {
			panic("Entity field does not exist")
		}
		command.WriteString(column.Name())
	}

	command.WriteString(") VALUES(")
	for index := range statement.fields {
		if index > 0 {
			command.WriteRune(',')
		}
		command.WriteString(statement.connectioninfo.Parameter())
		command.WriteString(fmt.Sprintf("%d", index+1))
	}

	command.WriteRune(')')

	return &PreparedStatement{
		command:    command.String(),
		connection: statement.connection}
}
