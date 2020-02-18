package statements

import (
	"database/sql"
	"strings"

	"github.com/verticalgmbh/database-go/connection"
	"github.com/verticalgmbh/database-go/entities/models"
)

// InsertLoad - statement used to insert data into a database table
type InsertLoad struct {
	connection     *sql.DB
	connectioninfo connection.IConnectionInfo
	model          *models.EntityModel
	columns        []string
	load           *LoadStatement
}

// NewInsertLoad - creates a new statement used to insert data to a database table
func NewInsertLoad(model *models.EntityModel, connection *sql.DB, connectioninfo connection.IConnectionInfo) *InsertLoad {
	return &InsertLoad{model: model, connection: connection, connectioninfo: connectioninfo}
}

// Fields specify fields to fill
//
// **Parameters**
//   - `fieldnames`: names of field to insert. Remember that you have to specify at least all fields which don't map to columns having an autoincrement or default value
//
// **Returns**
//   - InsertLoad: this statement for fluent behavior
func (statement *InsertLoad) Fields(fieldnames ...string) *InsertLoad {
	for _, field := range fieldnames {
		column := statement.model.ColumnFromField(field)
		statement.columns = append(statement.columns, column.Name())
	}

	return statement
}

// Columns specifies fields to fill from existing column descriptors
//
// **Parameters**
//   - columns: columns to fill
//
// **Returns**
//   - InsertLoad: this statement for fluent behavior
func (statement *InsertLoad) Columns(columns ...*models.ColumnDescriptor) *InsertLoad {
	for _, column := range columns {
		statement.columns = append(statement.columns, column.Name())
	}

	return statement
}

// Load specifies the load statement used to insert data into the table
//
// **Parameters**
//   - load: statement used to load data to insert
//
// **Returns**
//   - *InsertLoad: this statement for fluent behavior
func (statement *InsertLoad) Load(load *LoadStatement) *InsertLoad {
	statement.load = load
	return statement
}

// Prepare prepares the insert statement for execution
//
// **Returns**
// - `PreparedStatement`: Statement to execute
func (statement *InsertLoad) Prepare() *PreparedStatement {
	var command strings.Builder

	command.WriteString("INSERT INTO ")
	command.WriteString(statement.model.Table)
	command.WriteString(" (")

	for index, column := range statement.columns {
		if index > 0 {
			command.WriteRune(',')
		}

		command.WriteString(statement.connectioninfo.MaskColumn(column))
	}

	command.WriteString(") ")

	command.WriteString(statement.load.buildCommand())

	return &PreparedStatement{
		command:    command.String(),
		connection: statement.connection}
}
