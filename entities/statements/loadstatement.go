package statements

import (
	"database/sql"
	"strings"

	"nightlycode.de/database/connection"
	"nightlycode.de/database/entities/models"
	"nightlycode.de/database/entities/walkers"
)

// LoadStatement statement used to load data from the database
type LoadStatement struct {
	connection     *sql.DB
	connectioninfo connection.IConnectionInfo
	table          string

	fields []interface{}
	where  interface{}
}

// NewLoadStatement creates a new statement used to load data from the database
//
// **Parameters**
//   - model: model of entity of which to load data
//   - connection: connection used to send sql statements
//   - connectioninfo: driver specific database information
//
// **Returns**
//   - LoadStatement: statement to use to prepare operation
func NewLoadStatement(table string, connection *sql.DB, connectioninfo connection.IConnectionInfo) *LoadStatement {
	return &LoadStatement{
		connection:     connection,
		connectioninfo: connectioninfo,
		table:          table}
}

// Where set predicate for data to match
//
// **Parameters**
//   - predicate: predicate expression specifying filter
//
// **Returns**
//   - LoadStatement: this statement for fluent behavior
func (statement *LoadStatement) Where(predicate interface{}) *LoadStatement {
	statement.where = predicate
	return statement
}

// Columns specifies columns to load
//
// **Parameters**
//   - columns: columns to load in result set
//
// **Returns**
//   - LoadStatement: this statement for fluent behavior
func (statement *LoadStatement) Columns(columns []*models.ColumnDescriptor) *LoadStatement {
	for _, column := range columns {
		statement.fields = append(statement.fields, column)
	}
	return statement
}

// Fields set fields to load from database
//
// **Parameters**
//   - fields: field expressions for data to load
//
// **Returns**
//   - LoadStatement: this statement for fluent behavior
func (statement *LoadStatement) Fields(fields ...interface{}) *LoadStatement {
	statement.fields = fields
	return statement
}

func (statement *LoadStatement) buildCommand() string {
	var command strings.Builder
	sqlwalker := walkers.NewSqlWalker(statement.connectioninfo, &command)

	command.WriteString("SELECT ")
	for index, field := range statement.fields {
		if index > 0 {
			command.WriteRune(',')
		}

		sqlwalker.Visit(field)
	}

	command.WriteString(" FROM ")
	command.WriteString(statement.table)

	if statement.where != nil {
		command.WriteString(" WHERE ")
		sqlwalker.Visit(statement.where)
	}

	return command.String()
}

// Prepare prepares the load statement for execution
//
// **Returns**
//   - PreparedLoadStatement: statement to be used to load data
func (statement *LoadStatement) Prepare() *PreparedLoadStatement {
	return &PreparedLoadStatement{
		command:        statement.buildCommand(),
		connection:     statement.connection,
		connectioninfo: statement.connectioninfo}
}
