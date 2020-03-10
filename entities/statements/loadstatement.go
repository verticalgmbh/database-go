package statements

import (
	"database/sql"
	"strings"

	"github.com/verticalgmbh/database-go/xpr"

	"github.com/verticalgmbh/database-go/connection"
	"github.com/verticalgmbh/database-go/entities/models"
	"github.com/verticalgmbh/database-go/entities/walkers"
)

// LoadStatement statement used to load data from the database
type LoadStatement struct {
	connection     *sql.DB
	connectioninfo connection.IConnectionInfo
	from           interface{}

	fields  []interface{}
	groupby []interface{}
	where   interface{}

	union *union
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
func NewLoadStatement(connection *sql.DB, connectioninfo connection.IConnectionInfo) *LoadStatement {
	return &LoadStatement{
		connection:     connection,
		connectioninfo: connectioninfo}
}

// Table specifies a table to load data from
//
// **Parameters**
//   - table: table to load data from
//
// **Returns**
//   - LoadStatement: this statement for fluent behavior
func (statement *LoadStatement) Table(table string) *LoadStatement {
	statement.from = xpr.Table(table)
	return statement
}

// From - specifies a data set to load results from
//
// **Parameters**
//   - from: data to select result from
//
// **Returns**
//   - *LoadStatement: this statement for fluent behavior
func (statement *LoadStatement) From(from interface{}) *LoadStatement {
	statement.from = from
	return statement
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

// GroupBy set criterias for result grouping
//
// **Parameters**
//   - fields: expression which specifies fields to group by
//
// **Returns**
//   - *LoadStatement: this statement for fluent behavior
func (statement *LoadStatement) GroupBy(fields ...interface{}) *LoadStatement {
	statement.groupby = fields
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

	if statement.from != nil {
		command.WriteString(" FROM ")
		sqlwalker.Visit(statement.from)
	}

	if statement.where != nil {
		command.WriteString(" WHERE ")
		sqlwalker.Visit(statement.where)
	}

	if statement.groupby != nil {
		command.WriteString(" GROUP BY ")
		for index, field := range statement.groupby {
			if index > 0 {
				command.WriteRune(',')
			}

			sqlwalker.Visit(field)
		}
	}

	if statement.union != nil {
		command.WriteString(" UNION ")
		if statement.union.all {
			command.WriteString("ALL ")
		}

		command.WriteString(statement.union.statement.Command())
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
