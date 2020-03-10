package statements

import (
	"database/sql"
	"log"
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
	alias          string // alias to use for selection source

	model   *models.EntityModel // model to base select on
	fields  []interface{}
	groupby []interface{}
	where   interface{}

	joins []*join
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
	return statement.From(xpr.Table(table))
}

// Model - specifies a model to base SELECT on
//
// **Parameters**
//   - model: model for which to load data
//
// **Returns**
//   - LoadStatement: this statement for fluent behavior
func (statement *LoadStatement) Model(model *models.EntityModel) *LoadStatement {
	return statement.From(model)
}

// From - specifies a data set to load results from
//
// **Parameters**
//   - from: data to select result from
//
// **Returns**
//   - *LoadStatement: this statement for fluent behavior
func (statement *LoadStatement) From(from interface{}) *LoadStatement {
	if model, ok := from.(*models.EntityModel); ok {
		if statement.from != nil {
			log.Panicf("You can't specify a model source when a FROM source is already set")
		}
		statement.model = model
		statement.from = xpr.Table(model.Table)
	} else {
		if statement.model != nil {
			log.Panicf("You can't specify a FROM source when a model is already set")
		}

		statement.from = from
	}
	return statement
}

// Alias set an alias to use when loading from the table
//       mainly used to prevent conflicts with joined tables
func (statement *LoadStatement) Alias(alias string) *LoadStatement {
	statement.alias = alias
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
	if statement.model != nil {
		log.Panicf("You can't specify fields to load if model source is set.")
	}

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

// Join adds a join operation to apply to the load statement
func (statement *LoadStatement) Join(jointype JoinType, table string, predicate interface{}, alias string) *LoadStatement {
	statement.joins = append(statement.joins, &join{
		jointype:  jointype,
		table:     table,
		predicate: predicate,
		alias:     alias})
	return statement
}

// Union - concatenates another result set to a result
func (statement *LoadStatement) Union(load *PreparedLoadStatement, all bool) *LoadStatement {
	statement.union = &union{
		statement: load,
		all:       all}
	return statement
}

func (statement *LoadStatement) buildCommand() string {
	var command strings.Builder
	sqlwalker := walkers.NewSqlWalker(statement.connectioninfo, &command)

	command.WriteString("SELECT ")
	if statement.model != nil {
		for index, column := range statement.model.Columns() {
			if index > 0 {
				command.WriteRune(',')
			}

			command.WriteString(statement.connectioninfo.MaskColumn(column.Name()))
		}
	} else {
		for index, field := range statement.fields {
			if index > 0 {
				command.WriteRune(',')
			}

			sqlwalker.Visit(field)
		}
	}

	if statement.from != nil {
		command.WriteString(" FROM ")
		sqlwalker.Visit(statement.from)
	}

	if len(statement.alias) > 0 {
		command.WriteString(" AS ")
		command.WriteString(statement.alias)
	}

	if len(statement.joins) > 0 {
		for _, joinoperation := range statement.joins {
			switch joinoperation.jointype {
			case JoinTypeInner:
				command.WriteString(" INNER JOIN ")
			default:
				log.Panicf("Invalid join type %v", joinoperation.jointype)
			}

			command.WriteString(joinoperation.table)
			if len(joinoperation.alias) > 0 {
				command.WriteString(" AS ")
				command.WriteString(joinoperation.alias)
			}

			if joinoperation.predicate != nil {
				command.WriteString(" ON ")
				sqlwalker.Visit(joinoperation.predicate)
			}
		}
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
		connectioninfo: statement.connectioninfo,
		model:          statement.model}
}
