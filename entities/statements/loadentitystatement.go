package statements

import (
	"database/sql"
	"log"
	"strings"

	"github.com/verticalgmbh/database-go/connection"
	"github.com/verticalgmbh/database-go/entities/models"
	"github.com/verticalgmbh/database-go/entities/walkers"
)

// ILoadEntityStatement - interface for statement used to load entities from database
type ILoadEntityStatement interface {

	// adds a where predicate to a load statement
	Where(predicate interface{}) ILoadEntityStatement

	// prepares the statement for execution
	Prepare() IPreparedLoadEntityStatement
}

// LoadEntityStatement - builds a load entity statement used to load entities from database
type LoadEntityStatement struct {
	connection     *sql.DB
	connectioninfo connection.IConnectionInfo
	model          *models.EntityModel

	alias string
	where interface{}

	joins []*join
}

// NewLoadEntityStatement - creates a new LoadEntityStatement
func NewLoadEntityStatement(model *models.EntityModel, connection *sql.DB, connectioninfo connection.IConnectionInfo) *LoadEntityStatement {
	return &LoadEntityStatement{
		model:          model,
		connection:     connection,
		connectioninfo: connectioninfo}
}

func (statement *LoadEntityStatement) buildCommandText() string {
	var command strings.Builder
	sqlwalker := walkers.NewSqlWalker(statement.connectioninfo, &command)

	command.WriteString("SELECT ")
	for index, column := range statement.model.Columns() {
		if index > 0 {
			command.WriteRune(',')
		}

		command.WriteString(statement.connectioninfo.MaskColumn(column.Name()))
	}

	command.WriteString(" FROM ")
	command.WriteString(statement.model.Table)

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

	return command.String()
}

// Alias set an alias to use when loading from the table
//       mainly used to prevent conflicts with joined tables
func (statement *LoadEntityStatement) Alias(alias string) ILoadEntityStatement {
	statement.alias = alias
	return statement
}

// Where - adds a where predicate to a load statement
func (statement *LoadEntityStatement) Where(predicate interface{}) ILoadEntityStatement {
	statement.where = predicate
	return statement
}

// Join adds a join operation to apply to the load statement
func (statement *LoadEntityStatement) Join(jointype JoinType, table string, predicate interface{}, alias string) {
	statement.joins = append(statement.joins, &join{
		jointype:  jointype,
		table:     table,
		predicate: predicate,
		alias:     alias})
}

// Prepare - prepares the statement for execution
func (statement *LoadEntityStatement) Prepare() IPreparedLoadEntityStatement {
	return &PreparedLoadEntityStatement{
		model:          statement.model,
		connection:     statement.connection,
		connectioninfo: statement.connectioninfo,
		command:        statement.buildCommandText()}
}
