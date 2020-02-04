package statements

import (
	"database/sql"
	"strings"

	"nightlycode.de/database/connection"
	"nightlycode.de/database/entities/models"
	"nightlycode.de/database/entities/walkers"
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

	where interface{}
}

// NewLoadEntityStatement - creates a new LoadEntityStatement
func NewLoadEntityStatement(model *models.EntityModel, connection *sql.DB, connectioninfo connection.IConnectionInfo) *LoadEntityStatement {
	return &LoadEntityStatement{
		model:          model,
		connection:     connection,
		connectioninfo: connectioninfo}
}

func (statement LoadEntityStatement) buildCommandText() string {
	var command strings.Builder

	command.WriteString("SELECT ")
	for index, column := range statement.model.Columns() {
		if index > 0 {
			command.WriteRune(',')
		}

		command.WriteString(statement.connectioninfo.MaskColumn(column.Name()))
	}

	command.WriteString(" FROM ")
	command.WriteString(statement.model.Table)

	if statement.where != nil {
		command.WriteString(" WHERE ")

		sqlwalker := walkers.NewSqlWalker(statement.connectioninfo, &command)
		sqlwalker.Visit(statement.where)
	}

	return command.String()
}

// Where - adds a where predicate to a load statement
func (statement *LoadEntityStatement) Where(predicate interface{}) ILoadEntityStatement {
	statement.where = predicate
	return statement
}

// Prepare - prepares the statement for execution
func (statement *LoadEntityStatement) Prepare() IPreparedLoadEntityStatement {
	return PreparedLoadEntityStatement{
		model:          statement.model,
		connection:     statement.connection,
		connectioninfo: statement.connectioninfo,
		command:        statement.buildCommandText()}
}
