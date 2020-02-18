package statements

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/verticalgmbh/database-go/connection"
	"github.com/verticalgmbh/database-go/entities/models"
)

// CreateIndexStatement statement used to prepare an operation used to create an index
type CreateIndexStatement struct {
	connection     *sql.DB
	connectioninfo connection.IConnectionInfo
	model          *models.EntityModel
	index          *models.IndexDescriptor
}

// NewCreateIndexStatement creates a new CreateIndexStatement
//
// **Parameters**
//   - model:          model for which to create index
//   - index:          index to create
//   - connection:     connection used to execute sql statement
//   - connectioninfo: driver specific connection info
//
// **Returns**
//   -*CreateIndexStatement: statement used to prepare operation
func NewCreateIndexStatement(model *models.EntityModel, index *models.IndexDescriptor, connection *sql.DB, connectioninfo connection.IConnectionInfo) *CreateIndexStatement {
	return &CreateIndexStatement{
		model:          model,
		index:          index,
		connection:     connection,
		connectioninfo: connectioninfo}
}

func (statement *CreateIndexStatement) buildCommandText() string {
	var command strings.Builder

	indexname := fmt.Sprintf("idx_%s_%s", statement.model.Table, statement.index.Name())

	command.WriteString("DROP INDEX IF EXISTS ")
	command.WriteString(indexname)
	command.WriteString(";\n")

	command.WriteString("CREATE INDEX ")
	command.WriteString(indexname)
	command.WriteString(" ON ")
	command.WriteString(statement.model.Table)
	command.WriteString(" (")

	for index, column := range statement.index.Columns() {
		if index > 0 {
			command.WriteRune(',')
		}

		command.WriteString(statement.connectioninfo.MaskColumn(column))
	}
	command.WriteString(");")

	return command.String()
}

// Prepare prepares the statement for execution
//
// **Returns**
//   - *PreparedStatement: operation used to execute statement
func (statement *CreateIndexStatement) Prepare() *PreparedStatement {
	return &PreparedStatement{
		connection: statement.connection,
		command:    statement.buildCommandText()}
}
