package statements

import (
	"database/sql"
	"strings"

	"github.com/verticalgmbh/database-go/connection"
	"github.com/verticalgmbh/database-go/entities/models"
)

// AddUnique statement which adds a unique index to a table
type AddUnique struct {
	connection     *sql.DB
	connectioninfo connection.IConnectionInfo
	model          *models.EntityModel
	unique         *models.IndexDescriptor
}

// NewAddUnique creates a new AddUnique statement
//
// **Parameters**
//   - connection:     connection used to execute sql statement
//   - connectioninfo: driver specific connection info
//   - model:          model for which to create index
//   - unique:         unique index to add
//
// **Returns**
//   - *AddUnique: statement used to prepare operation
func NewAddUnique(connection *sql.DB, connectioninfo connection.IConnectionInfo, model *models.EntityModel, unique *models.IndexDescriptor) *AddUnique {
	return &AddUnique{
		connection:     connection,
		connectioninfo: connectioninfo,
		model:          model,
		unique:         unique}
}

func (statement *AddUnique) buildCommandText() string {
	var command strings.Builder

	command.WriteString("ALTER TABLE ")
	command.WriteString(statement.model.Table)
	command.WriteString(" ADD UNIQUE(")

	for index, column := range statement.unique.Columns() {
		if index > 0 {
			command.WriteRune(',')
		}

		command.WriteString(statement.connectioninfo.MaskColumn(column))
	}

	command.WriteString(")")

	return command.String()
}

// Prepare prepares the statement for execution
//
// **Returns**
//   - *PreparedStatement: operation used to execute statement
func (statement *AddUnique) Prepare() *PreparedStatement {
	return &PreparedStatement{
		connection: statement.connection,
		command:    statement.buildCommandText()}
}
