package statements

import (
	"database/sql"
	"strings"

	"github.com/verticalgmbh/database-go/connection"
	"github.com/verticalgmbh/database-go/entities/models"
)

// DropIndex statement which removes an index from the database
type DropIndex struct {
	connection     *sql.DB
	connectioninfo connection.IConnectionInfo
	model          *models.EntityModel
	name           string
}

// NewDropIndex creates a new DropIndex statement
//
// **Parameters**
//   - connection:     connection used to execute sql statement
//   - connectioninfo: driver specific connection info
//   - model:          model for which to create index
//   - name:           name of index to drop
//
// **Returns**
//   - *DropIndex: statement used to prepare operation
func NewDropIndex(connection *sql.DB, connectioninfo connection.IConnectionInfo, model *models.EntityModel, name string) *DropIndex {
	return &DropIndex{
		connection:     connection,
		connectioninfo: connectioninfo,
		model:          model,
		name:           name}
}

func (statement *DropIndex) buildCommandText() string {
	var command strings.Builder

	command.WriteString("DROP INDEX ")
	command.WriteString(statement.name)

	return command.String()
}

// Prepare prepares the statement for execution
//
// **Returns**
//   - *PreparedStatement: operation used to execute statement
func (statement *DropIndex) Prepare() *PreparedStatement {
	return &PreparedStatement{
		connection: statement.connection,
		command:    statement.buildCommandText()}
}
