package statements

import (
	"database/sql"
	"strings"

	"github.com/verticalgmbh/database-go/connection"
)

// DropTable statement to remove a table in a database
type DropTable struct {
	connection     *sql.DB
	connectioninfo connection.IConnectionInfo
	name           string
}

// NewDropTable creates a new DropTable statement
//
// **Parameters**
//   - connection:     connection to use to execute statement
//   - connectioninfo: driver specific connection info
//   - name:           name of table to remove
//
// **Returns**
//   - *DropTable: statement to use to prepare operation
func NewDropTable(connection *sql.DB, connectioninfo connection.IConnectionInfo, name string) *DropTable {
	return &DropTable{
		connection:     connection,
		connectioninfo: connectioninfo,
		name:           name}
}

func (statement *DropTable) buildCommandText() string {
	var command strings.Builder

	command.WriteString("DROP TABLE ")
	command.WriteString(statement.name)

	return command.String()
}

// Prepare prepares the statement for execution
//
// **Returns**
//   - *PreparedStatement: prepared operation to execute
func (statement *DropTable) Prepare() *PreparedStatement {
	return &PreparedStatement{
		connection: statement.connection,
		command:    statement.buildCommandText()}
}
