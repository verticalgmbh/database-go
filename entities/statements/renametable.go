package statements

import (
	"database/sql"
	"strings"

	"nightlycode.de/database/connection"
)

// RenameTable statement used to rename a table in a database
type RenameTable struct {
	connection     *sql.DB
	connectioninfo connection.IConnectionInfo
	oldname        string
	newname        string
}

// NewRenameTable creates a new RenameTable statement
//
// **Parameters**
//   - connection:     connection to use to execute statement
//   - connectioninfo: driver specific connection info
//   - oldname:        current name of table
//   - newname:        name to rename table to
//
// **Returns**
//   - *RenameTable: statement to use to prepare operation
func NewRenameTable(connection *sql.DB, connectioninfo connection.IConnectionInfo, oldname string, newname string) *RenameTable {
	return &RenameTable{
		connection:     connection,
		connectioninfo: connectioninfo,
		oldname:        oldname,
		newname:        newname}
}

func (statement *RenameTable) buildCommandText() string {
	var command strings.Builder

	command.WriteString("ALTER TABLE ")
	command.WriteString(statement.oldname)
	command.WriteString(" RENAME TO ")
	command.WriteString(statement.newname)

	return command.String()
}

// Prepare prepares the statement for execution
//
// **Returns**
//   - *PreparedStatement: prepared operation to execute
func (statement *RenameTable) Prepare() *PreparedStatement {
	return &PreparedStatement{
		connection: statement.connection,
		command:    statement.buildCommandText()}
}
