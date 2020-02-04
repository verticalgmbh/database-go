package statements

import (
	"database/sql"
	"fmt"
)

// PreparedStatement - statement containing a prepared command to be executed
type PreparedStatement struct {
	command    string
	connection *sql.DB
}

// Command sql command string send to database
//
// **Returns**
//   - string: sql-command
func (statement *PreparedStatement) Command() string {
	return statement.command
}

// Execute executes the statement
//
// **Parameters**
//   - arguments: parameter values for statement
//
// **Returns**
//   - int64: number of affected rows
//   - error: error if any occured
func (statement *PreparedStatement) Execute(arguments ...interface{}) (int64, error) {
	return statement.ExecuteTransaction(nil, arguments...)
}

// ExecuteTransaction executes the statement using a transaction
//
// **Parameters**
//   - transaction: transaction used to execute statement
//   - arguments:   parameter values for statement
//
// **Returns**
//   - int64: number of affected rows
//   - error: error if any occured
func (statement *PreparedStatement) ExecuteTransaction(transaction *sql.Tx, arguments ...interface{}) (int64, error) {
	var result sql.Result
	var err error

	if transaction != nil {
		result, err = transaction.Exec(statement.command, arguments...)
	} else {
		result, err = statement.connection.Exec(statement.command, arguments...)
	}

	if err != nil {
		return 0, fmt.Errorf("Error executing '%s': %s", statement.command, err.Error())
	}

	affected, err := result.RowsAffected()
	if err != nil {
		return 0, err
	}

	return affected, nil
}
