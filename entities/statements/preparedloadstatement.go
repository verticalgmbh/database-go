package statements

import (
	"database/sql"
	"errors"

	"github.com/verticalgmbh/database-go/connection"
)

// PreparedLoadStatement statement used to load data from the database
type PreparedLoadStatement struct {
	command        string
	connection     *sql.DB
	connectioninfo connection.IConnectionInfo
}

// Command sql command string sent to database
//
// **Returns**
//   - string: sql-command
func (statement *PreparedLoadStatement) Command() string {
	return statement.command
}

// Execute executes the statement and returns the result rows
//
// **Parameters**
//   - arguments: arguments used to fill statement parameters
//
// **Returns**
//   - Rows: result rows
//   - error: error if statement could not get executed
func (statement *PreparedLoadStatement) Execute(arguments ...interface{}) (*sql.Rows, error) {
	return statement.connection.Query(statement.command, arguments...)
}

// ExecuteSet executes the statement and returns a set of result values. This means the statement should return a set of rows with exactly one column
//
// **Parameters**
//   - arguments: arguments used to fill statement parameters
//
// **Returns**
//   - []interface{}: result set
//   - error: error if statement could not get executed
func (statement *PreparedLoadStatement) ExecuteSet(arguments ...interface{}) ([]interface{}, error) {
	rows, err := statement.connection.Query(statement.command, arguments...)

	if err != nil {
		return nil, err
	}

	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	if len(columns) > 1 {
		return nil, errors.New("The result rows contain more than one column. ExecuteSet is not the appropriate method to call in this case. You probably need to change the statement or call 'Execute'")
	}

	var result []interface{}

	for rows.Next() {
		var value interface{}
		err := rows.Scan(&value)
		if err != nil {
			return nil, err
		}

		result = append(result, value)
	}

	return result, nil
}

// ExecuteScalar executes the statement and returns one value as result. This means the statement should return exactly one column.
//               Multiple rows are supported however.
//
// **Parameters**
//   - arguments: arguments used to fill statement parameters
//
// **Returns**
//   - interface{}: result scalar
//   - error: error if statement could not get executed
func (statement *PreparedLoadStatement) ExecuteScalar(arguments ...interface{}) (interface{}, error) {
	rows, err := statement.connection.Query(statement.command, arguments...)

	if err != nil {
		return nil, err
	}

	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	if len(columns) > 1 {
		return nil, errors.New("The result rows contain more than one column. ExecuteScalar is not the appropriate method to call in this case. You probably need to change the statement or call 'Execute'")
	}

	for rows.Next() {
		var value interface{}
		err := rows.Scan(&value)
		if err != nil {
			return nil, err
		}

		return value, nil
	}

	return nil, errors.New("No result rows where returned by the statement")
}
