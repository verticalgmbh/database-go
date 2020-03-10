package statements

import (
	"database/sql"
	"errors"
	"log"
	"reflect"

	"github.com/verticalgmbh/database-go/entities/models"

	"github.com/verticalgmbh/database-go/connection"
)

// PreparedLoadStatement statement used to load data from the database
type PreparedLoadStatement struct {
	command        string
	connection     *sql.DB
	connectioninfo connection.IConnectionInfo
	model          *models.EntityModel // model on which select was based on
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

// ExecuteTransaction executes the statement and returns the result rows
//
// **Parameters**
//   - arguments: arguments used to fill statement parameters
//
// **Returns**
//   - Rows: result rows
//   - error: error if statement could not get executed
func (statement *PreparedLoadStatement) ExecuteTransaction(transaction *sql.Tx, arguments ...interface{}) (*sql.Rows, error) {
	return transaction.Query(statement.command, arguments...)
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
	return statement.ExecuteSetTransaction(nil, arguments...)
}

// ExecuteSetTransaction executes the statement and returns a set of result values. This means the statement should return a set of rows with exactly one column
//
// **Parameters**
//   - arguments: arguments used to fill statement parameters
//
// **Returns**
//   - []interface{}: result set
//   - error: error if statement could not get executed
func (statement *PreparedLoadStatement) ExecuteSetTransaction(transaction *sql.Tx, arguments ...interface{}) ([]interface{}, error) {
	var rows *sql.Rows
	var err error

	if transaction != nil {
		rows, err = transaction.Query(statement.command, arguments...)
	} else {
		rows, err = statement.connection.Query(statement.command, arguments...)
	}

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
	return statement.ExecuteScalarTransaction(nil, arguments...)
}

// ExecuteScalarTransaction executes the statement and returns one value as result. This means the statement should return exactly one column.
//               Multiple rows are supported however.
//
// **Parameters**
//   - arguments: arguments used to fill statement parameters
//
// **Returns**
//   - interface{}: result scalar
//   - error: error if statement could not get executed
func (statement *PreparedLoadStatement) ExecuteScalarTransaction(transaction *sql.Tx, arguments ...interface{}) (interface{}, error) {
	var rows *sql.Rows
	var err error

	if transaction != nil {
		rows, err = transaction.Query(statement.command, arguments...)
	} else {
		rows, err = statement.connection.Query(statement.command, arguments...)
	}

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

// ExecuteEntity - loads matching entity data from database
func (statement *PreparedLoadStatement) ExecuteEntity(arguments ...interface{}) ([]interface{}, error) {
	return statement.ExecuteEntityTransaction(nil, arguments...)
}

// ExecuteEntityTransaction - loads matching entity data from database
func (statement *PreparedLoadStatement) ExecuteEntityTransaction(transaction *sql.Tx, arguments ...interface{}) ([]interface{}, error) {
	return statement.ExecuteMappedEntityTransaction(transaction, statement.model, arguments...)
}

// ExecuteMappedEntity - loads matching entity data from database
func (statement *PreparedLoadStatement) ExecuteMappedEntity(model *models.EntityModel, arguments ...interface{}) ([]interface{}, error) {
	return statement.ExecuteMappedEntityTransaction(nil, model, arguments...)
}

// ExecuteMappedEntityTransaction - loads matching entity data from database
func (statement *PreparedLoadStatement) ExecuteMappedEntityTransaction(transaction *sql.Tx, model *models.EntityModel, arguments ...interface{}) ([]interface{}, error) {
	var rows *sql.Rows
	var err error

	if transaction != nil {
		rows, err = transaction.Query(statement.command, arguments...)
	} else {
		rows, err = statement.connection.Query(statement.command, arguments...)
	}

	if err != nil {
		return nil, err
	}

	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	var values []interface{} = make([]interface{}, len(columns))
	var entities []interface{}
	for rows.Next() {
		entity := reflect.New(model.EntityType())

		for index, column := range columns {
			columndescription := statement.model.Column(column)
			entityfield := entity.Elem().FieldByName(columndescription.Field())
			if entityfield.IsValid() && entityfield.CanSet() {
				values[index] = entityfield.Addr().Interface()
			}
		}

		err := rows.Scan(values...)
		if err != nil {
			log.Printf("ERR: %s", err.Error())
			continue
		}

		entities = append(entities, entity.Interface())
	}

	return entities, nil
}
