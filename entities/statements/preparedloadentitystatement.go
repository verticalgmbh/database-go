package statements

import (
	"database/sql"
	"log"
	"reflect"

	"github.com/verticalgmbh/database-go/connection"

	"github.com/verticalgmbh/database-go/entities/models"
)

// IPreparedLoadEntityStatement - interface for a prepared statement used to load entities from a database
type IPreparedLoadEntityStatement interface {

	// Command sql command string sent to database
	//
	// **Returns**
	//   - string: sql-command
	Command() string

	// loads the data from database
	Execute(arguments ...interface{}) ([]interface{}, error)
}

// PreparedLoadEntityStatement - statement containing a prepared command string to be executed
type PreparedLoadEntityStatement struct {
	command        string
	connection     *sql.DB
	connectioninfo connection.IConnectionInfo
	model          *models.EntityModel
}

// Command sql command string sent to database
//
// **Returns**
//   - string: sql-command
func (statement *PreparedLoadEntityStatement) Command() string {
	return statement.command
}

// Execute - loads matching entity data from database
func (statement *PreparedLoadEntityStatement) Execute(arguments ...interface{}) ([]interface{}, error) {
	rows, err := statement.connection.Query(statement.command, arguments...)

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
		entity := reflect.New(statement.model.EntityType())

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
