package connection

import (
	"database/sql"
	"fmt"
	"log"
	"reflect"
	"strings"

	"github.com/verticalgmbh/database-go/entities/models"
	"github.com/verticalgmbh/database-go/xpr"
)

// SQLServerInfo - ms sql server specific information
type SQLServerInfo struct {
}

// EvaluateParameter - literal used to specify parameters
//
// **Parameters**
//   - function: function to evaluate
//   - command: command to write evaluation result to
func (info *SQLServerInfo) EvaluateParameter(parameter *xpr.ParameterNode, command *strings.Builder) {
	if len(parameter.Name()) > 0 {
		command.WriteString("@")
		command.WriteString(parameter.Name())
	} else {
		command.WriteString("?")
	}
}

// MaskColumn masks a column for use in an sql statement
//
// **Parameters**
//   - name: name of column to mask
//
// **Returns**
//   - string: masked column name
func (info *SQLServerInfo) MaskColumn(name string) string {
	return fmt.Sprintf("[%s]", name)
}

// EvaluateFunction evaluates representation of a function in database
//
// **Parameters**
//   - function: function to evaluate
//   - command: command to write evaluation result to
func (info *SQLServerInfo) EvaluateFunction(function *xpr.FunctionNode, command *strings.Builder, eval func(interface{}) error) error {
	_, err := EvaluateFunction(function, command, eval)
	return err
}

// ExistsTableOrView determines whether a table exists in database
//
// **Parameters**
//   - connection: connection to database
//   - name: name of table or view
//
// **Returns**
//   - bool: true if table or view exists, false otherwise
func (info *SQLServerInfo) ExistsTableOrView(connection *sql.DB, name string) (bool, error) {
	log.Panicf("Not implemented")
	return false, nil
}

// GetDatabaseType get type used in database
//
// **Parameters**
//   - type: application data type
//
// **Returns**
//   - string: database type name
func (info *SQLServerInfo) GetDatabaseType(datatype reflect.Type) string {
	log.Panicf("Not implemented")
	return ""
}

// CreateColumn creates sql text to use when creating a column
//
// **Parameters**
//   - column:  column to create
//   - command: command builder string
func (info *SQLServerInfo) CreateColumn(column *models.ColumnDescriptor, command *strings.Builder) {
	log.Panicf("Not implemented")
}

// GetSchema get schema of a table or view in database
//
// **Parameters**
//   - connection: connection to database
//   - name: name of table or view
//
// **Returns**
//   - *Schema: schema information retrieved from database
//   - error: error information if any error occured
func (info *SQLServerInfo) GetSchema(connection *sql.DB, name string) (models.Schema, error) {
	log.Panicf("Not implemented")
	return nil, nil
}

// GetSchemas get all schemas in database
//
// **Parameters**
//   - connection: connection of which to retrieve schematas
//
// **Returns**
//   - []Schema: schemas in database
//   - error   : errors if any occured
func (info *SQLServerInfo) GetSchemas(connection *sql.DB) ([]models.Schema, error) {
	log.Panicf("Not implemented")
	return nil, nil
}
