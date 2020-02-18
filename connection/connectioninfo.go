package connection

import (
	"database/sql"
	"reflect"
	"strings"

	"github.com/verticalgmbh/database-go/entities/models"

	"github.com/verticalgmbh/database-go/xpr"
)

// IConnectionInfo - database driver specific information
type IConnectionInfo interface {

	// Parameter - literal used to specify parameters
	Parameter() string

	// MaskColumn masks a column for use in an sql statement
	//
	// **Parameters**
	//   - name: name of column to mask
	//
	// **Returns**
	//   - string: masked column name
	MaskColumn(name string) string

	// GetDatabaseType get type used in database
	//
	// **Parameters**
	//   - type: application data type
	//
	// **Returns**
	//   - string: database type name
	GetDatabaseType(datatype reflect.Type) string

	// EvaluateFunction evaluates representation of a function in database
	//
	// **Parameters**
	//   - function: function to evaluate
	//   - command: command to write evaluation result to
	EvaluateFunction(function *xpr.FunctionNode, command *strings.Builder)

	// ExistsTableOrView determines whether a table exists in database
	//
	// **Parameters**
	//   - connection: connection to database
	//   - name: name of table or view
	//
	// **Returns**
	//   - bool: true if table or view exists, false otherwise
	ExistsTableOrView(connection *sql.DB, name string) (bool, error)

	// CreateColumn creates sql text to use when creating a column
	//
	// **Parameters**
	//   - column:  column to create
	//   - command: command builder string
	CreateColumn(column *models.ColumnDescriptor, command *strings.Builder)

	// GetSchema get schema of a table or view in database
	//
	// **Parameters**
	//   - connection: connection to database
	//   - name: name of table or view
	//
	// **Returns**
	//   - *Schema: schema information retrieved from database
	//   - error: error information if any error occured
	GetSchema(connection *sql.DB, name string) (models.Schema, error)

	// GetSchemas get all schemas in database
	//
	// **Parameters**
	//   - connection: connection of which to retrieve schematas
	//
	// **Returns**
	//   - []Schema: schemas in database
	//   - error   : errors if any occured
	GetSchemas(connection *sql.DB) ([]models.Schema, error)
}
