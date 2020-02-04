package connection

import (
	"database/sql"
	"fmt"
	"reflect"
	"regexp"
	"strings"
	"time"

	"nightlycode.de/database/entities/models"
	"nightlycode.de/database/xpr"
)

// SqliteInfo - sqlite specific information
type SqliteInfo struct {
}

// NewSqliteInfo creates a new sqlite info
//
// **Returns**
//   - *SqliteInfo: created sqlite connection info
func NewSqliteInfo() *SqliteInfo {
	return &SqliteInfo{}
}

// Parameter - literal used to specify parameters
func (info *SqliteInfo) Parameter() string {
	return "@"
}

// MaskColumn masks a column for use in an sql statement
//
// **Parameters**
//   - name: name of column to mask
//
// **Returns**
//   - string: masked column name
func (info *SqliteInfo) MaskColumn(name string) string {
	return fmt.Sprintf("[%s]", name)
}

// EvaluateFunction evaluates representation of a function in database
//
// **Parameters**
//   - function: function to evaluate
//   - command: command to write evaluation result to
func (info *SqliteInfo) EvaluateFunction(function *xpr.FunctionNode, command *strings.Builder) {
	switch function.Function() {
	case xpr.FunctionCount:
		command.WriteString("COUNT()")
	}
}

// ExistsTableOrView determines whether a table exists in database
//
// **Parameters**
//   - connection: connection to database
//   - name: name of table or view
//
// **Returns**
//   - bool: true if table or view exists, false otherwise
func (info *SqliteInfo) ExistsTableOrView(connection *sql.DB, name string) (bool, error) {
	rows, err := connection.Query("SELECT name FROM sqlite_master WHERE (type='table' OR type='view') AND name = @1", name)
	if err != nil {
		return false, err
	}

	defer rows.Close()

	for rows.Next() {
		return true, nil
	}

	return false, nil
}

// GetDatabaseType get type used in database
//
// **Parameters**
//   - type: application data type
//
// **Returns**
//   - string: database type name
func (info *SqliteInfo) GetDatabaseType(datatype reflect.Type) string {
	switch datatype.Kind() {
	case reflect.Bool:
		return "BOOLEAN"
	case reflect.String:
		return "TEXT"
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return "INTEGER"
	case reflect.Float32, reflect.Float64:
		return "FLOAT"
	case reflect.Slice, reflect.Array:
		return "BLOB"
	case reflect.Struct:
		if datatype == reflect.TypeOf(time.Time{}) {
			return "TIMESTAMP"
		}
		return "TEXT"
	default:
		return "TEXT"
	}
}

// CreateColumn creates sql text to use when creating a column
//
// **Parameters**
//   - column:  column to create
//   - command: command builder string
func (info *SqliteInfo) CreateColumn(column *models.ColumnDescriptor, command *strings.Builder) {
	command.WriteString(info.MaskColumn(column.Name()))
	command.WriteString(fmt.Sprintf(" %s", info.GetDatabaseType(column.DataType())))

	if column.IsPrimaryKey() {
		command.WriteRune(' ')
		command.WriteString("PRIMARY KEY")
	}

	if column.IsAutoIncrement() {
		command.WriteRune(' ')
		command.WriteString("AUTOINCREMENT")
	}

	if column.IsUnique() {
		command.WriteRune(' ')
		command.WriteString("UNIQUE")
	}

	if column.IsNotNull() {
		command.WriteRune(' ')
		command.WriteString("NOT NULL")
	}

	if column.DefaultValue() != "" {
		command.WriteString(" DEFAULT ")
		command.WriteString(column.DefaultValue())
	}
}

func (info *SqliteInfo) analyseColumnDefinition(definition string) (*models.ColumnDescriptor, error) {
	expression := regexp.MustCompile("^['\\[]?(?P<name>[^ '\\]]+)['\\]]?\\s+(?P<type>[^ ]+)(?P<pk> PRIMARY KEY)?(?P<ai> AUTOINCREMENT)?(?P<uq> UNIQUE)?(?P<nn> NOT NULL)?( DEFAULT '?(?P<default>.+)'?)?$")

	groups := expression.FindStringSubmatch(definition)
	if groups == nil {
		return nil, fmt.Errorf("Unable to analyse column definition '%s'", definition)
	}

	isprimarykey := false
	isautoincrement := false
	isunique := false
	isnotnull := false
	defaultvalue := ""
	for index, value := range groups {
		switch value {
		case " PRIMARY KEY":
			isprimarykey = true
		case " AUTOINCREMENT":
			isautoincrement = true
		case " UNIQUE":
			isunique = true
		case " NOT NULL":
			isnotnull = true
		default:
			if index > 2 {
				defaultvalue = value
			}
		}
	}

	return models.NewSchemaColumn(groups[1], groups[2], isprimarykey, isautoincrement, isunique, isnotnull, defaultvalue), nil
}

func (info *SqliteInfo) analyseUnique(definition string) []string {
	sql := strings.TrimSpace(definition)
	columnssql := strings.Split(sql, ",")

	var columns []string
	for _, column := range columnssql {
		columns = append(columns, strings.Trim(column, " '[]"))
	}

	return columns
}

func (info *SqliteInfo) analyseTableSQL(sql string) ([]*models.ColumnDescriptor, []*models.IndexDescriptor, error) {
	expression := regexp.MustCompile("^CREATE TABLE\\s+(?P<name>[^ ]+)\\s+\\((?P<columns>.+?)(\\s*,\\s*UNIQUE\\s*\\((?P<unique>.+?)\\))*\\s*\\)$")

	groups := expression.FindStringSubmatch(sql)
	if groups == nil {
		return nil, nil, fmt.Errorf("Unable to analyse table sql '%s'", sql)
	}

	columns := strings.Split(groups[2], ",")

	var tablecolumns []*models.ColumnDescriptor
	var tableuniques []*models.IndexDescriptor

	for _, column := range columns {
		columndesc := strings.TrimSpace(column)
		columndef, err := info.analyseColumnDefinition(columndesc)
		if err != nil {
			return nil, nil, err
		}
		tablecolumns = append(tablecolumns, columndef)
	}

	for i := 3; i < len(groups); i++ {
		uniquecolumns := info.analyseUnique(groups[i])
		tableuniques = append(tableuniques, models.NewIndexDescriptor("", uniquecolumns...))
	}

	return tablecolumns, tableuniques, nil
}

func (info *SqliteInfo) analyseIndexDefinitions(connection *sql.DB, tablename string) ([]*models.IndexDescriptor, error) {
	indexrows, err := connection.Query("SELECT sql FROM sqlite_master WHERE type='index' AND tbl_name=@1 AND sql IS NOT NULL", tablename)
	if err != nil {
		return nil, err
	}
	defer indexrows.Close()

	var indices []*models.IndexDescriptor

	expression := regexp.MustCompile("^CREATE INDEX idx_(?P<tablename>.+)_(?P<name>.+) ON (?P<table>.+) \\((?P<columns>.+)\\)$")
	var indexsql string
	for indexrows.Next() {
		err = indexrows.Scan(&indexsql)
		if err != nil {
			return nil, fmt.Errorf("Error matching index sql: %s", err.Error())
		}

		if indexsql == "" {
			continue
		}

		groups := expression.FindStringSubmatch(indexsql)
		if groups == nil {
			return nil, fmt.Errorf("Error matching index sql '%s'", indexsql)
		}

		columnssplit := strings.Split(groups[4], ",")
		var columns []string
		for _, column := range columnssplit {
			columns = append(columns, strings.TrimSpace(column))
		}

		indices = append(indices, models.NewIndexDescriptor(groups[2], columns...))
	}

	return indices, nil
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
func (info *SqliteInfo) GetSchema(connection *sql.DB, name string) (models.Schema, error) {
	row := connection.QueryRow("SELECT type, tbl_name, sql FROM sqlite_master WHERE name=@1", name)

	var typename string
	var tablename string
	var sql string

	err := row.Scan(&typename, &tablename, &sql)
	if err != nil {
		return nil, fmt.Errorf("Error scanning table data: %s", err.Error())
	}

	schema, err := info.toSchema(connection, typename, tablename, sql)
	if err != nil {
		return nil, fmt.Errorf("Error converting schema row: %s", err.Error())
	}

	return schema, nil
}

func (info *SqliteInfo) toSchema(connection *sql.DB, typename string, tablename string, sql string) (models.Schema, error) {
	switch typename {
	case "table":
		columns, uniques, err := info.analyseTableSQL(sql)
		if err != nil {
			return nil, err
		}

		indices, err := info.analyseIndexDefinitions(connection, tablename)
		if err != nil {
			return nil, err
		}

		table := models.NewTableDescriptor(tablename, columns, indices, uniques)
		return table, nil
	case "view":
		return &models.View{
			Name: tablename,
			SQL:  sql}, nil
	default:
		return nil, fmt.Errorf("Unsupported table type '%s'", typename)
	}
}

func (info *SqliteInfo) loadSchemas(connection *sql.DB) ([]*SchemaModel, error) {
	rows, err := connection.Query("SELECT type, tbl_name, sql FROM sqlite_master WHERE type = 'view' OR type = 'table'")
	if err != nil {
		return nil, fmt.Errorf("Unable to retrieve schema infos: %s", err.Error())
	}

	defer rows.Close()

	var schemas []*SchemaModel
	for rows.Next() {
		var typename string
		var tablename string
		var sql sql.NullString

		err := rows.Scan(&typename, &tablename, &sql)
		if err != nil {
			return nil, fmt.Errorf("Error scanning table data: %s", err.Error())
		}

		if !sql.Valid || strings.HasPrefix(tablename, "sqlite") {
			continue
		}

		schemas = append(schemas, &SchemaModel{
			SchemaType: typename,
			TableName:  tablename,
			SQL:        sql.String})
	}

	return schemas, nil
}

// GetSchemas get all schemas in database
//
// **Parameters**
//   - connection: connection of which to retrieve schematas
//
// **Returns**
//   - []Schema: schemas in database
//   - error   : errors if any occured
func (info *SqliteInfo) GetSchemas(connection *sql.DB) ([]models.Schema, error) {
	schemas, err := info.loadSchemas(connection)
	if err != nil {
		return nil, fmt.Errorf("Unable to load schema information: %s", err.Error())
	}

	var result []models.Schema

	for _, schemainfo := range schemas {
		schema, err := info.toSchema(connection, schemainfo.SchemaType, schemainfo.TableName, schemainfo.SQL)
		if err != nil {
			return nil, fmt.Errorf("Error creating schema: %s", err.Error())
		}

		result = append(result, schema)
	}

	return result, nil
}
