package models

import (
	"reflect"
)

// ColumnDescriptor - metadata of a column in an entity model
type ColumnDescriptor struct {
	name            string
	isprimarykey    bool
	isunique        bool
	isautoincrement bool
	isnotnull       bool
	defaultvalue    string

	field    string
	datatype reflect.Type

	dbtype string
}

// NewSchemaColumn creates a new SchemaColumn
//
// **Parameters**
//   - name:            name of column
//   - dbtype:          database type of column
//   - isprimarykey:    determines whether column in a primary key
//   - isautoincrement: determines whether column has auto increment flag
//   - isunique:        determines whether values of column have to be unique
//   - isnotnull:       determines whether values of column are not allowed to contain null values
//   - defaultvalue:    default value of column if no value is specified in insert statement
//
// **Returns**
//   - *SchemaColumn: created column information
func NewSchemaColumn(name string, dbtype string, isprimarykey bool, isautoincrement bool, isunique bool, isnotnull bool, defaultvalue string) *ColumnDescriptor {
	return &ColumnDescriptor{
		name:            name,
		dbtype:          dbtype,
		isprimarykey:    isprimarykey,
		isautoincrement: isautoincrement,
		isunique:        isunique,
		isnotnull:       isnotnull,
		defaultvalue:    defaultvalue}
}

func (column *ColumnDescriptor) Name() string {
	return column.name
}

func (column *ColumnDescriptor) Field() string {
	return column.field
}

func (column *ColumnDescriptor) DBType() string {
	return column.dbtype
}

func (column *ColumnDescriptor) DataType() reflect.Type {
	return column.datatype
}

func (column *ColumnDescriptor) DefaultValue() string {
	return column.defaultvalue
}

func (column *ColumnDescriptor) IsPrimaryKey() bool {
	return column.isprimarykey
}

func (column *ColumnDescriptor) IsUnique() bool {
	return column.isunique
}

func (column *ColumnDescriptor) IsAutoIncrement() bool {
	return column.isautoincrement
}

func (column *ColumnDescriptor) IsNotNull() bool {
	return column.isnotnull
}

// HasDefault determines whether column has a default value
//
// **Returns**
//   - bool: true when column has a default value, false otherwise
func (column *ColumnDescriptor) HasDefault() bool {
	return column.defaultvalue != ""
}
