package models

// Table information about a regular database table
type Table struct {
	name    string
	columns []*ColumnDescriptor
	indices []*IndexDescriptor
	uniques []*IndexDescriptor
}

// NewTableDescriptor creates a new Table
//
// **Parameters**
//   - name:    name of table
//   - columns: columns of table
//   - indices: index definitions of table
//   - uniques: unique descriptors of table
//
// **Returns**
//   - *Table: created table descriptor
func NewTableDescriptor(name string, columns []*ColumnDescriptor, indices []*IndexDescriptor, uniques []*IndexDescriptor) *Table {
	return &Table{
		name:    name,
		columns: columns,
		indices: indices,
		uniques: uniques}
}

// SchemaName name of table in database
//
// **Returns**
//   - string: name string
func (table *Table) SchemaName() string {
	return table.name
}

// Columns columns of table definition
//
// **Returns**
//   - []*ColumnDescriptor: columns contained in table definition
func (table *Table) Columns() []*ColumnDescriptor {
	return table.columns
}

// Indices index definitions of table definition
//
// **Returns**
//   - []*IndexDescriptor: index definitions contained in table definition
func (table *Table) Indices() []*IndexDescriptor {
	return table.indices
}

// Uniques unique index definitions of table definition
//
// **Returns**
//   - []*IndexDescriptor: unique index definitions contained in table definition
func (table *Table) Uniques() []*IndexDescriptor {
	return table.uniques
}

// Type type of schema
//
// **Returns**
//   - SchemaType: always returns SchemaTypeTable
func (table *Table) Type() SchemaType {
	return SchemaTypeTable
}
