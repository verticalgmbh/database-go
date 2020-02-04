package models

// SchemaType type of database schema
type SchemaType int

const (
	// SchemaTypeTable regular database table
	SchemaTypeTable SchemaType = iota

	// SchemaTypeView database view
	SchemaTypeView
)

// Schema schema of a structure in database
type Schema interface {
	SchemaName() string
	Type() SchemaType
}
