package models

// IndexDescriptor database description of an index
type IndexDescriptor struct {
	name    string
	columns []string
}

// NewIndexDescriptor creates a new IndexDescriptor
func NewIndexDescriptor(name string, columns ...string) *IndexDescriptor {
	return &IndexDescriptor{
		name:    name,
		columns: columns}
}

// Name name of index
func (index *IndexDescriptor) Name() string {
	return index.name
}

// Columns database column referenced by index
func (index *IndexDescriptor) Columns() []string {
	return index.columns
}
