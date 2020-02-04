package models

// View information about a view
type View struct {
	Name string
	SQL  string
}

// SchemaName name of view in database
//
// **Returns**
//   - string: name string
func (view *View) SchemaName() string {
	return view.Name
}

// Type type of schema
//
// **Returns**
//   - SchemaType: type of schema
func (view *View) Type() SchemaType {
	return SchemaTypeView
}
