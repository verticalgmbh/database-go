package connection

// SchemaModel model of a schema in database
type SchemaModel struct {
	SchemaType string `database:"column=type"`
	TableName  string `database:"column=tbl_name"`
	SQL        string
}
