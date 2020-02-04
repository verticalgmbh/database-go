package connection

import (
	"database/sql"
	"testing"

	"nightlycode.de/database/entities/models"

	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
)

func TestGetSchema(t *testing.T) {
	database, err := sql.Open("sqlite3", ":memory:")
	assert.NoError(t, err)

	defer database.Close()

	database.Exec("CREATE TABLE schemaentity (id INTEGER PRIMARY KEY AUTOINCREMENT, guid TEXT UNIQUE, firstname TEXT, lastname TEXT, firstsec TEXT, secondsec TEXT)")

	connectioninfo := SqliteInfo{}

	schema, err := connectioninfo.GetSchema(database, "schemaentity")
	assert.NoError(t, err)

	assert.Equal(t, "schemaentity", schema.SchemaName())
	assert.Equal(t, models.SchemaTypeTable, schema.Type())

	table := schema.(*models.Table)

	expectedcolumns := make(map[string]bool, 0)
	expectedcolumns["id"] = true
	expectedcolumns["guid"] = true
	expectedcolumns["firstname"] = true
	expectedcolumns["lastname"] = true
	expectedcolumns["firstsec"] = true
	expectedcolumns["secondsec"] = true

	for _, column := range table.Columns() {
		switch column.Name() {
		case "id":
			assert.Equal(t, "INTEGER", column.DBType())
			assert.True(t, column.IsPrimaryKey())
			assert.True(t, column.IsAutoIncrement())
		case "guid":
			assert.Equal(t, "TEXT", column.DBType())
			assert.True(t, column.IsUnique())
		case "firstname":
			assert.Equal(t, "TEXT", column.DBType())
		case "lastname":
			assert.Equal(t, "TEXT", column.DBType())
		case "firstsec":
			assert.Equal(t, "TEXT", column.DBType())
		case "secondsec":
			assert.Equal(t, "TEXT", column.DBType())
		default:
			assert.Failf(t, "Unexpected column '%s'", column.Name())
		}

		delete(expectedcolumns, column.Name())
	}

	assert.Equal(t, 0, len(expectedcolumns))
}
