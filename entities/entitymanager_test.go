package entities

import (
	"database/sql"
	"reflect"
	"testing"

	"github.com/verticalgmbh/database-go/entities/models"
	"github.com/verticalgmbh/database-go/xpr"

	"github.com/stretchr/testify/assert"

	"github.com/verticalgmbh/database-go/connection"

	_ "github.com/mattn/go-sqlite3"
)

type CreateEntity struct {
	ID        int64  `database:"primarykey,autoincrement"`
	GUID      string `database:"unique"`
	Firstname string `database:"index=name"`
	Lastname  string `database:"index=name"`
	Firstsec  string `database:"unique=secret"`
	Secondsec string `database:"unique=secret"`
}

type TestEntity struct {
	Data    string
	Counter int
}

func TestExists(t *testing.T) {
	database, err := sql.Open("sqlite3", ":memory:")
	assert.NoError(t, err)

	defer database.Close()

	entitymanager := NewEntitymanager(database, connection.NewSqliteInfo())

	model := models.CreateModel(reflect.TypeOf(TestEntity{}))

	result, err := entitymanager.Exists(model)

	assert.NoError(t, err)
	assert.False(t, result)
}

func TestCreateEntity(t *testing.T) {
	database, err := sql.Open("sqlite3", ":memory:")
	assert.NoError(t, err)

	defer database.Close()

	entitymanager := NewEntitymanager(database, connection.NewSqliteInfo())

	model := models.CreateModel(reflect.TypeOf(TestEntity{}))

	err = entitymanager.Create(model)
	assert.NoError(t, err)

	result, err := entitymanager.Exists(model)

	assert.NoError(t, err)
	assert.True(t, result)
}

func TestUpdateEntityWithoutFilter(t *testing.T) {
	database, err := sql.Open("sqlite3", ":memory:")
	assert.NoError(t, err)

	defer database.Close()

	entitymanager := NewEntitymanager(database, connection.NewSqliteInfo())

	model := models.CreateModel(reflect.TypeOf(TestEntity{}))

	err = entitymanager.Create(model)
	assert.NoError(t, err)

	insert := entitymanager.Insert(model).Columns("Data", "Counter").Prepare()

	insert.Execute("Test1", 1)
	insert.Execute("Test2", 5)
	insert.Execute("Test3", 12)

	update := entitymanager.Update(model).Set(xpr.Assign(xpr.Field(model, "Counter"), xpr.Add(xpr.Field(model, "Counter"), 1))).Prepare()
	affected, err := update.Execute()
	assert.NoError(t, err)

	assert.Equal(t, int64(3), affected)

	result, err := entitymanager.LoadEntities(model).Prepare().Execute()
	assert.NoError(t, err)

	assert.Equal(t, 3, len(result))
	for _, entity := range result {
		castentity := entity.(*TestEntity)
		switch castentity.Data {
		case "Test1":
			assert.Equal(t, 2, castentity.Counter)
		case "Test2":
			assert.Equal(t, 6, castentity.Counter)
		case "Test3":
			assert.Equal(t, 13, castentity.Counter)
		default:
			assert.Fail(t, "Unexpected data")
		}
	}
}

func TestCreateTable(t *testing.T) {
	database, err := sql.Open("sqlite3", ":memory:")
	assert.NoError(t, err)

	defer database.Close()

	connectioninfo := connection.NewSqliteInfo()
	entitymanager := NewEntitymanager(database, connectioninfo)

	model := models.CreateModel(reflect.TypeOf(CreateEntity{}))

	err = entitymanager.Create(model)
	assert.NoError(t, err)

	schema, err := connectioninfo.GetSchema(database, model.Table)
	assert.NoError(t, err)

	assert.Equal(t, "createentity", schema.SchemaName())
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

	indices := model.Indices()
	assert.Equal(t, 1, len(indices))
	assert.Equal(t, 2, len(indices[0].Columns()))
	assert.Equal(t, "name", indices[0].Name())
	assert.Equal(t, "firstname", indices[0].Columns()[0])
	assert.Equal(t, "lastname", indices[0].Columns()[1])

	uniques := model.Uniques()
	assert.Equal(t, 1, len(uniques))
	assert.Equal(t, 2, len(uniques[0].Columns()))
	assert.Equal(t, "secret", uniques[0].Name())
	assert.Equal(t, "firstsec", uniques[0].Columns()[0])
	assert.Equal(t, "secondsec", uniques[0].Columns()[1])
}

func TestCreateUsingUpdateSchema(t *testing.T) {
	database, err := sql.Open("sqlite3", ":memory:")
	assert.NoError(t, err)

	defer database.Close()

	connectioninfo := connection.NewSqliteInfo()
	entitymanager := NewEntitymanager(database, connectioninfo)

	model := models.CreateModel(reflect.TypeOf(CreateEntity{}))

	err = entitymanager.UpdateSchema(model)
	assert.NoError(t, err)

	insert := entitymanager.Insert(model)
	insert.Columns("GUID", "Firstname", "Lastname", "Firstsec", "Secondsec")
	affected, err := insert.Prepare().Execute("bla", "Hans", "Mensch", "123", "asd")

	assert.NoError(t, err)
	assert.Equal(t, int64(1), affected)
}

func TestSchemaUpdateNewColumn(t *testing.T) {
	database, err := sql.Open("sqlite3", ":memory:")

	assert.NoError(t, err)
	connectioninfo := connection.NewSqliteInfo()

	defer database.Close()

	_, err = database.Exec("CREATE TABLE createentity (id INTEGER PRIMARY KEY AUTOINCREMENT, guid TEXT UNIQUE, firstname TEXT, lastname TEXT)")
	assert.NoError(t, err)

	entitymanager := NewEntitymanager(database, connectioninfo)

	model := models.CreateModel(reflect.TypeOf(CreateEntity{}))

	err = entitymanager.UpdateSchema(model)
	assert.NoError(t, err)

	insert := entitymanager.Insert(model)
	insert.Columns("GUID", "Firstname", "Lastname", "Firstsec", "Secondsec")
	affected, err := insert.Prepare().Execute("bla", "Hans", "Mensch", "123", "asd")

	assert.NoError(t, err)
	assert.Equal(t, int64(1), affected)
}
