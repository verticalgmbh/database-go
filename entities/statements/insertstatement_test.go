package statements

import (
	"database/sql"
	"reflect"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"github.com/verticalgmbh/database-go/connection"
	"github.com/verticalgmbh/database-go/entities/models"
)

type InsertModel struct {
	Something string
	SomeInt   int
	SomeFloat float32
}

func TestPlainInsert(t *testing.T) {
	database, _ := sql.Open("sqlite3", ":memory:")
	defer database.Close()

	database.Exec("CREATE TABLE insertmodel (something string, someint int, somefloat real)")

	model := models.CreateModel(reflect.TypeOf(InsertModel{}))
	statement := NewInsertStatement(model, database, &connection.SqliteInfo{})
	statement.Columns("Something", "SomeInt", "SomeFloat")

	operation := statement.Prepare()

	count, err := operation.Execute("Tralla", 42, 1.3)
	assert.NoError(t, err)
	assert.Equal(t, int64(1), count)

	loadstatement := NewLoadEntityStatement(model, database, &connection.SqliteInfo{})
	loadoperation := loadstatement.Prepare()
	result, err := loadoperation.Execute()

	assert.NoError(t, err)
	assert.Equal(t, 1, len(result))

	result1 := result[0].(*InsertModel)
	assert.Equal(t, "Tralla", result1.Something)
	assert.Equal(t, 42, result1.SomeInt)
	assert.Equal(t, float32(1.3), result1.SomeFloat)
}

/*func TestEntityInsert(t *testing.T) {
	database, _ := sql.Open("sqlite3", ":memory:")
	defer database.Close()

	dbconnection := connection.NewConnection(database)
	dbconnection.NonQuery("CREATE TABLE insertmodel (something string, someint int, somefloat real)")

	model := models.CreateModel(reflect.TypeOf(InsertModel{}))
	statement := NewInsertStatement(model, dbconnection, connection.SqliteInfo{})
	statement.FromModel(false)

	operation, err := statement.Prepare()

	count, err := operation.Execute("Tralla", 42, 1.3)
	assert.NoError(t, err)
	assert.Equal(t, int64(1), count)

	loadstatement := NewLoadEntityStatement(model, dbconnection, connection.SqliteInfo{})
	loadoperation := loadstatement.Prepare()
	result, err := loadoperation.Execute()

	assert.NoError(t, err)
	assert.Equal(t, 1, len(result))

	result1 := result[0].(*InsertModel)
	assert.Equal(t, "Tralla", result1.Something)
	assert.Equal(t, 42, result1.SomeInt)
	assert.Equal(t, float32(1.3), result1.SomeFloat)
}*/
