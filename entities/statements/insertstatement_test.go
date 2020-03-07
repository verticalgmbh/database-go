package statements

import (
	"database/sql"
	"reflect"
	"testing"

	"github.com/verticalgmbh/database-go/xpr"

	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/require"
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
	require.NoError(t, err)
	require.Equal(t, int64(1), count)

	loadstatement := NewLoadEntityStatement(model, database, &connection.SqliteInfo{})
	loadoperation := loadstatement.Prepare()
	result, err := loadoperation.Execute()

	require.NoError(t, err)
	require.Equal(t, 1, len(result))

	result1 := result[0].(*InsertModel)
	require.Equal(t, "Tralla", result1.Something)
	require.Equal(t, 42, result1.SomeInt)
	require.Equal(t, float32(1.3), result1.SomeFloat)
}

func TestSubstatementInValuesInsert(t *testing.T) {
	// this is not supported by all databases
	// but sqlite supports it

	info := &connection.SqliteInfo{}
	database, _ := sql.Open("sqlite3", ":memory:")
	defer database.Close()

	_, err := database.Exec("CREATE TABLE insertmodel (something string, someint int, somefloat real)")
	require.NoError(t, err)
	_, err = database.Exec("INSERT INTO insertmodel (something, someint, somefloat) VALUES('test', 5, 0.0)")
	require.NoError(t, err)

	model := models.CreateModel(reflect.TypeOf(InsertModel{}))

	subload := NewLoadStatement(database, info).Table(model.Table)
	subload.Fields(xpr.Max(xpr.Field(model, "SomeInt")))
	preparedload := subload.Prepare()

	statement := NewInsertStatement(model, database, info)
	statement.Columns("Something", "SomeInt", "SomeFloat")
	statement.Values("Test", xpr.Add(xpr.Statement(preparedload), 9), 0.3)

	operation := statement.Prepare()

	count, err := operation.Execute()
	require.NoError(t, err)
	require.Equal(t, int64(1), count)

	loadstatement := NewLoadEntityStatement(model, database, &connection.SqliteInfo{})
	loadoperation := loadstatement.Prepare()
	result, err := loadoperation.Execute()

	require.NoError(t, err)
	require.Equal(t, 2, len(result))

	result1 := result[1].(*InsertModel)
	require.Equal(t, "Test", result1.Something)
	require.Equal(t, 14, result1.SomeInt)
	require.Equal(t, float32(0.3), result1.SomeFloat)
}

func TestSubstatementInsert(t *testing.T) {
	info := &connection.SqliteInfo{}
	database, _ := sql.Open("sqlite3", ":memory:")
	defer database.Close()

	_, err := database.Exec("CREATE TABLE insertmodel (something string, someint int, somefloat real)")
	require.NoError(t, err)
	_, err = database.Exec("INSERT INTO insertmodel (something, someint, somefloat) VALUES('test', 5, 0.0)")
	require.NoError(t, err)

	model := models.CreateModel(reflect.TypeOf(InsertModel{}))

	subload := NewLoadStatement(database, info).Table(model.Table)
	subload.Fields("Test", xpr.Add(xpr.Max(xpr.Field(model, "SomeInt")), 9), 0.3)
	preparedload := subload.Prepare()

	statement := NewInsertStatement(model, database, info)
	statement.Columns("Something", "SomeInt", "SomeFloat")
	statement.Values(preparedload)

	operation := statement.Prepare()

	count, err := operation.Execute()
	require.NoError(t, err)
	require.Equal(t, int64(1), count)

	loadstatement := NewLoadEntityStatement(model, database, &connection.SqliteInfo{})
	loadoperation := loadstatement.Prepare()
	result, err := loadoperation.Execute()

	require.NoError(t, err)
	require.Equal(t, 2, len(result))

	result1 := result[1].(*InsertModel)
	require.Equal(t, "Test", result1.Something)
	require.Equal(t, 14, result1.SomeInt)
	require.Equal(t, float32(0.3), result1.SomeFloat)
}

func TestReturnId(t *testing.T) {
	info := &connection.SqliteInfo{}
	database, _ := sql.Open("sqlite3", ":memory:")
	defer database.Close()

	_, err := database.Exec("CREATE TABLE insertmodel (something string, someint int, somefloat real)")
	require.NoError(t, err)
	_, err = database.Exec("INSERT INTO insertmodel (something, someint, somefloat) VALUES('test', 5, 0.0)")
	require.NoError(t, err)

	model := models.CreateModel(reflect.TypeOf(InsertModel{}))

	statement := NewInsertStatement(model, database, info)
	statement.Columns("Something", "SomeInt", "SomeFloat")
	statement.Values("Test", 1, 0.3)
	statement.ReturnID()

	id, err := statement.Prepare().Execute()
	require.NoError(t, err)
	require.Equal(t, int64(2), id)
}
