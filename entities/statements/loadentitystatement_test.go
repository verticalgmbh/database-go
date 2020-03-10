package statements

import (
	"database/sql"
	"reflect"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/require"
	"github.com/verticalgmbh/database-go/connection"
	"github.com/verticalgmbh/database-go/entities/models"
	"github.com/verticalgmbh/database-go/xpr"
)

type ExampleModel struct {
	Something string
	SomeInt   int
	SomeFloat float32
}

func TestWhere(t *testing.T) {
	database, _ := sql.Open("sqlite3", ":memory:")
	defer database.Close()

	database.Exec("CREATE TABLE examplemodel (something string, someint int, somefloat real)")
	database.Exec("INSERT INTO examplemodel (something, someint, somefloat) VALUES ('hallo', 0, 0.5)")
	database.Exec("INSERT INTO examplemodel (something, someint, somefloat) VALUES ('hello', 2, 0.2)")
	database.Exec("INSERT INTO examplemodel (something, someint, somefloat) VALUES ('hillo', 1, 0.8)")
	database.Exec("INSERT INTO examplemodel (something, someint, somefloat) VALUES ('hullo', 5, 1.3)")
	database.Exec("INSERT INTO examplemodel (something, someint, somefloat) VALUES ('hollo', 4, 1.1)")

	model := models.CreateModel(reflect.TypeOf(ExampleModel{}))

	statement := NewLoadStatement(database, &connection.SqliteInfo{})
	statement.From(model)
	statement.Where(xpr.Les(xpr.Field(model, "SomeInt"), xpr.Parameter()))

	operation := statement.Prepare()

	result, err := operation.ExecuteEntity(4)

	require.NoError(t, err)
	require.Equal(t, 3, len(result))

	result1 := result[0].(*ExampleModel)
	result2 := result[1].(*ExampleModel)
	result3 := result[2].(*ExampleModel)

	require.Equal(t, "hallo", result1.Something)
	require.Equal(t, 0, result1.SomeInt)
	require.Equal(t, float32(0.5), result1.SomeFloat)

	require.Equal(t, "hello", result2.Something)
	require.Equal(t, 2, result2.SomeInt)
	require.Equal(t, float32(0.2), result2.SomeFloat)

	require.Equal(t, "hillo", result3.Something)
	require.Equal(t, 1, result3.SomeInt)
	require.Equal(t, float32(0.8), result3.SomeFloat)
}

func TestJoin(t *testing.T) {
	model := models.CreateModel(reflect.TypeOf(ExampleModel{}))
	statement := NewLoadStatement(nil, &connection.SqliteInfo{})
	statement.From(model)
	statement.Alias("t")
	statement.Where(xpr.Equals(xpr.AliasColumn("t", "test"), 10))
	statement.Join(JoinTypeInner, "differenttable", xpr.Equals(xpr.AliasColumn("dt", "key"), 8), "dt")

	prepared := statement.Prepare()
	require.Equal(t, "SELECT [something],[someint],[somefloat] FROM examplemodel AS t INNER JOIN differenttable AS dt ON dt.[key] = 8 WHERE t.[test] = 10", prepared.Command())
}
