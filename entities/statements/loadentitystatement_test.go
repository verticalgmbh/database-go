package statements

import (
	"database/sql"
	"reflect"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
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

	statement := NewLoadEntityStatement(model, database, &connection.SqliteInfo{})
	statement.Where(xpr.Les(xpr.Field(model, "SomeInt"), xpr.Parameter(1)))

	operation := statement.Prepare()

	result, err := operation.Execute(4)

	assert.NoError(t, err)
	assert.Equal(t, 3, len(result))

	result1 := result[0].(*ExampleModel)
	result2 := result[1].(*ExampleModel)
	result3 := result[2].(*ExampleModel)

	assert.Equal(t, "hallo", result1.Something)
	assert.Equal(t, 0, result1.SomeInt)
	assert.Equal(t, float32(0.5), result1.SomeFloat)

	assert.Equal(t, "hello", result2.Something)
	assert.Equal(t, 2, result2.SomeInt)
	assert.Equal(t, float32(0.2), result2.SomeFloat)

	assert.Equal(t, "hillo", result3.Something)
	assert.Equal(t, 1, result3.SomeInt)
	assert.Equal(t, float32(0.8), result3.SomeFloat)
}
