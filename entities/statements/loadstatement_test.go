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

type LoadModel struct {
	Something string
	SomeInt   int
	SomeFloat float32
}

func TestLoadDataCount(t *testing.T) {
	database, _ := sql.Open("sqlite3", ":memory:")
	defer database.Close()

	database.Exec("CREATE TABLE loadmodel (something string, someint int, somefloat real)")
	database.Exec("INSERT INTO loadmodel (something, someint, somefloat) VALUES ('hallo', 0, 0.5)")
	database.Exec("INSERT INTO loadmodel (something, someint, somefloat) VALUES ('hello', 2, 0.2)")
	database.Exec("INSERT INTO loadmodel (something, someint, somefloat) VALUES ('hillo', 1, 0.8)")
	database.Exec("INSERT INTO loadmodel (something, someint, somefloat) VALUES ('hullo', 5, 1.3)")
	database.Exec("INSERT INTO loadmodel (something, someint, somefloat) VALUES ('hollo', 4, 1.1)")

	model := models.CreateModel(reflect.TypeOf(LoadModel{}))

	statement := NewLoadStatement(model.Table, database, &connection.SqliteInfo{})
	statement.Fields(xpr.Count())
	statement.Where(xpr.Les(xpr.Field(model, "SomeInt"), xpr.Parameter()))

	operation := statement.Prepare()

	result, err := operation.ExecuteScalar(4)

	assert.NoError(t, err)
	count := result.(int64)

	assert.Equal(t, int64(3), count)
}
