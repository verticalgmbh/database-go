package statements

import (
	"database/sql"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/verticalgmbh/database-go/connection"
	"github.com/verticalgmbh/database-go/entities/models"
	"github.com/verticalgmbh/database-go/xpr"
)

type DeleteModel struct {
	Id        int64
	Something string
}

func Test_DeleteSpecificEntity(t *testing.T) {
	database, _ := sql.Open("sqlite3", ":memory:")
	defer database.Close()

	_, err := database.Exec("CREATE TABLE deletemodel (id INTEGER PRIMARY KEY AUTOINCREMENT, something text)")
	require.NoError(t, err)

	_, err = database.Exec("INSERT INTO deletemodel (something) VALUES('test')")
	require.NoError(t, err)
	_, err = database.Exec("INSERT INTO deletemodel (something) VALUES('tost')")
	require.NoError(t, err)
	_, err = database.Exec("INSERT INTO deletemodel (something) VALUES('tust')")
	require.NoError(t, err)
	_, err = database.Exec("INSERT INTO deletemodel (something) VALUES('tist')")
	require.NoError(t, err)

	model := models.CreateModel(reflect.TypeOf(DeleteModel{}))
	statement := NewDeleteStatement(model, database, &connection.SqliteInfo{})

	affected, err := statement.Where(xpr.Equals(xpr.Field(model, "Id"), xpr.Parameter())).Prepare().Execute(2)
	require.NoError(t, err)
	require.Equal(t, int64(1), affected)

	entities, err := NewLoadStatement(database, &connection.SqliteInfo{}).Model(model).Prepare().ExecuteEntity()

	require.NoError(t, err)
	require.Equal(t, 3, len(entities))
}
