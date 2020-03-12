package statements

import (
	"database/sql"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/verticalgmbh/database-go/connection"
	"github.com/verticalgmbh/database-go/xpr"
)

func TestCoalesce(t *testing.T) {
	database, err := sql.Open("sqlite3", ":memory:")
	require.NoError(t, err)
	defer database.Close()

	statement := NewLoadStatement(database, &connection.SqliteInfo{})
	statement.Fields(xpr.Coalesce(nil, 8))

	operation := statement.Prepare()

	result, err := operation.ExecuteScalar()

	require.NoError(t, err)
	number := result.(int64)

	require.Equal(t, int64(8), number)
}
