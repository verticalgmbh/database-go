package walkers

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/verticalgmbh/database-go/connection"
	"github.com/verticalgmbh/database-go/xpr"
)

func TestParameterExpression(t *testing.T) {
	var command strings.Builder

	walker := SqlWalker{
		connectioninfo: &connection.SqliteInfo{},
		builder:        &command}

	walker.Visit(xpr.Equals(xpr.Parameter(), "teststring"))

	assert.Equal(t, `? = 'teststring'`, command.String())
}

func TestInExpression(t *testing.T) {
	var command strings.Builder

	walker := SqlWalker{
		connectioninfo: &connection.SqliteInfo{},
		builder:        &command}

	walker.Visit(xpr.In(xpr.Parameter(), 1, 6, 33, 4))

	assert.Equal(t, `? IN (1,6,33,4)`, command.String())
}
