package walkers

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"nightlycode.de/database/connection"
	"nightlycode.de/database/xpr"
)

func TestParameterExpression(t *testing.T) {
	var command strings.Builder

	walker := SqlWalker{
		connectioninfo: &connection.SqliteInfo{},
		builder:        &command}

	walker.Visit(xpr.Equals(xpr.Parameter(1), "teststring"))

	assert.Equal(t, `@1 = 'teststring'`, command.String())
}
