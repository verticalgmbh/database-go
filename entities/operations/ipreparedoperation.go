package operations

// IPreparedOperation a statement prepared for execution. Provides a prepared sql string which can get send to database as is (sometimes adding parameters)
type IPreparedOperation interface {

	// Command sql command string sent to database
	//
	// **Returns**
	//   - string: sql-command
	Command() string
}
