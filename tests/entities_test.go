package tests

type SchemaEntity struct {
	Id        int64  `database:primarykey,autoincrement`
	Guid      string `database:unique`
	Firstname string `database:index=name`
	Lastname  string `database:index=name`
	Firstsec  string `database:unique=secret`
	Secondsec string `database:unique=secret`
}
