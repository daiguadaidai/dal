package types

const (
	MYSQL_ROLE_NONE int8 = iota
	MYSQL_ROLE_MASTER
	MYSQL_ROLE_SLAVE
)

const (
	DAL_NO int8 = iota
	DAL_YES
)
