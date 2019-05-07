package models

type Node struct {
	DefaultModel
	GroupID      int64  `gorm:"column:group_id" json:"group_id"`
	Role         int8   `gorm:"column:role" json:"role"`
	IsCandidate  int8   `gorm:"column:is_candidate" json:"is_candidate"`
	Username     string `gorm:"column:username" json:"username"`
	Password     string `gorm:"column:password" json:"password"`
	Host         string `gorm:"column:host" json:"host"`
	Port         int    `gorm:"column:port" json:"port"`
	DBName       string `gorm:"column:db_name" json:"db_name"`
	ReadWeight   int    `gorm:"column:read_weight" json:"read_weight"`
	Charset      string `gorm:"column:charset" json:"charset"`
	IsAutoCommit int8   `gorm:"column:is_auto_commit" json:"is_auto_commit"`
}

func (Node) TableName() string {
	return "nodes"
}
