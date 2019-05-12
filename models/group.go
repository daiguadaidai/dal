package models

type Group struct {
	DefaultModel
	ServerID int64  `gorm:"column:server_id" json:"server_id"`
	DBName   string `gorm:"column:db_name" json:"db_name"`
	GNO      int    `gorm:"column:gno" json:"gno"`
	Shards   string `gorm:"column:shards" json:"shards"`
}

func (Group) TableName() string {
	return "groups"
}
