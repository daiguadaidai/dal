package models

type Group struct {
	DefaultModel
	ClusterID int64  `gorm:"column:cluster_id" json:"cluster_id"`
	DBName    string `gorm:"column:db_name" json:"db_name"`
	GNO       int    `gorm:"column:gno" json:"gno"`
	Shards    string `gorm:"column:shards" json:"shards"`
}

func (Group) TableName() string {
	return "groups"
}
