package models

type ShardTable struct {
	DefaultModel
	ClusterID int64  `gorm:"column:cluster_id" json:"cluster_id"`
	DBName    string `gorm:"column:db_name" json:"db_name"`
	Name      string `gorm:"column:name" json:"name"`
	ShardCols string `gorm:"column:shard_cols" json:"shard_cols"`
}

func (ShardTable) TableName() string {
	return "shard_tables"
}
