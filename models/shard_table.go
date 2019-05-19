package models

import "strings"

type ShardTable struct {
	DefaultModel
	ServerID  int64  `gorm:"column:server_id" json:"server_id"`
	DBName    string `gorm:"column:db_name" json:"db_name"`
	Name      string `gorm:"column:name" json:"name"`
	ShardCols string `gorm:"column:shard_cols" json:"shard_cols"`
}

func (ShardTable) TableName() string {
	return "shard_tables"
}

func (this *ShardTable) Columns() []string {
	items := strings.Split(this.ShardCols, ",")
	cols := make([]string, len(items))
	for i, item := range items {
		cols[i] = strings.TrimSpace(item)
	}

	return cols
}
