package dao

import (
	"github.com/daiguadaidai/dal/config"
	"github.com/daiguadaidai/dal/gdbc"
	"github.com/daiguadaidai/dal/models"
)

type ShardTableDao struct {
	cfg *config.MySQLConfig
}

func NewShardTableDao(cfg *config.MySQLConfig) *ShardTableDao {
	return &ShardTableDao{cfg: cfg}
}

// 通过cluster名称获取cluster
func (this *ShardTableDao) FindByServerName(name string) ([]*models.ShardTable, error) {
	sql := `
    SELECT st.*
    FROM shard_tables AS st
    LEFT JOIN servers AS s
        ON st.server_id = s.id
    WHERE s.name = ?
`
	db, err := gdbc.GetDB(this.cfg)
	if err != nil {
		return nil, err
	}
	defer db.Close()

	var tables []*models.ShardTable
	if err := db.Raw(sql, name).Find(&tables).Error; err != nil {
		return nil, err
	}

	return tables, nil
}
