package dao

import (
	"github.com/daiguadaidai/dal/config"
	"github.com/daiguadaidai/dal/gdbc"
	"github.com/daiguadaidai/dal/models"
)

type GroupDao struct {
	cfg *config.MySQLConfig
}

func NewGroupDao(cfg *config.MySQLConfig) *GroupDao {
	return &GroupDao{cfg: cfg}
}

// 通过cluster名称获取cluster
func (this *GroupDao) FindGrupByServerName(name string) ([]*models.Group, error) {
	sqlStr := `
    SELECT g.*
    FROM groups AS g
    LEFT JOIN servers AS s
        ON g.server_id = s.id
    WHERE s.name = ?
`
	db, err := gdbc.GetDB(this.cfg)
	if err != nil {
		return nil, err
	}
	defer db.Close()

	var groups []*models.Group
	if err := db.Raw(sqlStr, name).Find(&groups).Error; err != nil {
		return nil, err
	}

	return groups, nil
}
