package dao

import (
	"github.com/daiguadaidai/dal/config"
	"github.com/daiguadaidai/dal/gdbc"
	"github.com/daiguadaidai/dal/models"
)

type NodeDao struct {
	cfg *config.MySQLConfig
}

func NewNodeDao(cfg *config.MySQLConfig) *NodeDao {
	return &NodeDao{cfg: cfg}
}

// 通过cluster名称获取cluster
func (this *NodeDao) FindNodeByClusterName(name string) ([]*models.Node, error) {
	sql := `
    SELECT n.*
    FROM nodes AS n
    LEFT JOIN groups AS g
        ON n.group_id = g.ID
    LEFT JOIN clusters AS c
        ON g.cluster_id = c.id
    WHERE c.name = ?
`
	db, err := gdbc.GetDB(this.cfg)
	if err != nil {
		return nil, err
	}
	defer db.Close()

	var nodes []*models.Node
	if err := db.Raw(sql, name).Find(&nodes).Error; err != nil {
		return nil, err
	}

	return nodes, nil
}
