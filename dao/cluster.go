package dao

import (
	"github.com/daiguadaidai/dal/config"
	"github.com/daiguadaidai/dal/gdbc"
	"github.com/daiguadaidai/dal/models"
)

type ClusterDao struct {
	cfg *config.MySQLConfig
}

func NewClusterDao(cfg *config.MySQLConfig) *ClusterDao {
	return &ClusterDao{cfg: cfg}
}

// 通过cluster名称获取cluster
func (this *ClusterDao) GetClusterByName(name string) (*models.Cluster, error) {
	db, err := gdbc.GetDB(this.cfg)
	if err != nil {
		return nil, err
	}
	defer db.Close()

	cluster := new(models.Cluster)
	if err := db.Model(cluster).Where("name=?", name).Scan(cluster).Error; err != nil {
		return nil, err
	}

	return cluster, nil
}
