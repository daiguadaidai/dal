package dao

import (
	"github.com/daiguadaidai/dal/config"
	"github.com/daiguadaidai/dal/gdbc"
	"github.com/daiguadaidai/dal/models"
	"github.com/jinzhu/gorm"
)

type ClusterDao struct {
	DB *gorm.DB
}

func NewClusterDao(cfg *config.MySQLConfig) (*ClusterDao, error) {
	db, err := gdbc.GetDB(cfg)
	if err != nil {
		return nil, err
	}
	return &ClusterDao{DB: db}, nil
}

// 关闭链接
func (this *ClusterDao) Close() {
	if this.DB != nil {
		this.DB.Close()
	}
}

// 通过cluster名称获取cluster
func (this *ClusterDao) GetClusterByName(name string) (*models.Cluster, error) {
	cluster := new(models.Cluster)
	if err := this.DB.Model(cluster).Where("name=?", name).Scan(cluster).Error; err != nil {
		return nil, err
	}

	return cluster, nil
}
