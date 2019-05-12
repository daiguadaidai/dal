package dao

import (
	"github.com/daiguadaidai/dal/config"
	"github.com/daiguadaidai/dal/gdbc"
	"github.com/daiguadaidai/dal/models"
)

type ServerDao struct {
	cfg *config.MySQLConfig
}

func NewServerDao(cfg *config.MySQLConfig) *ServerDao {
	return &ServerDao{cfg: cfg}
}

// 通过server名称获取server
func (this *ServerDao) GetServerByName(name string) (*models.Server, error) {
	db, err := gdbc.GetDB(this.cfg)
	if err != nil {
		return nil, err
	}
	defer db.Close()

	server := new(models.Server)
	if err := db.Model(server).Where("name=?", name).First(server).Error; err != nil {
		return nil, err
	}

	return server, nil
}
