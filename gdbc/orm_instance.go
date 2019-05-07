package gdbc

import (
	"fmt"
	"github.com/daiguadaidai/dal/config"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jinzhu/gorm"
)

func GetDB(cfg config.MySQLConfig) (*gorm.DB, error) {
	db, err := gorm.Open("mysql", cfg.GetDataSource())
	if err != nil { // 打开数据库失败
		return nil, fmt.Errorf("打开数据库失败 %s:%d , %v", cfg.Host, cfg.Port, err)
	}

	db.DB().SetMaxOpenConns(cfg.MaxOpenConns)
	db.DB().SetMaxIdleConns(cfg.MaxIdelConns)

	return db, nil
}
