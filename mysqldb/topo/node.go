package topo

import (
	"github.com/daiguadaidai/dal/mysqldb/pool"
)

type MySQLNode struct {
	DBName     string
	Username   string // 链接数据库的用户名
	Password   string // 链接数据库的密码
	Pool       *pool.MySQLPool
	ReadWeight int // 读写分离 读权重
}

func (this *MySQLNode) ResetReadWeight(weight int) {
	this.ReadWeight = weight
}
