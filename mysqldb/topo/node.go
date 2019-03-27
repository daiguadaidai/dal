package topo

import (
	"github.com/daiguadaidai/dal/mysqldb/pool"
)

type MySQLNode struct {
	Pool       *pool.MySQLPool
	ReadWeight int // 读写分离  读权重
}

func (this *MySQLNode) ResetReadWeight(weight int) {
	this.ReadWeight = weight
}
