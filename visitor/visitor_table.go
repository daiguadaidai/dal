package visitor

import "github.com/daiguadaidai/dal/mysqldb/topo"

type VisitorTable struct {
	ShardTable *topo.ShardTable
	ColValues  map[string]interface{} // 所有shard table字段对应的值, key:column, value:字段值
}

func NewVisitorTable(shardTable *topo.ShardTable) *VisitorTable {
	return &VisitorTable{
		ShardTable: shardTable,
		ColValues:  make(map[string]interface{}),
	}
}
