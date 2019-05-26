package topo

import (
	"fmt"
	"github.com/daiguadaidai/dal/utils"
)

// 分片的表
type ShardTable struct {
	Schema      string
	Name        string
	ShardColMap map[string]struct{}
	ShardCols   []string
}

func NewShardTable(schema, name string, cols ...string) (*ShardTable, error) {
	if len(cols) == 0 {
		return nil, fmt.Errorf("没有指定分表 %s.%s 的字段", schema, name)
	}

	shardColMap := make(map[string]struct{})
	shardCols := make([]string, len(cols))
	for i, col := range cols {
		shardColMap[col] = struct{}{}
		shardCols[i] = col
	}

	shardTable := &ShardTable{
		Schema:      schema,
		Name:        name,
		ShardColMap: shardColMap,
		ShardCols:   shardCols,
	}

	return shardTable, nil
}

// 获取完整表名
func (this *ShardTable) TableName() string {
	return utils.ConcatTableName(&this.Schema, &this.Name)
}

// 克隆一个分表. 深拷贝
func (this *ShardTable) Clone() *ShardTable {
	st := new(ShardTable)

	st.Schema = this.Schema
	st.Name = this.Name
	st.ShardCols = make([]string, len(this.ShardCols))
	st.ShardColMap = make(map[string]struct{})

	for i, col := range this.ShardCols {
		st.ShardCols[i] = col
		st.ShardColMap[col] = struct{}{}
	}

	return st
}
