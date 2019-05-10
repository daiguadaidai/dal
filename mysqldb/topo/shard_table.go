package topo

import (
	"fmt"
)

// 分片的表
type ShardTable struct {
	Schema    string
	Name      string
	ShardCols map[string]struct{}
}

func NewShardTable(schema, name string, cols ...string) (*ShardTable, error) {
	shardCols := make(map[string]struct{})
	if len(cols) == 0 {
		return nil, fmt.Errorf("没有指定分表 %s.%s 的字段", schema, name)
	}
	for _, col := range cols {
		shardCols[col] = struct{}{}
	}

	shardTable := &ShardTable{
		Schema:    schema,
		Name:      name,
		ShardCols: shardCols,
	}

	return shardTable, nil
}

// 获取完整表名
func (this *ShardTable) TableName() string {
	return fmt.Sprintf("%s.%s", this.Schema, this.Name)
}

// 克隆一个分表. 深拷贝
func (this *ShardTable) Clone() *ShardTable {
	st := new(ShardTable)

	st.Schema = this.Schema
	st.Name = this.Name
	st.ShardCols = make(map[string]struct{})

	for col, _ := range this.ShardCols {
		st.ShardCols[col] = struct{}{}
	}

	return st
}
