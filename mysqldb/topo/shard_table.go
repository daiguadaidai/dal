package topo

import (
	"fmt"
	"github.com/cihub/seelog"
	"math/rand"
	"sync"
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

type ShardTableMapInstance struct {
	ShardTableMaps []*sync.Map
	ShardTableCnt  int
}

func NewShardTableMapInstance(mapCnt int) *ShardTableMapInstance {
	stMaps := make([]*sync.Map, mapCnt)
	for i := 0; i < mapCnt; i++ {
		stMaps[i] = new(sync.Map)
	}

	return &ShardTableMapInstance{
		ShardTableMaps: stMaps,
		ShardTableCnt:  0,
	}
}

// 添加ShardTable
func (this *ShardTableMapInstance) AddShardTable(schema, table string, cols ...string) error {
	st, err := NewShardTable(schema, table, cols...)
	if err != nil {
		return err
	}

	for i, stMap := range this.ShardTableMaps {
		stMap.Store(st.TableName(), st.Clone())
		seelog.Debugf("shard table:%s.%s 成功添加到第%d个实例中", schema, table, i)
	}
	seelog.Infof("表:%s.%s. 成功添加到每一个shard table实例中. shard Table实例一共有 %d 个", schema, table, len(this.ShardTableMaps))

	return nil
}

// 获取shard表
func (this *ShardTableMapInstance) GetShardTable(schema, table string) (*ShardTable, bool) {
	// 随机计算出使用哪一个map实例
	mapSlot := rand.Intn(len(this.ShardTableMaps) - 1)
	stMap := this.ShardTableMaps[mapSlot]

	key := fmt.Sprintf("%s.%s", schema, table)
	st, ok := stMap.Load(key)
	if !ok {
		return nil, ok
	}
	return st.(*ShardTable), ok
}
