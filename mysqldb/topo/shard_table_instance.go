package topo

import (
	"fmt"
	"github.com/cihub/seelog"
	"github.com/daiguadaidai/dal/utils"
	"sync"
)

type ShardTableMapInstance struct {
	shardTables   []*sync.Map
	ShardTableCnt int
}

func NewShardTableMapInstance(mapCnt int) *ShardTableMapInstance {
	stMaps := make([]*sync.Map, mapCnt)
	for i := 0; i < mapCnt; i++ {
		stMaps[i] = new(sync.Map)
	}

	return &ShardTableMapInstance{
		shardTables:   stMaps,
		ShardTableCnt: mapCnt,
	}
}

// 添加ShardTable
func (this *ShardTableMapInstance) AddShardTable(schema, table string, cols ...string) error {
	st, err := NewShardTable(schema, table, cols...)
	if err != nil {
		return err
	}

	for i, stMap := range this.shardTables {
		stMap.Store(st.TableName(), st.Clone())
		seelog.Debugf("shard table:%s.%s 成功添加到第%d个实例中", schema, table, i)
	}
	seelog.Infof("表:%s.%s. 成功添加到每一个shard table实例中. shard Table实例一共有 %d 个", schema, table, len(this.shardTables))

	return nil
}

// 获取shard表
func (this *ShardTableMapInstance) GetShardTable(schema, table string) (*ShardTable, bool) {
	// 随机计算出使用哪一个map实例
	stMap := this.GetShardTableMapByRand()

	key := fmt.Sprintf("%s.%s", schema, table)
	st, ok := stMap.Load(key)
	if !ok {
		return nil, ok
	}
	return st.(*ShardTable), ok
}

// 获取随机的分表实例
func (this *ShardTableMapInstance) GetShardTableMapByRand() *sync.Map {
	slot := utils.GetRandSlot(this.ShardTableCnt)
	stMap := this.shardTables[slot]
	return stMap
}

// 获取所有的分表信息
func (this *ShardTableMapInstance) GetShardTables() []*ShardTable {
	shardTables := make([]*ShardTable, 0)
	stMap := this.GetShardTableMapByRand()
	stMap.Range(func(_, value interface{}) bool {
		shardTables = append(shardTables, value.(*ShardTable))
		return true
	})

	return shardTables
}
