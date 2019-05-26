package topo

import (
	"github.com/cihub/seelog"
	"github.com/daiguadaidai/dal/utils"
	"sync"
)

type ShardTableMapInstance struct {
	shardTableMaps   []*sync.Map
	shardTableMapCnt int // 保存shard table的实例数
	ShardTableCnt    int // 有几个表是需要分库分表的
}

func NewShardTableMapInstance(mapCnt int) *ShardTableMapInstance {
	stMaps := make([]*sync.Map, mapCnt)
	for i := 0; i < mapCnt; i++ {
		stMaps[i] = new(sync.Map)
	}

	return &ShardTableMapInstance{
		shardTableMaps:   stMaps,
		shardTableMapCnt: mapCnt,
	}
}

// 添加ShardTable
func (this *ShardTableMapInstance) AddShardTable(schema, table string, cols ...string) error {
	st, err := NewShardTable(schema, table, cols...)
	if err != nil {
		return err
	}

	for i, stMap := range this.shardTableMaps {
		stMap.Store(st.TableName(), st.Clone())
		seelog.Debugf("shard table:%s.%s 成功添加到第%d个实例中", schema, table, i)
	}
	seelog.Infof("表:%s.%s. 成功添加到每一个shard table实例中. shard Table实例一共有 %d 个", schema, table, len(this.shardTableMaps))

	this.ShardTableCnt++
	return nil
}

// 获取shard表
func (this *ShardTableMapInstance) GetShardTable(schema, table string) (*ShardTable, bool) {
	// 随机计算出使用哪一个map实例
	key := utils.ConcatTableName(&schema, &table)
	return this.GetShardTableByKey(key)
}

func (this *ShardTableMapInstance) GetShardTableByKey(key string) (*ShardTable, bool) {
	stMap := this.GetShardTableMapByRand()
	st, ok := stMap.Load(key)
	if !ok {
		return nil, ok
	}
	return st.(*ShardTable), ok
}

// 获取随机的分表实例
func (this *ShardTableMapInstance) GetShardTableMapByRand() *sync.Map {
	slot := utils.GetRandSlot(this.shardTableMapCnt)
	stMap := this.shardTableMaps[slot]
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
