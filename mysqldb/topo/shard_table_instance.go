package topo

import (
	"fmt"
	"github.com/cihub/seelog"
	"github.com/daiguadaidai/dal/utils"
	"sync"
)

type ShardTableMapInstance struct {
	ShardTables   []*sync.Map
	ShardTableCnt int
}

func NewShardTableMapInstance(mapCnt int) *ShardTableMapInstance {
	stMaps := make([]*sync.Map, mapCnt)
	for i := 0; i < mapCnt; i++ {
		stMaps[i] = new(sync.Map)
	}

	return &ShardTableMapInstance{
		ShardTables:   stMaps,
		ShardTableCnt: 0,
	}
}

// 添加ShardTable
func (this *ShardTableMapInstance) AddShardTable(schema, table string, cols ...string) error {
	st, err := NewShardTable(schema, table, cols...)
	if err != nil {
		return err
	}

	for i, stMap := range this.ShardTables {
		stMap.Store(st.TableName(), st.Clone())
		seelog.Debugf("shard table:%s.%s 成功添加到第%d个实例中", schema, table, i)
	}
	seelog.Infof("表:%s.%s. 成功添加到每一个shard table实例中. shard Table实例一共有 %d 个", schema, table, len(this.ShardTables))

	return nil
}

// 获取shard表
func (this *ShardTableMapInstance) GetShardTable(schema, table string) (*ShardTable, bool) {
	// 随机计算出使用哪一个map实例
	slot := utils.GetRandSlot(this.ShardTableCnt)
	stMap := this.ShardTables[slot]

	key := fmt.Sprintf("%s.%s", schema, table)
	st, ok := stMap.Load(key)
	if !ok {
		return nil, ok
	}
	return st.(*ShardTable), ok
}
