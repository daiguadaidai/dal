package topo

import (
	"fmt"
	"sync"
)

type MySQLCluster struct {
	sync.RWMutex
	shardGroupMap map[int]int         // 分片对应的组Map key: shard 号, value: 组号 GNO
	groups        map[int]*MySQLGroup // key: GNO, value: MySQLGroup
}

func DefaultMySQLCluster() *MySQLCluster {
	return &MySQLCluster{
		shardGroupMap: make(map[int]int),
		groups:        make(map[int]*MySQLGroup),
	}
}

func (this *MySQLCluster) AddGroup(group *MySQLGroup) {
	this.groups[group.GNO] = group
}

// 初始化 shard对应group
func (this *MySQLCluster) InitShardGroup() {
	shardGroupMap := make(map[int]int)
	// 新建一组group, 用于clone, 需要对 groups资源进行上锁
	tmpGroups := this.GetGroups()

	// 需要生成一个临时的tmpGroup主要是为了防止有死锁的情况, 在操作group的时候相关资源也是有加锁的
	for _, group := range tmpGroups {
		shardNoMap := group.GetShardNoMap()
		// 如果group没有分片信息 默认设置分片信息为 -1
		if len(shardNoMap) == 0 {
			shardGroupMap[-1] = group.GNO
			continue
		}
		for key, _ := range shardNoMap {
			shardGroupMap[key] = group.GNO
		}
	}

	this.Lock()
	defer this.Unlock()
	this.shardGroupMap = shardGroupMap
}

// 通过分片好来获取指定MySQL读节点
func (this *MySQLCluster) GetReadNodeByShard(shardNO int) (*MySQLNode, error) {
	group, err := this.GetGroupByShard(shardNO)
	if err != nil {
		return nil, err
	}

	return group.GetReadNode()
}

// 通过分片好来获取指定MySQL写节点
func (this *MySQLCluster) GetWriteNodeByShard(shardNO int) (*MySQLNode, error) {
	group, err := this.GetGroupByShard(shardNO)
	if err != nil {
		return nil, err
	}

	writeNode, ok := group.GetWriteNode()
	if !ok {
		return nil, fmt.Errorf("没有获取到可写节点, 请检查是否没有可写节点.")
	}

	return writeNode, nil
}

// 通过分片好获取组
func (this *MySQLCluster) GetGroupByShard(shardNO int) (*MySQLGroup, error) {
	this.RLock()
	defer this.RUnlock()

	gno, ok := this.shardGroupMap[shardNO]
	if !ok {
		return nil, fmt.Errorf("指定的分片号: %d 没有获取到对应的 GNO", shardNO)
	}

	group, ok := this.groups[gno]
	if !ok {
		return nil, fmt.Errorf("没有获取到对应的group. 分片号:%d, GNO:%d", shardNO, gno)
	}

	return group, nil
}

func (this *MySQLCluster) GetGroups() []*MySQLGroup {
	this.RLock()
	defer this.RUnlock()

	groups := make([]*MySQLGroup, len(this.groups))
	var i int
	for _, group := range this.groups {
		groups[i] = group
		i++
	}

	return groups
}

// 克隆一个cluster, 深拷贝, 除了 node 的pool
func (this *MySQLCluster) Clone() *MySQLCluster {
	cluster := DefaultMySQLCluster()

	tmpGroups := this.GetGroups()
	for _, group := range tmpGroups {
		cluster.AddGroup(group.Clone())
	}

	cluster.InitShardGroup()

	return cluster
}
