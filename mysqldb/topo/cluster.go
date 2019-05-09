package topo

import (
	"fmt"
	"sync"
)

type MySQLCluster struct {
	sync.RWMutex
	Name          string
	ListenHost    string
	ListenPort    int
	DBName        string
	Username      string
	Password      string
	shardGroupMap map[int]int         // 分片对应的组Map key: shard 号, value: 组号 GNO
	groups        map[int]*MySQLGroup // key: GNO, value: MySQLGroup
}

func DefaultMySQLCluster() *MySQLCluster {
	return &MySQLCluster{
		shardGroupMap: make(map[int]int),
		groups:        make(map[int]*MySQLGroup),
	}
}

// 显示集群概要信息
func (this *MySQLCluster) Summary() string {
	return fmt.Sprintf("{Name:%s, ListenHost:%s, ListenPort:%d, Username:%s, Password:******, Database:%s}",
		this.Name, this.ListenHost, this.ListenPort, this.Username, this.DBName)
}

func (this *MySQLCluster) Addr() string {
	return fmt.Sprintf("%s:%d", this.ListenHost, this.ListenPort)
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
		shardNumMap := group.GetShardNumMap()
		for key, _ := range shardNumMap {
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
	cluster.Name = this.Name
	cluster.DBName = this.DBName
	cluster.ListenHost = this.ListenHost
	cluster.ListenPort = this.ListenPort
	cluster.Username = this.Username
	cluster.Password = this.Password

	tmpGroups := this.GetGroups()
	for _, group := range tmpGroups {
		cluster.AddGroup(group.Clone())
	}

	cluster.InitShardGroup()

	return cluster
}

// 保存多个cluster元数据实例
type ClusterInstance struct {
	Clusters   []*MySQLCluster
	ClusterCnt int
}

func NewClusterInstance(clusterCnt int, cluster *MySQLCluster) *ClusterInstance {
	clusterInstance := &ClusterInstance{
		Clusters:   make([]*MySQLCluster, clusterCnt),
		ClusterCnt: clusterCnt,
	}

	for i := 0; i < clusterCnt; i++ {
		clusterInstance.Clusters[i] = cluster.Clone()
	}

	return clusterInstance
}
