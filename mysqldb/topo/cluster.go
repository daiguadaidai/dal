package topo

import "fmt"

type MySQLCluster struct {
	Name          string
	ListenHost    string
	ListenPort    int
	DBName        string
	Username      string
	Password      string
	ShardGroupMap map[int]int         // 分片对应的组Map key: shard 号, value: 组号 GNO
	Groups        map[int]*MySQLGroup // key: GNO, value: MySQLGroup
}

// 显示集群概要信息
func (this *MySQLCluster) Summary() string {
	return fmt.Sprintf("{Name:%s, ListenHost:%s, ListenPort:%d, Username:%s, Password:******, Database:%s}",
		this.Name, this.ListenHost, this.ListenPort, this.Username, this.DBName)
}

func (this *MySQLCluster) Addr() string {
	return fmt.Sprintf("%s:%d", this.ListenHost, this.ListenPort)
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

	return group.GetReadNode()
}

// 通过分片好获取组
func (this *MySQLCluster) GetGroupByShard(shardNO int) (*MySQLGroup, error) {
	gno, ok := this.ShardGroupMap[shardNO]
	if !ok {
		return nil, fmt.Errorf("指定的分片号: %d 没有获取到对应的 GNO", shardNO)
	}

	group, ok := this.Groups[gno]
	if !ok {
		return nil, fmt.Errorf("没有获取到对应的group. 分片号:%d, GNO:%d", shardNO, gno)
	}

	return group, nil
}
