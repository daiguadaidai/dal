package topo

import "github.com/daiguadaidai/dal/utils"

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

// 随机获取一个cluster
func (this *ClusterInstance) GetClusterByRand() *MySQLCluster {
	// 随机计算出使用哪一个map实例
	slot := utils.GetRandSlot(this.ClusterCnt)
	return this.Clusters[slot]
}

// 获取只读节点
func (this *ClusterInstance) GetReadNodeByShard(shardNo int) (*MySQLNode, error) {
	cluster := this.GetClusterByRand()
	return cluster.GetReadNodeByShard(shardNo)
}

// 获取读写节点
func (this *ClusterInstance) GetWriteNodeByShard(shardNo int) (*MySQLNode, error) {
	cluster := this.GetClusterByRand()
	return cluster.GetWriteNodeByShard(shardNo)
}

// 通过分片号, 获取组
func (this *ClusterInstance) GetGroupByShard(shardNo int) (*MySQLGroup, error) {
	cluster := this.GetClusterByRand()
	return cluster.GetGroupByShard(shardNo)
}

// 获取随机 gno
func (this *ClusterInstance) GetGnoByRand() int {
	cluster := this.GetClusterByRand()
	return cluster.GetGnoByRand()
}

/* 随机获取一个读节点
Return:
    int: 组号
	*MySQLNode: mysql节点
	error: 错误
*/
func (this *ClusterInstance) GetReadNodeByRand() (int, *MySQLNode, error) {
	cluster := this.GetClusterByRand()
	return cluster.GetReadNodeByRand()
}

// 通过分片好获取组号
func (this *ClusterInstance) GetGnoByShard(shardNo int) (int, error) {
	cluster := this.GetClusterByRand()
	return cluster.GetGnoByShard(shardNo)
}

// 通过 gno 获取读节点
func (this *ClusterInstance) GetReadNodeByGno(gno int) (*MySQLNode, error) {
	cluster := this.GetClusterByRand()
	return cluster.GetReadNodeByGno(gno)
}
