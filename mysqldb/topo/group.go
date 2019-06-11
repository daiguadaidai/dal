package topo

import (
	"fmt"
	"github.com/cihub/seelog"
	"github.com/daiguadaidai/dal/utils"
	"github.com/daiguadaidai/dal/utils/types"
	"strings"
	"sync"
)

type MySQLGroup struct {
	sync.RWMutex
	Gno              int // 组号
	DBName           string
	Master           string
	candidateMasters map[string]struct{} // 候选 master
	slaves           map[string]struct{} // 保存了所有的 slave 地址
	nodes            map[string]*MySQLNode
	totalReadWeight  int              // 总的读权重
	shardNoMap       map[int]struct{} // 该组用于哪些分片
}

func NewMySQLGroup(dbName string, gno int) *MySQLGroup {
	return &MySQLGroup{
		DBName:           dbName,
		Gno:              gno,
		candidateMasters: make(map[string]struct{}),
		slaves:           make(map[string]struct{}),
		nodes:            make(map[string]*MySQLNode),
		shardNoMap:       make(map[int]struct{}),
	}
}

func (this *MySQLGroup) String() string {
	slaves := this.GetSlaves()
	hosts := make([]string, len(slaves))
	for i, slave := range slaves {
		hosts[i] = slave.Addr()
	}
	return fmt.Sprintf("Gno:%d., Master:%s, Slaves:%s",
		this.Gno, this.Master, strings.Join(hosts, ", "))
}

// 通过字符串设置shard num: 1,2, 3,4
func (this *MySQLGroup) SetShardNoMapByStr(shardNoStr string) {
	// 将字符串转化为 map
	shardNoMap := utils.ShardNoStrsToIntMap(shardNoStr)

	// 设置shardNoMap
	this.SetShardNoMapByMap(shardNoMap)
}

func (this *MySQLGroup) SetShardNoMapByMap(shardNoMap map[int]struct{}) {
	this.Lock()
	defer this.Unlock()
	this.shardNoMap = shardNoMap
}

// 循环获取总读权重
func (this *MySQLGroup) loopGetTotalReadWeight() int {
	var totalWeight int
	for _, node := range this.nodes {
		totalWeight += node.ReadWeight
	}
	return totalWeight
}

// 从新设置权重
func (this *MySQLGroup) resetTotalReadWeight() error {
	this.Lock()
	defer this.Unlock()

	// 该组没有节点
	if len(this.nodes) == 0 {
		return fmt.Errorf("该组没有节点信息.")
	}

	var totalWeight int
	totalWeight = this.loopGetTotalReadWeight() // 循环获取总权重

	if totalWeight < 1 {
		seelog.Warnf("所有节点权重都为0, 将默认设置权重都为1, 其中也包括 master 的权重也设置为1. "+
			"该组的节点有:%s", this.String())
		for host, node := range this.nodes {
			node.ResetReadWeight(1)
			seelog.Warnf("节点:%s, 设置权重为1", host)
		}

		totalWeight = this.loopGetTotalReadWeight() // 循环获取总权重
	}

	this.totalReadWeight = totalWeight

	return nil
}

/* 获取读节点
选取节点算法:
    1. 通过总权重获取一个随机权重
    2. 循环所有节点 并且 进行叠加权重
    3. 随机权重 < 叠加权重 循环的当前节点被选中
*/
func (this *MySQLGroup) GetReadNode() (*MySQLNode, error) {
	if this.totalReadWeight <= 0 {
		if err := this.resetTotalReadWeight(); err != nil {
			return nil, err
		}
	}

	var incrWeight int                                   // 叠加权重, 用于比较是不是选用该节点
	randWeight := utils.RandInt(1, this.totalReadWeight) // 值为: 1 ~ 总权重值
	// 叠加权重
	this.RLock()
	defer this.RUnlock()
	for _, node := range this.nodes {
		if node.ReadWeight < 1 { // 没有设置权重, 跳过该节点
			continue
		}
		incrWeight += node.ReadWeight // 将当前权重添加至叠加权重

		if incrWeight >= randWeight { // 获取到了相关权重的节点
			return node, nil
		}
	}

	return nil, fmt.Errorf("没有选取到可用节点, 总权重:%d, 随机权重:%d, 轮训权重:%d",
		this.totalReadWeight, randWeight, incrWeight)
}

// 获取写节点
func (this *MySQLGroup) GetWriteNode() (*MySQLNode, error) {
	if strings.TrimSpace(this.Master) == "" {
		return nil, fmt.Errorf("组:%d, Master不存在", this.Gno)
	}

	this.RLock()
	defer this.RUnlock()

	master, ok := this.nodes[this.Master]
	if !ok {
		return nil, fmt.Errorf("组:%d, Master[%s]不存在", this.Gno, this.Master)
	}
	return master, nil
}

// 通过指定key获取节点
func (this *MySQLGroup) GetNode(key string) (*MySQLNode, bool) {
	this.RLock()
	defer this.RUnlock()

	node, ok := this.nodes[key]
	return node, ok
}

func (this *MySQLGroup) AddNode(node *MySQLNode) error {
	if _, ok := this.GetNode(node.Addr()); ok {
		return fmt.Errorf("节点 %s 已经存在. 不允许重复添加", node.Addr())
	}
	// 判断是否是master
	if node.Role == types.MYSQL_ROLE_MASTER && len(this.Master) == 0 {
		if len(this.Master) == 0 {
			this.Master = node.Addr()
		} else {
			return fmt.Errorf("已经存在master:%s, 不允许添加新master:%s", this.Master, node.Addr())
		}
	}

	this.Lock()
	// 添加节点
	this.nodes[node.Addr()] = node

	// 添加slave
	if node.Role == types.MYSQL_ROLE_SLAVE {
		this.slaves[node.Addr()] = struct{}{}
	}

	// 添加候选master
	if node.IsCandidate {
		this.candidateMasters[node.Addr()] = struct{}{}
	}
	this.Unlock()

	// 从新设置读权重
	if err := this.resetTotalReadWeight(); err != nil {
		return err
	}

	return nil
}

// 获取group的shard num map
func (this *MySQLGroup) GetShardNoMap() map[int]struct{} {
	shardNoMap := make(map[int]struct{})

	this.RLock()
	defer this.RUnlock()

	for key, _ := range this.shardNoMap {
		shardNoMap[key] = struct{}{}
	}

	return shardNoMap
}

// 获取slave所有的
func (this *MySQLGroup) GetSlaves() []*MySQLNode {
	this.RLock()
	defer this.RUnlock()

	slaves := make([]*MySQLNode, len(this.slaves))
	var i int
	for key, _ := range this.slaves {
		slave, ok := this.nodes[key]
		if ok {
			slaves[i] = slave
		}
		i++
	}
	return slaves
}

// 获取所有的节点
func (this *MySQLGroup) GetNodes() []*MySQLNode {
	this.RLock()
	defer this.RUnlock()

	nodes := make([]*MySQLNode, len(this.nodes))
	var i int
	for _, node := range this.nodes {
		nodes[i] = node
		i++
	}
	return nodes
}

func (this *MySQLGroup) Clone() *MySQLGroup {
	group := NewMySQLGroup(this.DBName, this.Gno)

	// 获取所有node, 并添加
	nodes := this.GetNodes()
	for _, node := range nodes {
		group.AddNode(node.Clone())
	}

	// 获取group对应的的分片信息
	shardNoMap := this.GetShardNoMap()
	group.SetShardNoMapByMap(shardNoMap)

	return group
}
