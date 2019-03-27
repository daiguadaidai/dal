package topo

import (
	"fmt"
	"github.com/cihub/seelog"
	"math/rand"
	"strings"
	"sync"
)

type MySQLGroup struct {
	sync.Mutex
	GNO              int // 组号
	Master           string
	CandidateMasters map[string]struct{} // 候选 master
	Slaves           map[string]struct{} // 保存了所有的 slave 地址
	Nodes            map[string]*MySQLNode
	TotalReadWeight  int // 总的读权重
}

func (this *MySQLGroup) String() string {
	hosts := make([]string, len(this.Slaves))
	for host, _ := range this.Slaves {
		hosts = append(hosts, host)
	}
	return fmt.Sprintf("组号:%d. 主节点:%s. 从节点:%s",
		this.GNO, this.Master, strings.Join(hosts, ", "))
}

// 循环获取总读权重
func (this *MySQLGroup) loopGetTotalReadWeight() int {
	var totalWeight int
	for _, node := range this.Nodes {
		totalWeight += node.ReadWeight
	}
	return totalWeight
}

// 从新设置权重
func (this *MySQLGroup) resetTotalReadWeight() error {
	this.Lock()
	defer this.Unlock()

	// 需要先判断一下权重是否已经大于0了, 主要是为了防止并发更新
	if this.TotalReadWeight > 0 {
		return nil
	}

	// 该组没有节点
	if len(this.Slaves) == 0 {
		return fmt.Errorf("该组没有节点信息.")
	}

	var totalWeight int
	totalWeight = this.loopGetTotalReadWeight() // 循环获取总权重

	if totalWeight < 1 {
		seelog.Warnf("所有节点权重都为0, 将默认设置权重都为1, 其中也包括 master 的权重也设置为1. "+
			"该组的节点有:%s", this.String())
		for host, node := range this.Nodes {
			node.ResetReadWeight(1)
			seelog.Warnf("节点:%s, 设置权重为1", host)
		}

		totalWeight = this.loopGetTotalReadWeight() // 循环获取总权重
	}

	this.TotalReadWeight = totalWeight

	return nil
}

/* 获取读节点
选取节点算法:
    1. 通过总权重获取一个随机权重
    2. 循环所有节点 并且 进行叠加权重
    3. 随机权重 < 叠加权重 循环的当前节点被选中
*/
func (this *MySQLGroup) GetReadNode() (*MySQLNode, error) {
	if this.TotalReadWeight <= 0 {
		if err := this.resetTotalReadWeight(); err != nil {
			return nil, err
		}
	}

	var incrWeight int // 叠加权重, 用于比较是不是选用该节点
	randWeight := rand.Intn(this.TotalReadWeight)
	// 叠加权重
	for _, node := range this.Nodes {
		if node.ReadWeight < 1 { // 没有设置权重, 跳过该节点
			continue
		}
		incrWeight = node.ReadWeight // 将当前权重添加至叠加权重

		if randWeight < incrWeight { // 获取到了相关权重的节点
			return node, nil
		}
	}

	return nil, fmt.Errorf("没有选取到可用节点, 总权重:%d, 随机权重:%d, 轮训权重:%d",
		randWeight, this.TotalReadWeight, incrWeight)
}

func (this *MySQLGroup) GetWriteNode() (*MySQLNode, bool) {
	master, ok := this.Nodes[this.Master]
	return master, ok
}
