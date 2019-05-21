package executor

import (
	"fmt"
	"github.com/cihub/seelog"
	"github.com/daiguadaidai/dal/dal_context"
)

const (
	RAND_GNO = -1
)

type MySQLConnectionManager struct {
	ctx         *dal_context.DalContext
	nodeConnMap map[int]*NodeConn // tidb的数据库客户端链接. key:Gno, value:*NodeConn
	randGno     int               // 随机组号
}

func NewMySQLConnectionManager(ctx *dal_context.DalContext) *MySQLConnectionManager {
	return &MySQLConnectionManager{
		ctx:         ctx,
		nodeConnMap: make(map[int]*NodeConn),
		randGno:     RAND_GNO,
	}
}

// 随机获取链接
func (this *MySQLConnectionManager) GetReadConnByRand() (int, *NodeConn, error) {
	// 链接已经存在
	if this.randGno != RAND_GNO {
		nodeConn, err := this.GetReadNodeConnByGno(this.randGno)
		if err != nil {
			this.randGno = RAND_GNO
			return RAND_GNO, nil, err
		}
		return this.randGno, nodeConn, nil
	}

	// 随机获取一个节点
	gno := this.ctx.ClusterInstance.GetGnoByRand()
	nodeConn, err := this.GetReadNodeConnByGno(gno)
	if err != nil {
		return RAND_GNO, nil, err
	}
	this.randGno = gno
	return gno, nodeConn, nil
}

// 通过组号获取
func (this *MySQLConnectionManager) GetReadConnByShard(shardNo int) (int, *NodeConn, error) {
	// 通过分片好获取组
	gno, err := this.ctx.ClusterInstance.GetGnoByShard(shardNo)
	if err != nil {
		return RAND_GNO, nil, err
	}

	// 通过Gno获取链接
	nodeConn, err := this.GetReadNodeConnByGno(gno)
	if err != nil {
		return RAND_GNO, nil, err
	}

	return gno, nodeConn, nil
}

// 通过Gno获取链接
func (this *MySQLConnectionManager) GetReadNodeConnByGno(gno int) (*NodeConn, error) {
	nodeConn, ok := this.nodeConnMap[gno]
	if ok {
		return nodeConn, nil
	}

	node, err := this.ctx.ClusterInstance.GetReadNodeByGno(gno)
	if err != nil {
		return nil, err
	}

	conn, err := node.Pool.Get()
	if err != nil {
		return nil, err
	}

	nodeConn = NewNodeConn(node, conn)
	// 将链接保存到本地
	this.nodeConnMap[gno] = nodeConn

	return nodeConn, nil
}

// 关闭链接
func (this *MySQLConnectionManager) CloseConnByGno(gno int) error {
	nodeConn, ok := this.nodeConnMap[gno]
	if !ok {
		msg := fmt.Sprintf("将链接返防给连接池失败. 通过Gno:%d, 获取不到相关链接", gno)
		seelog.Errorf(msg)
		return fmt.Errorf(msg)
	}

	nodeConn.Close()

	this.removeNodeConnByGno(gno)

	return nil
}

// 移除节点
func (this *MySQLConnectionManager) removeNodeConnByGno(gno int) {
	delete(this.nodeConnMap, gno)
}
