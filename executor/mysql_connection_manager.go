package executor

import (
	"fmt"
	"github.com/cihub/seelog"
	"github.com/daiguadaidai/dal/dal_context"
	"strings"
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
func (this *MySQLConnectionManager) GetReadConnByRand(autoCommit bool) (int, *NodeConn, error) {
	// 链接已经存在
	if this.randGno != RAND_GNO {
		nodeConn, err := this.GetReadNodeConnByGno(this.randGno, autoCommit)
		if err != nil {
			this.randGno = RAND_GNO
			return RAND_GNO, nil, err
		}
		return this.randGno, nodeConn, nil
	}

	// 随机获取一个节点
	gno := this.ctx.ClusterInstance.GetGnoByRand()
	nodeConn, err := this.GetReadNodeConnByGno(gno, autoCommit)
	if err != nil {
		return RAND_GNO, nil, err
	}
	this.randGno = gno
	return gno, nodeConn, nil
}

// 通过组号获取
func (this *MySQLConnectionManager) GetReadConnByShard(shardNo int, autoCommit bool) (int, *NodeConn, error) {
	// 通过分片好获取组
	gno, err := this.ctx.ClusterInstance.GetGnoByShard(shardNo)
	if err != nil {
		return RAND_GNO, nil, err
	}

	// 通过Gno获取链接
	nodeConn, err := this.GetReadNodeConnByGno(gno, autoCommit)
	if err != nil {
		return RAND_GNO, nil, err
	}

	return gno, nodeConn, nil
}

// 通过Gno获取链接
func (this *MySQLConnectionManager) GetReadNodeConnByGno(gno int, autoCommit bool) (*NodeConn, error) {
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

	// 判断获取的链接是否是需要自动提交
	if !autoCommit { // 不是自动提交则开始一个事务
		if _, err := nodeConn.Begin(); err != nil {
			nodeConn.Close()
			delete(this.nodeConnMap, gno)
			return nil, fmt.Errorf("获取链接失败同时执行 Begin 语句失败: %s", err.Error())
		}
	}

	// 将链接保存到本地
	this.nodeConnMap[gno] = nodeConn

	return nodeConn, nil
}

// 清空链接
func (this *MySQLConnectionManager) cleanConn() {
	this.nodeConnMap = make(map[int]*NodeConn)
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

// 执行commit语句
func (this *MySQLConnectionManager) Commit() error {
	msgs := make([]string, 0)
	for gno, nodeConn := range this.nodeConnMap {
		if _, err := nodeConn.Commit(); err != nil {
			msgs = append(msgs, fmt.Sprintf("组: %d, 执行 commit 失败. %s", gno, err.Error()))
		}
	}

	if len(msgs) != 0 { // 提交失败
		return fmt.Errorf(strings.Join(msgs, ". "))
	}

	// 成功提交
	return nil
}

// 执行rollback语句
func (this *MySQLConnectionManager) Rollback() error {
	msgs := make([]string, 0)
	for gno, nodeConn := range this.nodeConnMap {
		if _, err := nodeConn.Rollback(); err != nil {
			msgs = append(msgs, fmt.Sprintf("组: %d, 执行 rollback 失败. %s", gno, err.Error()))
		}
	}

	if len(msgs) != 0 { // 执行rollback失败
		return fmt.Errorf(strings.Join(msgs, ". "))
	}

	// 成功回滚
	return nil
}

// 回收所有链接
func (this *MySQLConnectionManager) Close() error {
	msgs := make([]string, 0)
	for gno, nodeConn := range this.nodeConnMap {
		if err := nodeConn.Close(); err != nil {
			msgs = append(msgs, fmt.Sprintf("组: %d, 关闭链接失败. %s", gno, err.Error()))
		}
	}

	this.cleanConn()

	if len(msgs) != 0 { // 关闭失败
		return fmt.Errorf(strings.Join(msgs, ". "))
	}

	// 成功关闭
	return nil
}
