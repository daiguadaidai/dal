package executor

import (
	"fmt"
	"github.com/cihub/seelog"
	"github.com/daiguadaidai/dal/dal_context"
	"github.com/daiguadaidai/dal/mysqldb/topo"
	"strings"
)

type MySQLConnectionManager struct {
	ctx              *dal_context.DalContext
	readNodeConnMap  map[int]*NodeConn // 只读节点tidb的数据库客户端链接. key:Gno, value:*NodeConn
	writeNodeConnMap map[int]*NodeConn // 写节点tidb的数据库客户端链接. key:Gno, value:*NodeConn
}

func NewMySQLConnectionManager(ctx *dal_context.DalContext) *MySQLConnectionManager {
	return &MySQLConnectionManager{
		ctx:              ctx,
		readNodeConnMap:  make(map[int]*NodeConn),
		writeNodeConnMap: make(map[int]*NodeConn),
	}
}

/***********************************************************
 ********** 下面是对(读写)节点的一些操作 **********************
 **********************************************************/

// 随机获取可写链接
func (this *MySQLConnectionManager) GetWriteConnByRand(autoCommit bool) (int, *NodeConn, error) {
	// 已经有写节点存在
	if len(this.writeNodeConnMap) > 0 {
		for gno, writeNodeConn := range this.writeNodeConnMap {
			return gno, writeNodeConn, nil
		}
	}

	// 没有写节点, 则随机获取一个写节点
	gno, writeNode, err := this.ctx.ClusterInstance.GetWriteNodeByRand()
	if err != nil {
		return gno, nil, err
	}

	writeNodeConn, err := this.setAndGetWriteNodeConn(gno, writeNode, autoCommit)
	return gno, writeNodeConn, err
}

// 通过组号获取
func (this *MySQLConnectionManager) GetWriteConnByShard(shardNo int, autoCommit bool) (int, *NodeConn, error) {
	// 通过分片好获取组
	gno, err := this.ctx.ClusterInstance.GetGnoByShard(shardNo)
	if err != nil {
		return gno, nil, err
	}

	// 通过Gno获取链接
	nodeConn, err := this.GetWriteNodeConnByGno(gno, autoCommit)
	if err != nil {
		return gno, nil, err
	}

	return gno, nodeConn, nil
}

// 通过Gno获取写链接
func (this *MySQLConnectionManager) GetWriteNodeConnByGno(gno int, autoCommit bool) (*NodeConn, error) {
	nodeConn, ok := this.writeNodeConnMap[gno]
	if ok {
		return nodeConn, nil
	}

	node, err := this.ctx.ClusterInstance.GetWriteNodeByGno(gno)
	if err != nil {
		return nil, err
	}

	return this.setAndGetWriteNodeConn(gno, node, autoCommit)
}

// 设置并且获取 可写节点
func (this *MySQLConnectionManager) setAndGetWriteNodeConn(gno int, node *topo.MySQLNode, autoCommit bool) (*NodeConn, error) {
	conn, err := node.Pool.Get()
	if err != nil {
		return nil, err
	}
	writeNodeConn := NewNodeConn(node, conn)

	// 判断获取的链接是否是需要自动提交
	if !autoCommit { // 不是自动提交则开始一个事务
		fmt.Println("writeNodeConn.Begin()")
		if err := writeNodeConn.Begin(); err != nil {
			writeNodeConn.Close()
			this.removeWriteNodeConnByGno(gno)
			return nil, fmt.Errorf("获取可写链接失败同时执行 Begin 语句失败: %s", err.Error())
		}
	}

	// 将链接保存到本地
	this.writeNodeConnMap[gno] = writeNodeConn

	return writeNodeConn, nil
}

// 关闭链接
func (this *MySQLConnectionManager) CloseWriteConnByGno(gno int) error {
	nodeConn, ok := this.writeNodeConnMap[gno]
	if !ok {
		msg := fmt.Sprintf("将可写链接返防给连接池失败. 通过Gno:%d, 获取不到相关链接", gno)
		seelog.Errorf(msg)
		return fmt.Errorf(msg)
	}

	nodeConn.Close()

	this.removeWriteNodeConnByGno(gno)

	return nil
}

// 移除节点
func (this *MySQLConnectionManager) removeWriteNodeConnByGno(gno int) {
	delete(this.writeNodeConnMap, gno)
}

// 执行commit语句
func (this *MySQLConnectionManager) WriteConnCommit() error {
	msgs := make([]string, 0)
	for gno, nodeConn := range this.writeNodeConnMap {
		if err := nodeConn.Commit(); err != nil {
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
func (this *MySQLConnectionManager) WriteConnRollback() error {
	msgs := make([]string, 0)
	for gno, nodeConn := range this.writeNodeConnMap {
		if err := nodeConn.Rollback(); err != nil {
			msgs = append(msgs, fmt.Sprintf("组: %d, 执行 rollback 失败. %s", gno, err.Error()))
		}
	}

	if len(msgs) != 0 { // 执行rollback失败
		return fmt.Errorf(strings.Join(msgs, ". "))
	}

	// 成功回滚
	return nil
}

// 回收所有写链接
func (this *MySQLConnectionManager) WriteConnClose() error {
	if len(this.writeNodeConnMap) == 0 {
		return nil
	}

	msgs := make([]string, 0)
	for gno, nodeConn := range this.writeNodeConnMap {
		if err := nodeConn.Close(); err != nil {
			msgs = append(msgs, fmt.Sprintf("组: %d, 关闭(读写)链接失败. %s", gno, err.Error()))
		}
	}

	this.cleanWriteConn()

	if len(msgs) != 0 { // 关闭失败
		return fmt.Errorf(strings.Join(msgs, ". "))
	}

	// 成功关闭
	return nil
}

// 清空链接
func (this *MySQLConnectionManager) cleanWriteConn() {
	if len(this.writeNodeConnMap) > 0 {
		this.writeNodeConnMap = make(map[int]*NodeConn)
	}
}

/***********************************************************
 ********** 下面是对(只读)节点的一些操作 ***********************
 **********************************************************/

// 随机获取一个只读链接
func (this *MySQLConnectionManager) GetReadConnByRand() (int, *NodeConn, error) {
	// 不是事务, 或者没有写节点的情况下
	if len(this.readNodeConnMap) > 0 {
		for gno, readNodeConn := range this.readNodeConnMap {
			return gno, readNodeConn, nil
		}
	}

	// 没有缓存的可读链接则新建一个
	gno, readNode, err := this.ctx.ClusterInstance.GetReadNodeByRand()
	if err != nil {
		return gno, nil, err
	}

	readNodeConn, err := this.setAndGetReadNodeConn(gno, readNode)
	return gno, readNodeConn, err
}

// 通过组号获取
func (this *MySQLConnectionManager) GetReadConnByShard(shardNo int) (int, *NodeConn, error) {
	// 通过分片好获取组
	gno, err := this.ctx.ClusterInstance.GetGnoByShard(shardNo)
	if err != nil {
		return gno, nil, err
	}

	// 通过Gno获取链接
	nodeConn, err := this.GetReadNodeConnByGno(gno)
	if err != nil {
		return gno, nil, err
	}

	return gno, nodeConn, nil
}

// 通过Gno获取写链接
func (this *MySQLConnectionManager) GetReadNodeConnByGno(gno int) (*NodeConn, error) {
	nodeConn, ok := this.readNodeConnMap[gno]
	if ok {
		return nodeConn, nil
	}

	node, err := this.ctx.ClusterInstance.GetReadNodeByGno(gno)
	if err != nil {
		return nil, err
	}

	return this.setAndGetReadNodeConn(gno, node)
}

// 设置并且获取 只读节点
func (this *MySQLConnectionManager) setAndGetReadNodeConn(gno int, node *topo.MySQLNode) (*NodeConn, error) {
	conn, err := node.Pool.Get()
	if err != nil {
		return nil, err
	}
	readNodeConn := NewNodeConn(node, conn)

	// 将链接保存到本地
	this.readNodeConnMap[gno] = readNodeConn

	return readNodeConn, nil
}

func (this *MySQLConnectionManager) cleanReadConn() {
	if len(this.readNodeConnMap) > 0 {
		this.readNodeConnMap = make(map[int]*NodeConn)
	}
}

// 关闭只读链接
func (this *MySQLConnectionManager) CloseReadConnByGno(gno int) error {
	nodeConn, ok := this.readNodeConnMap[gno]
	if !ok {
		msg := fmt.Sprintf("将(只读)链接返防给连接池失败. 通过Gno:%d, 获取不到相关链接", gno)
		seelog.Errorf(msg)
		return fmt.Errorf(msg)
	}

	nodeConn.Close()

	this.removeReadNodeConnByGno(gno)

	return nil
}

// 回收所有写链接
func (this *MySQLConnectionManager) ReadConnClose() error {
	if len(this.readNodeConnMap) == 0 {
		return nil
	}

	msgs := make([]string, 0)
	for gno, nodeConn := range this.readNodeConnMap {
		if err := nodeConn.Close(); err != nil {
			msgs = append(msgs, fmt.Sprintf("组: %d, 关闭(只读)链接失败. %s", gno, err.Error()))
		}
	}

	this.cleanReadConn()

	if len(msgs) != 0 { // 关闭失败
		return fmt.Errorf(strings.Join(msgs, ". "))
	}

	// 成功关闭
	return nil
}

// 移除节点
func (this *MySQLConnectionManager) removeReadNodeConnByGno(gno int) {
	delete(this.readNodeConnMap, gno)
}

// 关闭所有链接
func (this *MySQLConnectionManager) Close() error {
	err1 := this.WriteConnClose()
	err2 := this.ReadConnClose()

	if err1 == nil && err2 == nil {
		return nil
	}

	var msg string
	if err1 != nil {
		msg += err1.Error()
	}
	if err2 != nil {
		msg += err2.Error()
	}

	return fmt.Errorf(msg)
}
