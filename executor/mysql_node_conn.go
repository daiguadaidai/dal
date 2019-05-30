package executor

import (
	"github.com/daiguadaidai/dal/go-mysql/client"
	"github.com/daiguadaidai/dal/go-mysql/mysql"
	"github.com/daiguadaidai/dal/mysqldb/topo"
)

type NodeConn struct {
	Node          *topo.MySQLNode
	Conn          *client.Conn
	inTransaction bool
	Disconnection bool
}

func NewNodeConn(node *topo.MySQLNode, conn *client.Conn) *NodeConn {
	return &NodeConn{
		Node: node,
		Conn: conn,
	}
}

func (this *NodeConn) Close() error {
	if this.inTransaction {
		if _, err := this.Rollback(); err != nil {
			return nil
		}
	}
	if this.Disconnection {
		this.Disconnection = false
		return nil
	}
	return this.Node.Pool.Release(this.Conn)
}

func (this *NodeConn) Execute(command string, args ...interface{}) (*mysql.Result, error) {
	rs, err := this.Conn.Execute(command, args...)
	if err != nil {
		if err.Error() == "connection was bad" { // 如果是链接断开将连接池中的链接减少一个
			this.Disconnection = true
			this.Node.Pool.DecrNumOpen()
		}
		return nil, err
	}

	return rs, nil
}

// 开始一个事物
func (this *NodeConn) Begin() (*mysql.Result, error) {
	if this.inTransaction {
		return nil, nil
	}

	rs, err := this.Execute("BEGIN")
	if err != nil {
		return nil, err
	}

	this.inTransaction = true

	return rs, nil
}

// commit
func (this *NodeConn) Commit() (*mysql.Result, error) {
	this.inTransaction = false
	return this.Execute("COMMIT")
}

// rollback
func (this *NodeConn) Rollback() (*mysql.Result, error) {
	this.inTransaction = false
	return this.Execute("ROLLBACK")
}
