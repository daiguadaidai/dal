package executor

import (
	"github.com/daiguadaidai/dal/go-mysql/client"
	"github.com/daiguadaidai/dal/mysqldb/topo"
)

type NodeConn struct {
	Node *topo.MySQLNode
	Conn *client.Conn
}

func (this *NodeConn) Close() error {
	return this.Node.Pool.Release(this.Conn)
}

func NewNodeConn(node *topo.MySQLNode, conn *client.Conn) *NodeConn {
	return &NodeConn{
		Node: node,
		Conn: conn,
	}
}
