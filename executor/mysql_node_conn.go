package executor

import (
	"github.com/daiguadaidai/dal/go-mysql/client"
	"github.com/daiguadaidai/dal/go-mysql/mysql"
	"github.com/daiguadaidai/dal/mysqldb/topo"
)

type NodeConn struct {
	Node          *topo.MySQLNode
	Conn          *client.Conn
	Disconnection bool
}

func NewNodeConn(node *topo.MySQLNode, conn *client.Conn) *NodeConn {
	return &NodeConn{
		Node: node,
		Conn: conn,
	}
}

// 重新初始化当前链接的DB
func (this *NodeConn) ReInitUseDB(db string) error {
	// 在没有指定需要初始化到哪个DB的情况下, 则初始化成和启动dal服务最开始的数据库
	if db == "" {
		// 链接当前的DB和最初的一样则不需要 use DB
		if this.Node.DBName == this.Conn.GetDB() {
			return nil
		}

		// 链接当前的DB 和 最初的不一样, 则初始化成最初的
		return this.UseDB(this.Node.DBName)
	}

	// 需要指定的 DB 和当前链接的一样, 不需要 use db
	if db == this.Conn.GetDB() {
		return nil
	}

	// 需要指定的和当前链接不一样, 使用 use db
	return this.UseDB(db)
}

func (this *NodeConn) Close() error {
	if this.IsInTransaction() {
		if err := this.Rollback(); err != nil {
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
func (this *NodeConn) Begin() error {
	if this.IsInTransaction() {
		return nil
	}

	if err := this.Conn.Begin(); err != nil {
		return err
	}

	return nil
}

// commit
func (this *NodeConn) Commit() error {
	return this.Conn.Commit()
}

// rollback
func (this *NodeConn) Rollback() error {
	return this.Conn.Rollback()
}

// UseDB
func (this *NodeConn) UseDB(db string) error {
	return this.Conn.UseDB(db)
}

// GetDB
func (this *NodeConn) GetDB() string {
	return this.Conn.GetDB()
}

// IsIntransaction
func (this *NodeConn) IsInTransaction() bool {
	return this.Conn.IsInTransaction()
}
