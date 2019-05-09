package topo

import (
	"fmt"
	"github.com/daiguadaidai/dal/mysqldb/pool"
)

type MySQLNode struct {
	DBName      string
	Host        string
	Port        uint16
	Username    string // 链接数据库的用户名
	Password    string // 链接数据库的密码
	Pool        *pool.MySQLPool
	Charset     string
	AutoCommit  bool
	ReadWeight  int // 读写分离 读权重
	Role        int8
	IsCandidate bool
	MinOpen     int32
	MaxOpen     int32
}

func NewMySQLNode(dbName string, host string, port uint16, username string, password string, charset string,
	autoCommit bool, isCandidate bool, readWeight int, role int8, minOpen int32, maxOpen int32,
) *MySQLNode {
	node := new(MySQLNode)
	node.DBName = dbName
	node.Host = host
	node.Port = port
	node.Username = username
	node.Password = password
	node.Charset = charset
	node.AutoCommit = autoCommit
	node.IsCandidate = isCandidate
	node.ReadWeight = readWeight
	node.Role = role
	node.MinOpen = minOpen
	node.MaxOpen = maxOpen

	return node
}

func (this *MySQLNode) InitPool() error {
	pool, err := pool.Open(this.Host, this.Port, this.Username, this.Password, this.DBName, this.Charset,
		this.AutoCommit, this.MinOpen, this.MaxOpen)
	if err != nil {
		return err
	}

	this.Pool = pool
	return nil
}

func (this *MySQLNode) Addr() string {
	return fmt.Sprintf("%s:%d", this.Host, this.Port)
}

func (this *MySQLNode) ResetReadWeight(weight int) {
	this.ReadWeight = weight
}

// pool 公用
func (this *MySQLNode) Clone() *MySQLNode {
	node := NewMySQLNode(this.DBName, this.Host, this.Port, this.Username, this.Password, this.Charset,
		this.AutoCommit, this.IsCandidate, this.ReadWeight, this.Role, this.MinOpen, this.MaxOpen)
	node.Pool = this.Pool

	return node
}
