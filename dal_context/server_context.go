package dal_context

import "fmt"

type ServerContext struct {
	Name                  string
	ListenHost            string
	ListenPort            int
	Username              string
	Password              string
	DBName                string
	ShardTableInstanceNum int
	ClusterInstanceNum    int
}

func NewServerContext(
	name string,
	listenHost string,
	listenPort int,
	username string,
	password string,
	dbName string,
	shardTableInstanceNum int,
	clusterInstanceNum int,
) *ServerContext {
	return &ServerContext{
		Name:                  name,
		ListenHost:            listenHost,
		ListenPort:            listenPort,
		Username:              username,
		Password:              password,
		DBName:                dbName,
		ShardTableInstanceNum: shardTableInstanceNum,
		ClusterInstanceNum:    clusterInstanceNum,
	}
}

func (this *ServerContext) Summary() string {
	return fmt.Sprintf("{Name:%s, ListenHost:%s, ListenPort:%d, Username:%s, Password:******, Database:%s, ShardTableInstanceNum:%d, ClusterInstanceNum:%d}",
		this.Name, this.ListenHost, this.ListenPort, this.Username, this.DBName, this.ShardTableInstanceNum,
		this.ClusterInstanceNum)
}

func (this *ServerContext) Addr() string {
	return fmt.Sprintf("%s:%d", this.ListenHost, this.ListenPort)
}
