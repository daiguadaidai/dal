package config

import "fmt"

const (
	SHARD_TABLE_INSTANCE_NUM = 1
	CLUSTER_INSTANCE_NUM     = 1
)

type DalConfig struct {
	Name                  string `toml:"name"`
	ListenHost            string `toml:"listen_host"`
	ListenPort            int    `toml:"listen_port"`
	Username              string `toml:"username"`
	Password              string `toml:"password"`
	Database              string `toml:"database"`
	ShardTableInstanceNum int    `toml:"shard_table_instance_num"`
	ClusterInstanceNum    int    `toml:"cluster_instance_num"`
}

func (this *DalConfig) SupDefault() {
	if this.ShardTableInstanceNum < 1 {
		this.ShardTableInstanceNum = SHARD_TABLE_INSTANCE_NUM
	}
	if this.ClusterInstanceNum < 1 {
		this.ClusterInstanceNum = CLUSTER_INSTANCE_NUM
	}
}

// tostring 方法
func (this *DalConfig) String() string {
	return fmt.Sprintf("{Name:%s, ListenHost:%s, ListenPort:%d, Username:%s, Password:******, Database:%s}",
		this.Name, this.ListenHost, this.ListenPort, this.Username, this.Database)
}

// dal socket 地址
func (this *DalConfig) Addr() string {
	return fmt.Sprintf("%s:%d", this.ListenHost, this.ListenPort)
}

// 是否设置了dal信息
func (this *DalConfig) IsSetDal() bool {
	return len(this.ListenHost) > 0 && this.ListenPort > 0
}

// 是否提供了dal名称
func (this *DalConfig) IsSetName() bool {
	return len(this.Name) > 0
}
