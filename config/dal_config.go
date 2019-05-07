package config

import "fmt"

type DalConfig struct {
	Name       string `toml:"name"`
	ListenHost string `toml:"listen_host"`
	ListenPort int    `toml:"listen_port"`
	Username   string `toml:"username"`
	Password   string `toml:"password"`
	Database   string `toml:"database"`
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
