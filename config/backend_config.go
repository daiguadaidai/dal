package config

import "fmt"

type BackendConfig struct {
	Username    string `toml:"username"`
	Password    string `toml:"password"`
	Database    string `toml:"database"`
	Charset     string `toml:"charset"`
	Host        string `toml:"host"`
	Timeout     int    `toml:"timeout"`
	Port        uint16 `toml:"port"`
	ReadWeight  int    `toml:"read_weight"` // 读写分离 读权重
	Role        int8   `toml:"role"`
	AutoCommit  bool   `toml:"auto_commit"`
	IsCandidate bool   `toml:"is_candidate"`
	MinOpen     int32  `toml:"min_open"`
	MaxOpen     int32  `toml:"max_open"`
}

func (this *BackendConfig) Addr() string {
	return fmt.Sprintf("%s:%d", this.Host, this.Port)
}
