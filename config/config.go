package config

import (
	"github.com/BurntSushi/toml"
)

const (
	CONFIG_FILE_PATH = "./dal.toml"
)

type Config struct {
	DalConfig    *DalConfig   `toml:"dal"`
	BackendMySQL *MySQLConfig `toml:"backend_mysql"`
	MySQLMeta    *MySQLConfig `toml:"mysql_meta"`
	LC           *LogConfig   `toml:"log"`
}

var cfg Config

func NewConfig(fPath string) (*Config, error) {
	if _, err := toml.DecodeFile(fPath, &cfg); err != nil {
		return nil, err
	}

	cfg.LC.SupDefault() // 补充日志配置文件默认值

	return &cfg, nil
}

// 是否配置文件总指定后端MySQL
func (this *Config) IsSetBackend() bool {
	return len(this.BackendMySQL.Host) > 0 && this.BackendMySQL.Port > 0 && len(this.BackendMySQL.Username) > 0
}
