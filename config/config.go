package config

import (
	"github.com/BurntSushi/toml"
	"github.com/daiguadaidai/dal/utils/types"
)

const (
	CONFIG_FILE_PATH = "./dal.toml"
)

type Config struct {
	DalConfig *DalConfig       `toml:"dal"`
	Backends  []*BackendConfig `toml:"backend"`
	MySQLMeta *MySQLConfig     `toml:"mysql_meta"`
	LC        *LogConfig       `toml:"log"`
}

var cfg Config

func NewConfig(fPath string) (*Config, error) {
	if _, err := toml.DecodeFile(fPath, &cfg); err != nil {
		return nil, err
	}

	cfg.DalConfig.SupDefault() // 补充dal配置文件信息
	cfg.LC.SupDefault()        // 补充日志配置文件默认值

	return &cfg, nil
}

// 获取master个数
func (this *Config) BakendMasterCount() int {
	var count int
	for _, backend := range this.Backends {
		if backend.Role == types.MYSQL_ROLE_MASTER {
			count++
		}
	}

	return count
}
