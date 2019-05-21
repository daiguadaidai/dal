package server

import (
	"github.com/BurntSushi/toml"
	"github.com/daiguadaidai/dal/config"
	"github.com/daiguadaidai/dal/dal_context"
	"testing"
)

var cfgStrWithDB string = `
# dal配置信息
[dal]
name = "cluster-name-02"
listen_host = ""
listen_port = 0
username = "root"
password = "123456"
database = "test"
shard_table_instance_num = 8 # 存放分表信息的资源个数, 思想来源于MySQL 多instance
cluster_instance_num = 8 # cluster相关资源个数, 思想来源于MySQL 多instance

# 直接指定后端需要链接的数据库
[[backend]]
username = "HH"
password = "oracle12"
host = "127.0.0.1"
port = 3306
database = "employees"
charset = "utf8mb4"
auto_commit = true # true/false
role = 1 # 角色 1.master, 2.slave
is_candidate = true # 是否是候选master true/flase
read_weight = 1 # 读权重
min_open = 10
max_open = 10

# 直接指定后端需要链接的数据库
[[backend]]
username = "HH"
password = "oracle12"
host = "localhost"
port = 3306
database = "employees"
charset = "utf8mb4"
auto_commit = true
role = 2 # 角色 1.master, 2.slave
is_candidate = true  # 是否是候选master
read_weight = 3 # 读权重
min_open = 10
max_open = 10

# 后端链接的数据库信息从mysql里面获取, 如果有指定 [mysql_target] 则以 [mysql_target] 为准
[mysql_meta]
username = "HH"
password = "oracle12"
host = "127.0.0.1"
port = 3306
database = "dal"
auto_commit = true
timeout = 10
charset="utf8mb4"
max_open_conns=10
max_idel_conns=10

[log]
# path="/data0/dal/logs"    # 日志路径, 没有填写打印到控制台
level="debug"
file_size=1073741824    # 每个日志文件的大小
file_keep_size=10    # 历史日志文件保存个数
`

// 打印shard table信息
func TestDalServer_Start(t *testing.T) {
	cfg := new(config.Config)
	if _, err := toml.Decode(cfgStrWithDB, cfg); err != nil {
		t.Fatal(err.Error())
	}

	dalCtx, err := dal_context.NewDalContext(cfg)
	if err != nil {
		t.Fatal(err.Error())
	}

	StartDal(dalCtx)
}
