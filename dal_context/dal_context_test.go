package dal_context

import (
	"fmt"
	"github.com/BurntSushi/toml"
	"github.com/daiguadaidai/dal/config"
	"math/rand"
	"testing"
)

var cfgStrWithConfig string = `
# dal配置信息
[dal]
name = "dal-server-test-from-config"
listen_host = "0.0.0.0"
listen_port = 13306
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

// 随机获取读节点, 看看度节点获取的次数
func TestDalContext_RandGetReadNodeWithConfig(t *testing.T) {
	cfg := new(config.Config)
	if _, err := toml.Decode(cfgStrWithConfig, cfg); err != nil {
		t.Fatal(err.Error())
	}

	dalCtx, err := NewDalContext(cfg)
	if err != nil {
		t.Fatal(err.Error())
	}

	readCountor := make(map[string]int)
	for i := 0; i < 1000000; i++ {
		node, err := dalCtx.ClusterInstance.GetReadNodeByShard(-1)
		if err != nil {
			t.Fatal(err.Error())
		}
		readCountor[node.Addr()]++
	}

	for key, value := range readCountor {
		fmt.Println(key, value)
	}
}

var cfgStrWithDB string = `
# dal配置信息
[dal]
name = "cluster-name-01"
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

func TestDalContext_RandGetReadNodeWithDB(t *testing.T) {
	cfg := new(config.Config)
	if _, err := toml.Decode(cfgStrWithDB, cfg); err != nil {
		t.Fatal(err.Error())
	}

	dalCtx, err := NewDalContext(cfg)
	if err != nil {
		t.Fatal(err.Error())
	}

	readCountor := make(map[string]int)
	groupCountor := make(map[int]int)
	for i := 0; i < 1000000; i++ {
		shardNo := rand.Intn(40)
		group, err := dalCtx.ClusterInstance.GetGroupByShard(shardNo)
		if err != nil {
			t.Fatal(err.Error())
		}
		groupCountor[group.GNO]++

		node, err := dalCtx.ClusterInstance.GetReadNodeByShard(shardNo)
		if err != nil {
			t.Fatal(err.Error())
		}
		readCountor[node.Addr()]++
	}

	for key, value := range readCountor {
		fmt.Println(key, value)
	}

	for key, value := range groupCountor {
		fmt.Println(key, value)
	}
}

// 打印shard table信息
func TestDalContext_PrintSharTableInstanceWithDB(t *testing.T) {
	cfg := new(config.Config)
	if _, err := toml.Decode(cfgStrWithDB, cfg); err != nil {
		t.Fatal(err.Error())
	}

	dalCtx, err := NewDalContext(cfg)
	if err != nil {
		t.Fatal(err.Error())
	}

	for i, shardTable := range dalCtx.ShardTableInstance.GetShardTables() {
		fmt.Printf("第%d个分表. Schema:%s, Table:%s, Cols:%v\n", i, shardTable.Schema, shardTable.Name, shardTable.ShardCols)
	}
}
