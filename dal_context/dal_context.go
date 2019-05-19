package dal_context

import (
	"fmt"
	"github.com/cihub/seelog"
	"github.com/daiguadaidai/dal/config"
	"github.com/daiguadaidai/dal/dao"
	"github.com/daiguadaidai/dal/mysqldb/topo"
	"github.com/daiguadaidai/peep"
	"strings"
)

type DalContext struct {
	Cfg                *config.Config
	ServerCtx          *ServerContext
	ShardTableInstance *topo.ShardTableMapInstance
	ClusterInstance    *topo.ClusterInstance
}

// 创建dal需要使用的上下文信息
func NewDalContext(cfg *config.Config) (*DalContext, error) {
	dalContext := new(DalContext)

	// 1. 设置配置信息
	dalContext.Cfg = cfg

	// 2. 获取server Context
	serverCtx, err := getServerContext(cfg)
	if err != nil {
		return nil, err
	}
	dalContext.ServerCtx = serverCtx
	seelog.Infof("成功获取到dal启动信息. %s", serverCtx.Summary())

	// 3. 设置cluster instance
	clusterInstance, err := getClusterInstance(cfg)
	if err != nil {
		return nil, err
	}
	dalContext.ClusterInstance = clusterInstance
	seelog.Info("成功获取到dal集群信息")

	// 4. 设置 shardTable instance
	shardTableInstance, err := getShardTableInstance(cfg)
	if err != nil {
		return nil, err
	}
	dalContext.ShardTableInstance = shardTableInstance
	seelog.Info("成功获取分表信息")

	return dalContext, nil
}

// 获取 cluster instance信息
func getClusterInstance(cfg *config.Config) (*topo.ClusterInstance, error) {
	cluster := topo.DefaultMySQLCluster()
	if cfg.DalConfig.IsSetDal() {
		seelog.Debugf("集群信息从dal配置文件中获取")
		// 通过配置文件设置dal元数据信息
		if err := setClusterFromConfig(cfg, cluster); err != nil {
			return nil, err
		}
	} else if cfg.DalConfig.IsSetName() { // 判断是否执行了dal名称, 是则从数据库中获取集群信息
		seelog.Debugf("集群信息从数据库中获取")
		if err := setClusterFromDB(cfg.DalConfig.Name, cfg.MySQLMeta, cluster); err != nil {
			return nil, err
		}
	} else {
		return nil, fmt.Errorf("没有指定启动dal信息, 也没有指定启动的dal名称, 从而无法从数据库中获取到集群信息")
	}
	// cluster 重新设置 分片好对应哪个group
	cluster.InitShardGroup()

	// 创建cluster instance
	clusterInstance := topo.NewClusterInstance(cfg.DalConfig.ClusterInstanceNum, cluster)

	return clusterInstance, nil
}

// 从配置文件中获取dal元数据信息
func setClusterFromConfig(cfg *config.Config, cluster *topo.MySQLCluster) error {
	// 1. 设置 group 信息
	if cfg.BakendMasterCount() > 1 {
		return fmt.Errorf("配置文件中有多个master请检测")
	}
	if err := setClusterGroupFromBackendConfig(cfg.Backends, cluster); err != nil {
		return err
	}

	return nil
}

// 设置group, 通过配置信息
func setClusterGroupFromBackendConfig(cfgs []*config.BackendConfig, cluster *topo.MySQLCluster) error {
	group := topo.NewMySQLGroup("", 0)
	dbNames := make([]string, 0)
	var beforeDBName string
	for _, backend := range cfgs {
		var password string
		// 设置密码
		if pwd, err := peep.Decrypt(backend.Password); err != nil {
			seelog.Warnf("后端链接数据库[%s]密码解密失败. 使用为解密前的密码. %s", backend.Addr(), err.Error())
			password = backend.Password
		} else {
			password = pwd
		}

		node := topo.NewMySQLNode(backend.Database, backend.Host, backend.Port, backend.Username, password, backend.Charset,
			backend.AutoCommit, backend.IsCandidate, backend.ReadWeight, backend.Role, backend.MinOpen, backend.MaxOpen)
		if err := node.InitPool(); err != nil {
			return err
		}
		// 将节点添加到group中
		if err := group.AddNode(node); err != nil {
			return err
		}

		// 添加dbName
		if beforeDBName != backend.Database {
			dbNames = append(dbNames, backend.Database)
			beforeDBName = backend.Database
		}
	}
	// 设置group 中的dbname group中的dbname可能会显示多个不主要是应为node中可能会有db不一样. DNName: db1, db2, db3
	group.DBName = strings.Join(dbNames, ", ")

	cluster.AddGroup(group)

	return nil
}

// 从数据库中获取cluster信息
func setClusterFromDB(serverName string, dbConfig *config.MySQLConfig, cluster *topo.MySQLCluster) error {
	// 设置cluster group信息
	if err := setClusterGroupFromDB(dbConfig, cluster, serverName); err != nil {
		return err
	}

	return nil
}

// 从数据库中获取group信息设置到cluster中
func setClusterGroupFromDB(dbConfig *config.MySQLConfig, cluster *topo.MySQLCluster, serverName string) error {
	// 获取数据库中的group元数据信息
	mGroups, err := dao.NewGroupDao(dbConfig).FindByServerName(serverName)
	if err != nil {
		return fmt.Errorf("从数据库获取group元数据失败. %s", err)
	}
	if len(mGroups) == 0 {
		return fmt.Errorf("从数据库中没有group元数据")
	}

	// 获取数据库中的所有Node信息
	mNodes, err := dao.NewNodeDao(dbConfig).FindByServerName(serverName)
	if err != nil {
		return fmt.Errorf("初始化node元数据失败. %s", err)
	}
	if len(mNodes) == 0 {
		return fmt.Errorf("从数据库中没有node元数据")
	}

	// 创建group和node并设置到 cluster中
	// 创建 临时groupmap
	groupMap := make(map[int64]*topo.MySQLGroup)
	for _, group := range mGroups {
		mysqlGroup := topo.NewMySQLGroup(group.DBName, group.GNO)
		mysqlGroup.SetShardNoMapByStr(group.Shards)
		groupMap[group.ID] = mysqlGroup
	}

	// 循环node设置group node信息
	for _, mNode := range mNodes {
		group, ok := groupMap[mNode.GroupID]
		if !ok {
			return fmt.Errorf("node没有对应的group: {id:%d, groupID:%d, host:%s, port:%d}", mNode.ID, mNode.GroupID, mNode.Host, mNode.Port)
		}

		var password string
		// 设置密码
		if pwd, err := peep.Decrypt(mNode.Password); err != nil {
			seelog.Warnf("后端链接数据库[%s:%d]密码解密失败. 使用为解密前的密码. %s", mNode.Host, mNode.Port, err.Error())
			password = mNode.Password
		} else {
			password = pwd
		}

		node := topo.NewMySQLNode(mNode.DBName, mNode.Host, mNode.Port, mNode.Username, password, mNode.Charset,
			mNode.AutoCommit, mNode.IsCandidate, mNode.ReadWeight, mNode.Role, mNode.MinOpen, mNode.MaxOpen)
		if err := node.InitPool(); err != nil {
			return err
		}
		// 将节点添加到group中
		if err := group.AddNode(node); err != nil {
			return err
		}
	}

	// 循环groupMap添加到cluster中
	for _, group := range groupMap {
		cluster.AddGroup(group)
	}

	return nil
}

// 获取服务的context
func getServerContext(cfg *config.Config) (*ServerContext, error) {
	if cfg.DalConfig.IsSetDal() {
		seelog.Debugf("server context 信息从配置信息中获取")
		return getServerContextFromDalConfig(cfg.DalConfig), nil
	} else if cfg.DalConfig.IsSetName() { // 判断是否指定了dal名称, 如果指定则从数据中获取
		seelog.Debugf("dal元数据信息从数据库中获取, dal名称:%s", cfg.DalConfig.Name)
		return getServerContextFromDB(cfg.MySQLMeta, cfg.DalConfig.Name)
	}

	return nil, fmt.Errorf("没有指定启动dal信息, 也没有指定启动的dal名称, 从而无法从数据库中获取到dal信息")
}

// 从配置文件中获取dal信息
func getServerContextFromDalConfig(cfg *config.DalConfig) *ServerContext {
	var password string
	// 设置密码
	if pwd, err := peep.Decrypt(cfg.Password); err != nil {
		seelog.Warnf("dal服务密码解密失败. 使用为解密前的密码. %s", err.Error())
		password = cfg.Password
	} else {
		password = pwd
	}

	return NewServerContext(cfg.Name, cfg.ListenHost, cfg.ListenPort, cfg.Username, password, cfg.Database,
		cfg.ShardTableInstanceNum, cfg.ClusterInstanceNum)
}

// 从数据库中获取dal信息
func getServerContextFromDB(dbConfig *config.MySQLConfig, serverName string) (*ServerContext, error) {
	server, err := dao.NewServerDao(dbConfig).GetServerByName(serverName)
	if err != nil {
		return nil, err
	}

	var password string
	// 设置密码
	if pwd, err := peep.Decrypt(server.Password); err != nil {
		seelog.Warnf("dal服务密码解密失败. 使用为解密前的密码. %s", err.Error())
		password = server.Password
	} else {
		password = pwd
	}

	return NewServerContext(server.Name, server.ListenHost, server.ListenPort, server.Username, password, server.DBName,
		server.ShardTableInstanceNum, server.ClusterInstanceNum), nil
}

// 获取 shard table instance
func getShardTableInstance(cfg *config.Config) (*topo.ShardTableMapInstance, error) {
	if cfg.DalConfig.IsSetDal() {
		seelog.Debugf("元信息是从配置文件中获取, 因此不可能会有分表信息")
	} else if cfg.DalConfig.IsSetName() { // 判断是否执行了dal名称, 是则从数据库中获取集群信息
		seelog.Debugf("分表信息从数据库中获取")
		return getShardTableInstanceFromDB(cfg.DalConfig.Name, cfg.DalConfig.ShardTableInstanceNum, cfg.MySQLMeta)
	}

	return topo.NewShardTableMapInstance(cfg.DalConfig.ShardTableInstanceNum), nil
}

func getShardTableInstanceFromDB(name string, instanceNum int, dbConfig *config.MySQLConfig) (*topo.ShardTableMapInstance, error) {
	shardTableInstance := topo.NewShardTableMapInstance(instanceNum)

	shardTables, err := dao.NewShardTableDao(dbConfig).FindByServerName(name)
	if err != nil {
		return nil, err
	}

	for _, shardTable := range shardTables {
		if err1 := shardTableInstance.AddShardTable(shardTable.DBName, shardTable.Name, shardTable.Columns()...); err != nil {
			return nil, err1
		}
	}

	return shardTableInstance, nil
}
