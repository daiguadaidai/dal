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
	ShardTableMapInstance *topo.ShardTableMapInstance
	MySQLCluster          *topo.MySQLCluster
}

// 创建dal需要使用的上下文信息
func NewDalContext(cfg *config.Config) (*DalContext, error) {
	dalContext := new(DalContext)

	// 1. 设置cluster 基本信息
	cluster, err := getCluster(cfg)
	if err != nil {
		return nil, err
	}
	dalContext.MySQLCluster = cluster

	// 2. 设置 cluster中group信息
	return dalContext, nil
}

// 获取 cluster信息
func getCluster(cfg *config.Config) (*topo.MySQLCluster, error) {
	cluster := new(topo.MySQLCluster)
	if cfg.DalConfig.IsSetDal() {
		seelog.Debugf("dal元数据信息从配置文件中获取")
		// 通过配置文件设置dal元数据信息
		setClusterFromConfig(cfg, cluster)
	} else if cfg.DalConfig.IsSetName() { // 判断是否指定了dal名称, 如果指定则从数据中获取
		seelog.Debugf("dal元数据信息从数据库中获取, dal名称:%s", cfg.DalConfig.Name)
		if err := setClusterFromDB(cfg.DalConfig.Name, cfg.MySQLMeta, cluster); err != nil {
			return nil, err
		}
	} else {
		return nil, fmt.Errorf("没有指定启动dal信息, 也没有指定启动的dal名称, 从而无法从数据库中获取到dal信息")
	}
	seelog.Infof("成功获取到dal启动信息. %s", cluster.Summary())

	return cluster, nil
}

// 从配置文件中获取dal元数据信息
func setClusterFromConfig(cfg *config.Config, cluster *topo.MySQLCluster) error {
	// 1. 设置dql基本信息
	setClusterFromDalConfig(cfg.DalConfig, cluster)

	// 2. 设置 group 信息
	if cfg.BakendMasterCount() > 1 {
		return fmt.Errorf("配置文件中有多个master请检测")
	}
	if err := setClusterGroupFromBackendConfig(cfg.Backends, cluster); err != nil {
		return err
	}

	return nil
}

// 从配置文件中设置 MySQLCluster 信息
func setClusterFromDalConfig(cfg *config.DalConfig, cluster *topo.MySQLCluster) {
	cluster.ListenHost = cfg.ListenHost
	cluster.ListenPort = cfg.ListenPort
	cluster.DBName = cfg.Database
	cluster.Username = cfg.Username
	// 设置密码
	if pwd, err := peep.Decrypt(cfg.Password); err != nil {
		seelog.Warnf("dal配置文件中dal登录密码解密失败. 使用为解密前的密码. %s", err.Error())
		cluster.Password = cfg.Password
	} else {
		cluster.Password = pwd
	}
	cluster.Name = cfg.Name
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

	return nil
}

// 从数据库中获取cluster信息并且赋值给 MySQLCluster
func setClusterFromDB(clusterName string, dbConfig *config.MySQLConfig, cluster *topo.MySQLCluster) error {
	clusterDao, err := dao.NewClusterDao(dbConfig)
	if err != nil {
		return err
	}
	defer clusterDao.Close()

	mCluster, err := clusterDao.GetClusterByName(clusterName)
	if err != nil {
		return err
	}
	cluster.ListenHost = mCluster.ListenHost
	cluster.ListenPort = mCluster.ListenPort
	cluster.DBName = mCluster.DBName
	cluster.Username = mCluster.Username
	// 设置密码
	if pwd, err := peep.Decrypt(mCluster.Password); err != nil {
		seelog.Warnf("dal配置文件中dal登录密码解密失败. 使用为解密前的密码. %s", err.Error())
		cluster.Password = mCluster.Password
	} else {
		cluster.Password = pwd
	}
	cluster.Name = mCluster.Name

	return nil
}
