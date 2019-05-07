package dal_context

import (
	"fmt"
	"github.com/cihub/seelog"
	"github.com/daiguadaidai/dal/config"
	"github.com/daiguadaidai/dal/mysqldb/topo"
	"github.com/daiguadaidai/peep"
)

type DalContext struct {
	ShardTableMapInstance *topo.ShardTableMapInstance
	MySQLCluster          *topo.MySQLCluster
}

// 创建dal需要使用的上下文信息
func NewDalContext(cfg *config.Config) (*DalContext, error) {
	dalContext := new(DalContext)

	// 设置cluster 基本信息
	// dal有执行对外服务的 host port, 则直接使用, 否则从数据库中获取
	cluster := new(topo.MySQLCluster)
	if cfg.DalConfig.IsSetDal() {
		seelog.Debugf("dal启动信息从配置文件中获取")
		cluster.ListenHost = cfg.DalConfig.ListenHost
		cluster.ListenPort = cfg.DalConfig.ListenPort
		cluster.Username = cfg.DalConfig.Username
		// 设置密码
		if pwd, err := peep.Decrypt(cfg.DalConfig.Password); err != nil {
			seelog.Warnf("dal配置文件中dal登录密码解密失败. 使用为解密前的密码. %s", err.Error())
			cluster.Password = cfg.DalConfig.Password
		} else {
			cluster.Password = pwd
		}
		cluster.Name = cfg.DalConfig.Name
	} else if cfg.DalConfig.IsSetName() { // 判断是否指定了dal名称, 如果指定则从数据中获取
		seelog.Debugf("dal启动信息从数据库中获取, dal名称:%s", cfg.DalConfig.Name)
		if err := GetClusterFromDB(cfg.DalConfig.Name, cfg.MySQLMeta, cluster); err != nil {
			return nil, fmt.Errorf("从数据库中获取dal信息失败. %s", err.Error())
		}
	} else {
		return nil, fmt.Errorf("没有指定启动dal信息, 也没有指定启动的dal名称, 从而无法从数据库中获取到dal信息")
	}
	dalContext.MySQLCluster = cluster
	seelog.Infof("成功获取到dal启动信息. %s", cluster.Summary())

	return dalContext, nil
}

func GetClusterFromDB(clusterName string, dbConfig *config.MySQLConfig, cluster *topo.MySQLCluster) error {

	return nil
}
