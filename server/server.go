package server

import (
	"github.com/cihub/seelog"
	"github.com/daiguadaidai/dal/config"
	"github.com/daiguadaidai/dal/mysqldb"
)

func Start(cfg *config.Config) {
	defer seelog.Flush()
	logger, _ := seelog.LoggerFromConfigAsBytes([]byte(cfg.LC.Raw()))
	seelog.ReplaceLogger(logger)

	NewDal

	mysqldb.StartDal(cfg)
}
