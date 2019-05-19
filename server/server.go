package server

import (
	"github.com/cihub/seelog"
	"github.com/daiguadaidai/dal/config"
	"github.com/daiguadaidai/dal/dal_context"
	"syscall"
)

func Start(cfg *config.Config) {
	defer seelog.Flush()
	logger, _ := seelog.LoggerFromConfigAsBytes([]byte(cfg.LC.Raw()))
	seelog.ReplaceLogger(logger)

	_, err := dal_context.NewDalContext(cfg)
	if err != nil {
		seelog.Errorf("获取dal context失败, 程序退出. %s", err.Error())
		syscall.Exit(1)
	}
}
