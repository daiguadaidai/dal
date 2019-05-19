package handler

import (
	"fmt"
	"github.com/daiguadaidai/dal/dal_context"
	"github.com/daiguadaidai/dal/go-mysql/mysql"
)

type DalHandler struct {
	ctx        *dal_context.DalContext
	AutoCommit bool
	DB         string // 当前执行的数据库
}

func NewDalHadler(ctx *dal_context.DalContext) *DalHandler {
	return &DalHandler{
		ctx:        ctx,
		AutoCommit: true,
		DB:         ctx.ServerCtx.DBName,
	}
}

func (this *DalHandler) UseDB(dbName string) error {
	this.DB = dbName
	return fmt.Errorf("not supported now. UseDB")
}

func (this *DalHandler) HandleQuery(query string) (*mysql.Result, error) {
	return nil, fmt.Errorf("not supported now. HandleQuery")
}

func (this *DalHandler) HandleFieldList(table string, fieldWildcard string) ([]*mysql.Field, error) {
	return nil, fmt.Errorf("not supported now. HandleFieldList")
}

func (this *DalHandler) HandleStmtPrepare(query string) (params int, columns int, context interface{}, err error) {
	return 0, 0, nil, fmt.Errorf("not supported now. HandleStmtPrepare")
}

func (this *DalHandler) HandleStmtExecute(context interface{}, query string, args []interface{}) (*mysql.Result, error) {
	return nil, fmt.Errorf("not supported now. HandleStmtExecute")
}

func (this *DalHandler) HandleStmtClose(context interface{}) error {
	return nil
}

func (this *DalHandler) HandleOtherCommand(cmd byte, data []byte) error {
	return nil
}
