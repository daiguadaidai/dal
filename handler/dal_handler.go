package handler

import (
	"fmt"
	"github.com/daiguadaidai/dal/dal_context"
	"github.com/daiguadaidai/dal/executor"
	"github.com/daiguadaidai/dal/go-mysql/mysql"
	"strings"
)

const (
	RAND_GNO = -1
)

type DalHandler struct {
	ctx   *dal_context.DalContext
	mExec *executor.MySQLExecutor
}

func NewDalHadler(ctx *dal_context.DalContext) *DalHandler {
	return &DalHandler{
		ctx:   ctx,
		mExec: executor.NewMySQLExecutor(ctx),
	}
}

func (this *DalHandler) UseDB(dbName string) error {
	return this.mExec.UseDB(&dbName)
}

func (this *DalHandler) HandleQuery(query string) (*mysql.Result, error) {
	fmt.Println(query)
	// 将带有百分号替换成想 两个百分号
	newQuery := strings.ReplaceAll(query, "%", "%%")
	fmt.Println(newQuery)
	return this.mExec.HandleQuery(&newQuery)
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
	return fmt.Errorf("not supported now. HandleStmtClose")
}

func (this *DalHandler) HandleOtherCommand(cmd byte, data []byte) error {
	return fmt.Errorf("not supported now. HandleOtherCommand")
}

// 清理dal相关资源
func (this *DalHandler) Clean() error {
	if err := this.mExec.Clean(); err != nil {
		return err
	}

	return nil
}
