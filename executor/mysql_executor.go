package executor

import (
	"fmt"
	"github.com/cihub/seelog"
	"github.com/daiguadaidai/dal/dal_context"
	"github.com/daiguadaidai/dal/go-mysql/mysql"
	"github.com/daiguadaidai/dal/utils"
	"github.com/daiguadaidai/dal/visitor"
	"github.com/daiguadaidai/parser"
	"github.com/daiguadaidai/parser/ast"
	"github.com/daiguadaidai/parser/format"
	driver "github.com/daiguadaidai/tidb/types/parser_driver"
	"strings"
)

type MySQLExecutor struct {
	ctx           *dal_context.DalContext
	connMgr       *MySQLConnectionManager
	AutoCommit    bool
	DB            string // 当前执行的数据库
	Charset       string // 当前使用的字符集
	Collation     string // 当前collation
	InTransaction bool   // 是否在事务中
}

func NewMySQLExecutor(ctx *dal_context.DalContext) *MySQLExecutor {
	return &MySQLExecutor{
		AutoCommit: true,
		ctx:        ctx,
		connMgr:    NewMySQLConnectionManager(ctx),
	}
}

// 清理执行器中的资源
func (this *MySQLExecutor) Clean() error {
	if this.InTransaction {
		if err := this.connMgr.WriteConnRollback(); err != nil {
			seelog.Error(err.Error())
		}
	}

	return this.connMgr.Close()
}

func (this *MySQLExecutor) setIntransaction() {
	if this.InTransaction { // 已经在事务中了
		return
	}

	// 非 自动提交的情况下 设置为在事务中
	if !this.AutoCommit {
		this.InTransaction = true
	}
}

// 执行 shard 的 SELECT 类型语句
func (this *MySQLExecutor) executeShardQDL(query *string, shardNo int) (*mysql.Result, error) {
	var gno int
	var nodeConn *NodeConn
	var err error

	if this.InTransaction {
		// 如果在事务中则代表是非自动提交
		gno, nodeConn, err = this.connMgr.GetWriteConnByShard(shardNo, false)
		if err != nil {
			return nil, err
		}
	} else {
		gno, nodeConn, err = this.connMgr.GetReadConnByShard(shardNo)
		if err != nil {
			return nil, err
		}
		defer this.connMgr.CloseReadConnByGno(gno)
	}
	if err = nodeConn.ReInitUseDB(this.DB); err != nil {
		return nil, err
	}

	return nodeConn.Execute(*query)
}

// 实行SELECT类型语句
func (this *MySQLExecutor) executeQDL(query *string) (*mysql.Result, error) {
	var gno int
	var nodeConn *NodeConn
	var err error

	if this.InTransaction {
		// 如果在事务中则代表是非自动提交
		gno, nodeConn, err = this.connMgr.GetWriteConnByRand(false)
		if err != nil {
			return nil, err
		}
	} else {
		gno, nodeConn, err = this.connMgr.GetReadConnByRand()
		if err != nil {
			return nil, err
		}
		defer this.connMgr.CloseReadConnByGno(gno)
	}

	if err = nodeConn.ReInitUseDB(this.DB); err != nil {
		return nil, err
	}

	return nodeConn.Execute(*query)
}

// 指定分库分表DML语句
func (this *MySQLExecutor) executeShardDML(query *string, shardNo int) (*mysql.Result, error) {
	var gno int
	var nodeConn *NodeConn
	var err error

	if this.InTransaction {
		// 如果在事务中则代表是非自动提交
		gno, nodeConn, err = this.connMgr.GetWriteConnByShard(shardNo, false)
		if err != nil {
			return nil, err
		}
	} else {
		gno, nodeConn, err = this.connMgr.GetWriteConnByShard(shardNo, true)
		if err != nil {
			return nil, err
		}
		defer this.connMgr.CloseWriteConnByGno(gno)
	}

	if err = nodeConn.ReInitUseDB(this.DB); err != nil {
		return nil, err
	}

	return nodeConn.Execute(*query)
}

// 指定非分库分表 DML 语句
func (this *MySQLExecutor) executeDML(query *string) (*mysql.Result, error) {
	var gno int
	var nodeConn *NodeConn
	var err error

	if this.InTransaction {
		gno, nodeConn, err = this.connMgr.GetWriteConnByRand(false)
		if err != nil {
			return nil, err
		}
	} else {
		gno, nodeConn, err = this.connMgr.GetWriteConnByRand(true)
		if err != nil {
			return nil, err
		}
		defer this.connMgr.CloseWriteConnByGno(gno)
	}

	if err = nodeConn.ReInitUseDB(this.DB); err != nil {
		return nil, err
	}

	return nodeConn.Execute(*query)
}

// 处理SQL语句
func (this *MySQLExecutor) HandleQuery(query *string) (*mysql.Result, error) {
	ps := parser.New()
	stmtNode, err := ps.ParseOneStmt(*query, this.Charset, this.Collation)
	if err != nil {
		return nil, mysql.NewError(mysql.ER_SYNTAX_ERROR, fmt.Sprintf("%s. %s", mysql.ErrorMsg(mysql.ER_SYNTAX_ERROR), err.Error()))
	}

	switch stmt := stmtNode.(type) {
	case *ast.CreateDatabaseStmt:
		return nil, fmt.Errorf("Error: 不支持(创建)数据库, CreateDatabaseStmt. %s", *query)
	case *ast.DropDatabaseStmt:
		return nil, fmt.Errorf("Error: 不支持(删除)数据库, DropDatabaseStmt. %s", *query)
	case *ast.CreateTableStmt:
		return nil, fmt.Errorf("Error: 不支持(创建)表, CreateTableStmt. %s", *query)
	case *ast.DropTableStmt:
		return nil, fmt.Errorf("Error: 不支持(删除)表, DropTableStmt. %s", *query)
	case *ast.RenameTableStmt:
		return nil, fmt.Errorf("Error: 不支持(重命名)表, RenameTableStmt. %s", *query)
	case *ast.CreateViewStmt:
		return nil, fmt.Errorf("Error: 不支持(创建)视图, CreateViewStmt. %s", *query)
	case *ast.CreateIndexStmt:
		return nil, fmt.Errorf("Error: 不支持(创建)索引, CreateIndexStmt. %s", *query)
	case *ast.DropIndexStmt:
		return nil, fmt.Errorf("Error: 不支持(删除)索引, DropIndexStmt. %s", *query)
	case *ast.AlterTableStmt:
		return nil, fmt.Errorf("Error: 不支持(修改)表, AlterTableStmt. %s", *query)
	case *ast.TruncateTableStmt:
		return nil, fmt.Errorf("Error: 不支持(清空)表, TruncateTableStmt. %s", *query)
	case *ast.SelectStmt:
		return this.doSelectStmt(query, stmt)
	case *ast.UnionStmt:
		return nil, fmt.Errorf("Error: 不支持(Union)类型语句, UnionStmt. %s", *query)
	case *ast.LoadDataStmt:
		return nil, fmt.Errorf("Error: 不支持(Load)加载数据, LoadDataStmt. %s", *query)
	case *ast.InsertStmt:
		this.setIntransaction() // 设置在事务中
		return this.doInsertStmt(query, stmt)
	case *ast.DeleteStmt:
		this.setIntransaction() // 设置在事务中
		return this.doDeleteStmt(query, stmt)
	case *ast.UpdateStmt:
		this.setIntransaction() // 设置在事务中
		return this.doUpdateStmt(query, stmt)
	case *ast.ShowStmt:
		return nil, fmt.Errorf("Error: 不支持(show)操作, ShowStmt. %s", *query)
	case *ast.TraceStmt:
		return nil, fmt.Errorf("Error: 不支持(trace)操作, TraceStmt. %s", *query)
	case *ast.ExplainStmt:
		return nil, fmt.Errorf("Error: 不支持(explain)操作, ExplainStmt. %s", *query)
	case *ast.PrepareStmt:
		return nil, fmt.Errorf("Error: PrepareStmt")
	case *ast.DeallocateStmt:
		return nil, fmt.Errorf("Error: 不支持(重新分配)操作, DeallocateStmt. %s", *query)
	case *ast.ExecuteStmt:
		return nil, fmt.Errorf("Error: 不支持(Execute)操作, ExecuteStmt. %s", *query)
	case *ast.BeginStmt:
		return this.doBegin(query, stmt)
	case *ast.BinlogStmt:
		return nil, fmt.Errorf("Error: 不支持(binlog)操作, BinlogStmt. %s", *query)
	case *ast.CommitStmt:
		return this.doCommitStmt(query, stmt)
	case *ast.RollbackStmt:
		return this.doRollbackStmt(query, stmt)
	case *ast.UseStmt:
		return nil, this.UseDB(&stmt.DBName)
	case *ast.FlushStmt:
		return nil, fmt.Errorf("Error: 不支持(flush)操作, FlushStmt. %s", *query)
	case *ast.KillStmt:
		return nil, fmt.Errorf("Error: 不支持(kill)操作, KillStmt. %s", *query)
	case *ast.SetStmt:
		return this.doSetStmt(query, stmt)
	case *ast.SetPwdStmt:
		return nil, fmt.Errorf("Error: 不支持(set password)操作, SetPwdStmt. %s", *query)
	case *ast.CreateUserStmt:
		return nil, fmt.Errorf("Error: 不支持(创建)用户, CreateUserStmt. %s", *query)
	case *ast.AlterUserStmt:
		return nil, fmt.Errorf("Error: 不支持(修改)用户, AlterUserStmt. %s", *query)
	case *ast.DropUserStmt:
		return nil, fmt.Errorf("Error: 不支持(删除)用户, DropUserStmt. %s", *query)
	case *ast.DoStmt:
		return nil, fmt.Errorf("Error: 不支持(do)操作, DoStmt. %s", *query)
	case *ast.AdminStmt:
		return nil, fmt.Errorf("Error: 不支持(管理)语句, AdminStmt. %s", *query)
	case *ast.RevokeStmt:
		return nil, fmt.Errorf("Error: 不支持(revoke)操作, RevokeStmt. %s", *query)
	case *ast.GrantStmt:
		return nil, fmt.Errorf("Error: 不支持(grant)操作, GrantStmt. %s", *query)
	case *ast.AnalyzeTableStmt:
		return nil, fmt.Errorf("Error: 不支持(分析)表, AnalyzeTableStmt. %s", *query)
	case *ast.DropStatsStmt:
		return nil, fmt.Errorf("Error: 不支持(drop status)操作, DropStatsStmt. %s", *query)
	case *ast.LoadStatsStmt:
		return nil, fmt.Errorf("Error: 不支持(load status)操作, LoadStatsStmt. %s", *query)
	}

	return nil, fmt.Errorf("Error: 未知SQL类型. %s")
}

// use db 语句
func (this *MySQLExecutor) UseDB(dbName *string) error {
	// 如果当前已经是 use db, 不需要链接数据库操作
	if strings.TrimSpace(*dbName) == "" {
		return mysql.NewDefaultError(mysql.ER_NO_DB_ERROR)
	} else if strings.TrimSpace(*dbName) == "__$$__init__$$__" {
		this.DB = ""
		return nil
	}

	if this.DB == *dbName {
		return nil
	}

	gno, nodeConn, err := this.connMgr.GetReadConnByRand()
	if err != nil {
		return err
	}
	defer this.connMgr.CloseReadConnByGno(gno)

	if err := nodeConn.ReInitUseDB(*dbName); err != nil {
		return err
	}

	// 将当前数据库变成 use的数据库
	this.DB = *dbName
	return nil
}

// 操作 select 语句
func (this *MySQLExecutor) doSelectStmt(query *string, stmt *ast.SelectStmt) (*mysql.Result, error) {
	vst := visitor.NewSelectVisitor(this.ctx)
	stmt.Accept(vst)
	if vst.Err != nil { // 解析语句违反了分表中的一些规则
		return nil, vst.Err
	}

	var sqlStr string
	// 判断是不是分库分表语句
	if len(vst.VisitorStmtMap) != 0 { // 是分库分表
		// 获取分表的字段并且计算所在的shard
		var computShardNoOk bool
		var shardNo int
		for _, visitorStmt := range vst.VisitorStmtMap {
			var ok bool
			if shardNo, ok = visitorStmt.GetShardNo(this.ctx.ShardAlgorithm); ok { // 是分表就执行sql
				var sb strings.Builder
				if err := stmt.Restore(format.NewRestoreCtx(format.DefaultRestoreFlags, &sb)); err != nil {
					return nil, fmt.Errorf("SELECT (shard) 从写SQL失败. %s", err.Error())
				}
				sqlStr = fmt.Sprintf(sb.String(), shardNo)
				computShardNoOk = true
				break
			}
		}
		if !computShardNoOk { // 计算分片好失败
			return nil, fmt.Errorf("SELECT (shard) 无法从分表字段中计算出(分片号), 请检查提供的字段是否完整.")
		}
		return this.executeShardQDL(&sqlStr, shardNo)
	} else { // 非分库分表的情况
		var sb strings.Builder
		if err := stmt.Restore(format.NewRestoreCtx(format.DefaultRestoreFlags, &sb)); err != nil {
			return nil, fmt.Errorf("SELECT (非shard) 从写SQL失败. %s", err.Error())
		}
		sqlStr = fmt.Sprintf(sb.String())

		return this.executeQDL(&sqlStr)
	}

	return nil, nil
}

// 操作 insert 语句
func (this *MySQLExecutor) doInsertStmt(query *string, stmt *ast.InsertStmt) (*mysql.Result, error) {
	// 处理 insert into t select 语句
	if stmt.Select != nil {
		return this.doInsertSelectStmt(query, stmt)
	} else { // 处理正常的 insert into t values(xxx, yyy),(xxx, yyy) 或 values((xxx,yyy), (xxx,yyy)) 信息
		vst := visitor.NewInsertValuesVisitor(this.ctx)
		stmt.Accept(vst)
		if vst.Err != nil {
			return nil, vst.Err
		}

		if vst.CurrVisitorStmt == nil { // 非分库分表
			return this.doInsertValuesStmt(query, stmt, vst)
		} else { // 分库分表情况
			return this.doInsertValuesStmtShard(query, stmt, vst)
		}
	}

	return nil, nil
}

// 处理 insert into t select, 语句
func (this *MySQLExecutor) doInsertSelectStmt(query *string, stmt *ast.InsertStmt) (*mysql.Result, error) {
	vst := visitor.NewInsertSelectVisitor(this.ctx)
	stmt.Accept(vst)
	if vst.Err != nil { // 解析语句违反了分表中的一些规则
		return nil, vst.Err
	}

	var sqlStr string
	// 判断是不是分库分表语句
	if len(vst.VisitorStmtMap) != 0 { // 是分库分表
		// 获取分表的字段并且计算所在的shard
		var computShardNoOk bool
		var shardNo int
		for _, visitorStmt := range vst.VisitorStmtMap {
			var ok bool
			if shardNo, ok = visitorStmt.GetShardNo(this.ctx.ShardAlgorithm); ok { // 是分表就执行sql
				var sb strings.Builder
				if err := stmt.Restore(format.NewRestoreCtx(format.DefaultRestoreFlags, &sb)); err != nil {
					return nil, fmt.Errorf("INSERT INTO SELECT (shard) 从写SQL失败. %s", err.Error())
				}
				sqlStr = fmt.Sprintf(sb.String(), shardNo)
				computShardNoOk = true
				break
			}
		}
		if !computShardNoOk { // 计算分片好失败
			return nil, fmt.Errorf("INSERT INTO SELECT (shard) 无法从分表字段中计算出(分片号), 请检查提供的字段是否完整.")
		}
		return this.executeShardDML(&sqlStr, shardNo)
	} else { // 非分库分表的情况
		var sb strings.Builder
		if err := stmt.Restore(format.NewRestoreCtx(format.DefaultRestoreFlags, &sb)); err != nil {
			return nil, fmt.Errorf("INSERT INTO SELECT (非shard) 从写SQL失败. %s", err.Error())
		}
		sqlStr = fmt.Sprintf(sb.String())
		return this.executeDML(&sqlStr)
	}

	return nil, nil
}

// 处理 insert into values 语句
func (this *MySQLExecutor) doInsertValuesStmt(query *string, stmt *ast.InsertStmt, vst *visitor.InsertValuesVisitor) (*mysql.Result, error) {
	var sb strings.Builder
	if err := stmt.Restore(format.NewRestoreCtx(format.DefaultRestoreFlags, &sb)); err != nil {
		return nil, fmt.Errorf("INSERT INTO VALUES (非shard) 从写SQL失败. %s", err.Error())
	}
	sqlStr := fmt.Sprintf(sb.String())

	return this.executeDML(&sqlStr)
}

// 处理 分库分表的 insert into 语句
func (this *MySQLExecutor) doInsertValuesStmtShard(query *string, stmt *ast.InsertStmt, vst *visitor.InsertValuesVisitor) (*mysql.Result, error) {
	tmpLists := vst.ValueList
	insertCnt := uint64(len(tmpLists))
	if insertCnt > 1 { // 有多条 insert 的时候需要执行 begin commit 操作
		this.doBegin(query, nil)
		for i, row := range tmpLists {
			newLists := make([][]ast.ExprNode, 1)
			newLists[0] = row
			stmt.Lists = newLists

			vst.SetListValueToShardTable(i) // 设置分片使用字段的值

			shardNo, ok := vst.CurrVisitorStmt.GetShardNo(this.ctx.ShardAlgorithm)
			if !ok { // 是分表就执行sql
				this.doRollbackStmt(query, nil)
				return nil, fmt.Errorf("INSERT INTO VALUES (shard) 无法从分表字段中计算出(分片号), 请检查提供的字段是否完整.")
			}

			var sb strings.Builder
			if err := stmt.Restore(format.NewRestoreCtx(format.DefaultRestoreFlags, &sb)); err != nil {
				this.doRollbackStmt(query, nil)
				return nil, fmt.Errorf("INSERT INTO VALUES (shard) 从写SQL失败. %s", err.Error())
			}
			sqlStr := fmt.Sprintf(sb.String(), shardNo)
			if _, err := this.executeShardDML(&sqlStr, shardNo); err != nil {
				this.doRollbackStmt(query, nil)
				return nil, err
			}
		}
		rs, err := this.doCommitStmt(query, nil)
		rs = new(mysql.Result)
		rs.AffectedRows = insertCnt
		return rs, err
	} else {
		for i, row := range tmpLists {
			newLists := make([][]ast.ExprNode, 1)
			newLists[0] = row
			stmt.Lists = newLists

			vst.SetListValueToShardTable(i) // 设置分片使用字段的值

			shardNo, ok := vst.CurrVisitorStmt.GetShardNo(this.ctx.ShardAlgorithm)
			if !ok { // 是分表就执行sql
				return nil, fmt.Errorf("INSERT INTO VALUES (shard) 无法从分表字段中计算出(分片号), 请检查提供的字段是否完整.")
			}

			var sb strings.Builder
			if err := stmt.Restore(format.NewRestoreCtx(format.DefaultRestoreFlags, &sb)); err != nil {
				return nil, fmt.Errorf("INSERT INTO VALUES (shard) 从写SQL失败. %s", err.Error())
			}
			sqlStr := fmt.Sprintf(sb.String(), shardNo)
			return this.executeShardDML(&sqlStr, shardNo)
		}
	}
	return nil, nil
}

// 操作 delete 语句
func (this *MySQLExecutor) doDeleteStmt(query *string, stmt *ast.DeleteStmt) (*mysql.Result, error) {
	vst := visitor.NewDeleteVisitor(this.ctx)
	stmt.Accept(vst)
	if vst.Err != nil {
		return nil, vst.Err
	}

	var sqlStr string
	// 判断是不是分库分表语句
	if len(vst.VisitorStmtMap) != 0 { // 是分库分表
		// 获取分表的字段并且计算所在的shard
		var computShardNoOk bool
		var shardNo int
		for _, visitorStmt := range vst.VisitorStmtMap {
			var ok bool
			if shardNo, ok = visitorStmt.GetShardNo(this.ctx.ShardAlgorithm); ok { // 是分表就执行sql
				var sb strings.Builder
				if err := stmt.Restore(format.NewRestoreCtx(format.DefaultRestoreFlags, &sb)); err != nil {
					return nil, fmt.Errorf("DELETE FROM (shard) 从写SQL失败. %s", err.Error())
				}
				sqlStr = fmt.Sprintf(sb.String(), shardNo)
				computShardNoOk = true
				break
			}
		}
		if !computShardNoOk { // 计算分片好失败
			return nil, fmt.Errorf("DELETE FROM (shard) 无法从分表字段中计算出(分片号), 请检查提供的字段是否完整.")
		}
		return this.executeShardDML(&sqlStr, shardNo)
	} else { // 非分库分表的情况
		var sb strings.Builder
		if err := stmt.Restore(format.NewRestoreCtx(format.DefaultRestoreFlags, &sb)); err != nil {
			return nil, fmt.Errorf("DELETE FROM (非shard) 从写SQL失败. %s", err.Error())
		}
		sqlStr = fmt.Sprintf(sb.String())
		return this.executeDML(&sqlStr)
	}

	return nil, nil
}

// 操作 update 语句
func (this *MySQLExecutor) doUpdateStmt(query *string, stmt *ast.UpdateStmt) (*mysql.Result, error) {
	vst := visitor.NewUpdateVisitor(this.ctx)
	stmt.Accept(vst)
	if vst.Err != nil {
		return nil, vst.Err
	}

	var sqlStr string
	// 判断是不是分库分表语句
	if len(vst.VisitorStmtMap) != 0 { // 是分库分表
		// 获取分表的字段并且计算所在的shard
		var computShardNoOk bool
		var shardNo int
		for _, visitorStmt := range vst.VisitorStmtMap {
			var ok bool
			if shardNo, ok = visitorStmt.GetShardNo(this.ctx.ShardAlgorithm); ok { // 是分表就执行sql
				var sb strings.Builder
				if err := stmt.Restore(format.NewRestoreCtx(format.DefaultRestoreFlags, &sb)); err != nil {
					return nil, fmt.Errorf("UPDATE (shard) 从写SQL失败. %s", err.Error())
				}
				sqlStr = fmt.Sprintf(sb.String(), shardNo)
				computShardNoOk = true
				break
			}
		}
		if !computShardNoOk { // 计算分片好失败
			return nil, fmt.Errorf("UPDATE (shard) 无法从分表字段中计算出(分片号), 请检查提供的字段是否完整.")
		}
		return this.executeShardDML(&sqlStr, shardNo)
	} else { // 非分库分表的情况
		var sb strings.Builder
		if err := stmt.Restore(format.NewRestoreCtx(format.DefaultRestoreFlags, &sb)); err != nil {
			return nil, fmt.Errorf("UPDATE (非shard) 从写SQL失败. %s", err.Error())
		}
		sqlStr = fmt.Sprintf(sb.String())
		return this.executeDML(&sqlStr)
	}

	return nil, nil
}

// 执行 commit
func (this *MySQLExecutor) doCommitStmt(query *string, stmt *ast.CommitStmt) (*mysql.Result, error) {
	defer this.connMgr.Close()

	this.InTransaction = false
	if err := this.connMgr.WriteConnCommit(); err != nil {
		return nil, err
	}
	return nil, nil
}

// 执行 rollback
func (this *MySQLExecutor) doRollbackStmt(query *string, stmt *ast.RollbackStmt) (*mysql.Result, error) {
	defer this.connMgr.Close()

	this.InTransaction = false
	if err := this.connMgr.WriteConnRollback(); err != nil {
		return nil, err
	}
	return nil, nil
}

// 执行 Begin
func (this *MySQLExecutor) doBegin(query *string, stmt *ast.BeginStmt) (*mysql.Result, error) {
	this.InTransaction = true
	return nil, nil
}

// do set 语句
func (this *MySQLExecutor) doSetStmt(query *string, stmt *ast.SetStmt) (*mysql.Result, error) {
	for _, variable := range stmt.Variables {
		value, ok := variable.Value.(*driver.ValueExpr)
		if !ok {
			return nil, fmt.Errorf("未能正确解析 SET 语句的值")
		}

		switch strings.ToLower(variable.Name) {
		case "autocommit": // 处理 autocommit
			data, err := utils.InterfaceToInt64(value.GetValue())
			if err != nil {
				return nil, fmt.Errorf("SET autocommit = %#v, 无法将右值转化为数字", value.GetValue())
			}

			if data == 0 {
				this.AutoCommit = false
			} else {
				this.AutoCommit = true
				if err := this.connMgr.WriteConnCommit(); err != nil {
					seelog.Error(err.Error())
				}
				this.InTransaction = false
			}
		default:
			return nil, fmt.Errorf("不支持 set %s 语句", variable.Name)
		}
	}
	return nil, nil
}
