package executor

import (
	"fmt"
	"github.com/daiguadaidai/dal/dal_context"
	"github.com/daiguadaidai/dal/go-mysql/mysql"
	"github.com/daiguadaidai/parser"
	"github.com/daiguadaidai/parser/ast"
	_ "github.com/daiguadaidai/tidb/types/parser_driver"
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
		ctx:     ctx,
		connMgr: NewMySQLConnectionManager(ctx),
	}
}

// use db 语句
func (this *MySQLExecutor) UseDB(dbName *string) error {
	// 如果当前已经是 use db, 不需要链接数据库操作
	if strings.TrimSpace(*dbName) == "" {
		return mysql.NewDefaultError(mysql.ER_NO_DB_ERROR)
	}

	if this.DB == *dbName {
		return nil
	}

	gno, nodeConn, err := this.connMgr.GetReadConnByRand()
	if err != nil {
		return err
	}
	this.connMgr.CloseConnByGno(gno)

	// 指定变换当前数据库
	rs, err := nodeConn.Conn.Execute(fmt.Sprintf("SHOW DATABASES LIKE '%s'", dbName))
	if err != nil {
		return err
	}

	// 指定的数据库不存在
	if len(rs.RowDatas) == 0 {
		return mysql.NewDefaultError(mysql.ER_BAD_DB_ERROR, dbName)
	}

	// 将当前数据库变成 use的数据库
	this.DB = *dbName
	return nil
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
		return nil, fmt.Errorf("Error: CreateDatabaseStmt")
	case *ast.DropDatabaseStmt:
		return nil, fmt.Errorf("Error: DropDatabaseStmt")
	case *ast.CreateTableStmt:
		return nil, fmt.Errorf("Error: CreateTableStmt")
	case *ast.DropTableStmt:
		return nil, fmt.Errorf("Error: DropTableStmt")
	case *ast.RenameTableStmt:
		return nil, fmt.Errorf("Error: RenameTableStmt")
	case *ast.CreateViewStmt:
		return nil, fmt.Errorf("Error: CreateViewStmt")
	case *ast.CreateIndexStmt:
		return nil, fmt.Errorf("Error: CreateIndexStmt")
	case *ast.DropIndexStmt:
		return nil, fmt.Errorf("Error: DropIndexStmt")
	case *ast.AlterTableStmt:
		return nil, fmt.Errorf("Error: AlterTableStmt")
	case *ast.TruncateTableStmt:
		return nil, fmt.Errorf("Error: TruncateTableStmt")
	case *ast.SelectStmt:
		return nil, fmt.Errorf("Error: SelectStmt")
	case *ast.UnionStmt:
		return nil, fmt.Errorf("Error: UnionStmt")
	case *ast.LoadDataStmt:
		return nil, fmt.Errorf("Error: LoadDataStmt")
	case *ast.InsertStmt:
		return nil, fmt.Errorf("Error: InsertStmt")
	case *ast.DeleteStmt:
		return nil, fmt.Errorf("Error: DeleteStmt")
	case *ast.UpdateStmt:
		return nil, fmt.Errorf("Error: UpdateStmt")
	case *ast.ShowStmt:
		return nil, fmt.Errorf("Error: ShowStmt")
	case *ast.TraceStmt:
		return nil, fmt.Errorf("Error: TraceStmt")
	case *ast.ExplainStmt:
		return nil, fmt.Errorf("Error: ExplainStmt")
	case *ast.PrepareStmt:
		return nil, fmt.Errorf("Error: PrepareStmt")
	case *ast.DeallocateStmt:
		return nil, fmt.Errorf("Error: DeallocateStmt")
	case *ast.ExecuteStmt:
		return nil, fmt.Errorf("Error: ExecuteStmt")
	case *ast.BeginStmt:
		return nil, fmt.Errorf("Error: beginStmt")
	case *ast.BinlogStmt:
		return nil, fmt.Errorf("Error: BinlogStmt")
	case *ast.CommitStmt:
		return nil, fmt.Errorf("Error: CommitStmt")
	case *ast.RollbackStmt:
		return nil, fmt.Errorf("Error: RollbackStmt")
	case *ast.UseStmt:
		return nil, this.UseDB(&stmt.DBName)
	case *ast.FlushStmt:
		return nil, fmt.Errorf("Error: FlushStmt")
	case *ast.KillStmt:
		return nil, fmt.Errorf("Error: KillStmt")
	case *ast.SetStmt:
		return nil, fmt.Errorf("Error: SetStmt")
	case *ast.SetPwdStmt:
		return nil, fmt.Errorf("Error: SetPwdStmt")
	case *ast.CreateUserStmt:
		return nil, fmt.Errorf("Error: CreateUserStmt")
	case *ast.AlterUserStmt:
		return nil, fmt.Errorf("Error: AlterUserStmt")
	case *ast.DropUserStmt:
		return nil, fmt.Errorf("Error: DropUserStmt")
	case *ast.DoStmt:
		return nil, fmt.Errorf("Error: DoStmt")
	case *ast.AdminStmt:
		return nil, fmt.Errorf("Error: AdminStmt")
	case *ast.RevokeStmt:
		return nil, fmt.Errorf("Error: RevokeStmt")
	case *ast.GrantStmt:
		return nil, fmt.Errorf("Error: GrantStmt")
	case *ast.AnalyzeTableStmt:
		return nil, fmt.Errorf("Error: AnalyzeTableStmt")
	case *ast.DropStatsStmt:
		return nil, fmt.Errorf("Error: DropStatsStmt")
	case *ast.LoadStatsStmt:
		return nil, fmt.Errorf("Error: LoadStatsStmt")
	}

	return nil, fmt.Errorf("不支持该sql语句: %s")
}
