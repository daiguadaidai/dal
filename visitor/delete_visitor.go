package visitor

import (
	"fmt"
	"github.com/daiguadaidai/dal/dal_context"
	"github.com/daiguadaidai/dal/utils"
	"github.com/daiguadaidai/parser/ast"
	driver "github.com/daiguadaidai/tidb/types/parser_driver"
)

type DeleteVisitor struct {
	ctx             *dal_context.DalContext
	DefaultSchema   string // dal提供链接的 数据库名
	CurrNodeLevel   int    // 当前节点所在层级
	Err             error
	VisitorStmtMap  map[int]*VisitorStmt // key: stmtNo(第几条语句), value: *VisitorStmt
	CurrVisitorStmt *VisitorStmt         // 当前的语句Stmt
	StmtNoHeap      *utils.IntHeap       // 使用一个堆保存便利过的语句.
	StmtNo          int                  // 记录了总语句数
	CurrStmtNo      int                  // 记录了当前是第几个语句
	CurrBlock       int                  // 当前所在的语句块
	BlockHeap       *utils.IntHeap       // 当前所在语句块栈
}

func NewDeleteVisitor(ctx *dal_context.DalContext) *DeleteVisitor {
	return &DeleteVisitor{
		DefaultSchema:  ctx.ServerCtx.DBName,
		ctx:            ctx,
		VisitorStmtMap: make(map[int]*VisitorStmt),
		StmtNoHeap:     utils.NewIntHeap(),
		BlockHeap:      utils.NewIntHeap(),
		CurrBlock:      BLOCK_NONE,
	}
}

// 计算语句有几个
func (this *DeleteVisitor) incrStmtNo() {
	if this.CurrVisitorStmt != nil {
		this.VisitorStmtMap[this.CurrStmtNo] = this.CurrVisitorStmt
		this.CurrVisitorStmt = nil
	}

	this.StmtNoHeap.Push(this.CurrStmtNo)
	this.StmtNo++
	this.CurrStmtNo = this.StmtNo

}

// pop 出语句号, 并计算应该到第几个语句了
func (this *DeleteVisitor) popStmtNo() {
	this.CurrVisitorStmt = nil
	if currStmtNo, ok := this.StmtNoHeap.Pop(); ok {
		this.CurrStmtNo = currStmtNo
		if visitorStmt, ok := this.VisitorStmtMap[this.CurrStmtNo]; ok {
			this.CurrVisitorStmt = visitorStmt
		}
	}
}

// 记录当前语句块
func (this *DeleteVisitor) setCurrBlock(currBlock int) {
	this.BlockHeap.Push(this.CurrBlock) // 将当前block保存下来
	this.CurrBlock = currBlock          // 重置当前block
}

// pop语句块
func (this *DeleteVisitor) popBlock() {
	if block, ok := this.BlockHeap.Pop(); ok {
		this.CurrBlock = block
	}
}

func (this *DeleteVisitor) Enter(in ast.Node) (out ast.Node, skipChildren bool) {
	// 有错误直接退出
	if this.Err != nil {
		return in, true
	}
	// 增加节点层级
	this.CurrNodeLevel++

	fmt.Printf("%sEnter: %[2]T, %[2]v, %[2]p\n", utils.GetIntend(this.CurrNodeLevel-1, " ", 4), in)

	switch node := in.(type) {
	case *ast.DeleteStmt:
		this.Err = this.enterDeleteStmt(node)
	case *ast.SelectStmt:
		this.Err = this.enterSelectStmt(node)
	case *ast.FieldList:
		this.Err = this.enterFieldList(node)
	case *ast.TableSource:
		this.Err = this.enterTableSource(node)
	case *ast.BinaryOperationExpr:
		this.Err = this.enterBinaryOperationExpr(node)
	case *ast.PatternInExpr: // IN(1, 2, 3)
		this.Err = this.enterPatternInExpr(node)
	case *ast.PatternLikeExpr: // LIKE '%xxx%' 语句
		this.Err = this.enterPatternLikeExpr(node)
	case *ast.BetweenExpr: // a BETWEEN b AND c
		this.Err = this.enterBetweenExpr(node)
	}

	return in, false
}

func (this *DeleteVisitor) Leave(in ast.Node) (out ast.Node, ok bool) {
	defer func() {
		this.CurrNodeLevel--
	}()

	// 有错误直接退出
	if this.Err != nil {
		return in, false
	}

	fmt.Printf("%sLeave: %T, %[2]p\n", utils.GetIntend(this.CurrNodeLevel-1, " ", 4), in)
	switch node := in.(type) {
	case *ast.DeleteStmt:
		this.Err = this.leaveDeleteStmt(node)
	case *ast.SelectStmt:
		this.Err = this.leaveSelectStmt(node)
	case *ast.FieldList:
		this.Err = this.leaveFieldList(node)
	case *ast.TableSource:
		this.Err = this.leaveTableSource(node)
	case *ast.BinaryOperationExpr:
		this.Err = this.leaveBinaryOperationExpr(node)
	case *ast.PatternInExpr: // IN(1, 2, 3)
		this.Err = this.leavePatternInExpr(node)
	case *ast.PatternLikeExpr: // LIKE '%xxx%' 语句
		this.Err = this.leavePatternLikeExpr(node)
	case *ast.BetweenExpr: // a BETWEEN b AND c
		this.Err = this.leaveBetweenExpr(node)
	}

	return in, true
}

func (this *DeleteVisitor) enterDeleteStmt(node *ast.DeleteStmt) error {
	// 计算语句号, 计算语句有几个
	this.incrStmtNo()

	return nil
}

// 离开 *ast.SelectStmt 节点
func (this *DeleteVisitor) leaveDeleteStmt(node *ast.DeleteStmt) error {
	// pop 出语句号, 并计算应该到第几个语句了
	defer this.popStmtNo()

	// 将当前语句保存到, 访问语句Map中
	if this.CurrVisitorStmt != nil {
		this.VisitorStmtMap[this.CurrStmtNo] = this.CurrVisitorStmt
	}

	return nil
}

// 进入 *ast.SelectStmt 节点
func (this *DeleteVisitor) enterSelectStmt(node *ast.SelectStmt) error {
	// 计算语句号, 计算语句有几个
	this.incrStmtNo()
	return nil
}

// 离开 *ast.SelectStmt 节点
func (this *DeleteVisitor) leaveSelectStmt(node *ast.SelectStmt) error {
	// pop 出语句号, 并计算应该到第几个语句了
	defer this.popStmtNo()

	// 将当前语句保存到, 访问语句Map中
	if this.CurrVisitorStmt != nil {
		this.VisitorStmtMap[this.CurrStmtNo] = this.CurrVisitorStmt
	}

	return nil
}

// 进入 Select Field 节点(SELECT block)
func (this *DeleteVisitor) enterFieldList(node *ast.FieldList) error {
	this.setCurrBlock(BLOCK_SELECT)

	return nil
}

// 离开 Select Field 节点
func (this *DeleteVisitor) leaveFieldList(node *ast.FieldList) error {
	defer this.popBlock()

	return nil
}

// 进入 TableSource
func (this *DeleteVisitor) enterTableSource(node *ast.TableSource) error {
	// 判断该表是否是分表
	tableName, ok := node.Source.(*ast.TableName)
	if ok {
		schema, table := utils.GetSchemaAndTable(&this.DefaultSchema, &tableName.Schema.O, &tableName.Name.O, nil)
		if shardTable, ok := this.ctx.ShardTableInstance.GetShardTable(schema, table); ok { // 该表是shard表
			// 没有设置当前访问语句信息, 则创建一个
			if this.CurrVisitorStmt == nil {
				this.CurrVisitorStmt = NewVisitorStmt()
			}
			// 保存table别名对于的表明, 将该shardtable保存到 visitor stmt中
			// 获取数据库 名称
			if err := this.CurrVisitorStmt.AddVisitorTable(schema, table, node.AsName.O, shardTable); err != nil {
				return err
			}
		}
	}

	return nil
}

// 离开 TableSource
func (this *DeleteVisitor) leaveTableSource(node *ast.TableSource) error {
	// 当前语句没有分表信息, 直接退出
	if this.CurrVisitorStmt == nil {
		return nil
	}

	// 该表名是分表, 替换表名字
	tableName, ok := node.Source.(*ast.TableName)
	if ok { // 判断是否是分表, 如果是分表则修改表的名称有下划线
		if exists := this.CurrVisitorStmt.TableExists(&this.DefaultSchema, &tableName.Schema.O, &tableName.Name.O, &node.AsName.O); exists {
			tableName.Name.O += "_%[1]d"
		}
	}

	return nil
}

// 进入含有类似 a = 1, b = 2. 的语句块中.
func (this *DeleteVisitor) enterBinaryOperationExpr(node *ast.BinaryOperationExpr) error {
	this.setCurrBlock(BLOCK_WHERE)

	return nil
}

// 出 BinaryOperationExpr 节点
func (this *DeleteVisitor) leaveBinaryOperationExpr(node *ast.BinaryOperationExpr) error {
	defer this.popBlock()
	if this.CurrVisitorStmt == nil { // 如果该语句中没有分库分表, 就不用管了
		return nil
	}

	// 碰到谓词等式
	// 谓词左边字段名
	columnNameExpr, ok := node.L.(*ast.ColumnNameExpr)
	if ok {
		// 判断该字段是否是分表字段
		visitorTable, err := this.CurrVisitorStmt.GetVisitorTableIfIsShardCol(&this.DefaultSchema, columnNameExpr)
		if err != nil {
			return err
		}
		if visitorTable == nil { // 该字段不是分表计算字段
			return nil
		}
		// 确定字段是分表计算使用的字段, 则谓词右边必须是一个值, 不能是表达式, 或则子句, 或则方法
		// 谓词右边值, 比如(不合法)的写法有: name = max(1) 或 name = (select name from employees) 等等
		//                (合法)的有    : name = 1 或 name = 'aa'
		switch v := node.R.(type) {
		case *driver.ValueExpr:
			// 将值添加到 visitor table 中
			if _, ok := visitorTable.ColValues[columnNameExpr.Name.Name.O]; ok {
				return fmt.Errorf("分表字段: %s, 同一子句中不能出现多次, 不合法示例: name = 1 and name = 2", columnNameExpr.Name.Name.O)
			}
			visitorTable.ColValues[columnNameExpr.Name.Name.O] = v.GetValue()
		case *ast.SubqueryExpr:
			return fmt.Errorf("分表字段的右值不能为子查询, 错误示例: name = (select name from employees)")
		case *ast.FuncCallExpr, *ast.AggregateFuncExpr:
			return fmt.Errorf("分表字段的右值不能是函数, 错误示例: name = max(1)")
		}
	}

	return nil
}

// 进入 IN 语句
func (this *DeleteVisitor) enterPatternInExpr(node *ast.PatternInExpr) error {
	this.setCurrBlock(BLOCK_PATTERN_IN)
	return nil
}

// 离开 IN 语句
func (this *DeleteVisitor) leavePatternInExpr(node *ast.PatternInExpr) error {
	defer this.popBlock()
	if this.CurrVisitorStmt == nil { // 如果该语句中没有分库分表, 就不用管了
		return nil
	}

	if columnNameExpr, ok := node.Expr.(*ast.ColumnNameExpr); ok {
		// 判断该字段是否是分表字段
		visitorTable, err := this.CurrVisitorStmt.GetVisitorTableIfIsShardCol(&this.DefaultSchema, columnNameExpr)
		if err != nil {
			return err
		}
		if visitorTable != nil { // 是分表字段, 则报错. 分表字段不能使用IN
			return fmt.Errorf("分表字段:%s, 不允许使用IN(xx, yy)", columnNameExpr.Name.Name.O)
		}
	}

	return nil
}

// 进入 Like 语句
func (this *DeleteVisitor) enterPatternLikeExpr(node *ast.PatternLikeExpr) error {
	this.setCurrBlock(BLOCK_PATTERN_LIKE)

	return nil
}

// 离开 Like 语句
func (this *DeleteVisitor) leavePatternLikeExpr(node *ast.PatternLikeExpr) error {
	defer this.popBlock()
	if this.CurrVisitorStmt == nil { // 如果该语句中没有分库分表, 就不用管了
		return nil
	}

	if columnNameExpr, ok := node.Expr.(*ast.ColumnNameExpr); ok {
		// 判断该字段是否是分表字段
		visitorTable, err := this.CurrVisitorStmt.GetVisitorTableIfIsShardCol(&this.DefaultSchema, columnNameExpr)
		if err != nil {
			return err
		}
		if visitorTable != nil { // 是分表字段, 则报错. 分表字段不能使用IN
			return fmt.Errorf("分表字段:%s, 不允许使用LIKE 'xx'", columnNameExpr.Name.Name.O)
		}
	}

	return nil
}

// 进入 BETWEEN 语句
func (this *DeleteVisitor) enterBetweenExpr(node *ast.BetweenExpr) error {
	this.setCurrBlock(BLOCK_BETWEEN)
	return nil
}

// 离开 BETWEEN 语句
func (this *DeleteVisitor) leaveBetweenExpr(node *ast.BetweenExpr) error {
	defer this.popBlock()
	if this.CurrVisitorStmt == nil { // 如果该语句中没有分库分表, 就不用管了
		return nil
	}

	if columnNameExpr, ok := node.Expr.(*ast.ColumnNameExpr); ok {
		// 判断该字段是否是分表字段
		visitorTable, err := this.CurrVisitorStmt.GetVisitorTableIfIsShardCol(&this.DefaultSchema, columnNameExpr)
		if err != nil {
			return err
		}
		if visitorTable != nil { // 是分表字段, 则报错. 分表字段不能使用IN
			return fmt.Errorf("分表字段:%s, 不允许使用BETWEEN xx AND yy", columnNameExpr.Name.Name.O)
		}
	}

	return nil
}
