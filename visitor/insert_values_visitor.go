package visitor

import (
	"fmt"
	"github.com/daiguadaidai/dal/dal_context"
	"github.com/daiguadaidai/dal/utils"
	"github.com/daiguadaidai/parser/ast"
	"github.com/daiguadaidai/tidb/types/parser_driver"
)

type InsertValuesVisitor struct {
	ctx             *dal_context.DalContext
	DefaultSchema   string // dal提供链接的 数据库名
	Err             error
	CurrVisitorStmt *VisitorStmt // 当前的语句Stmt
	CurrNodeLevel   int
	ShardColPosMap  map[string]int // key: 字段名, value: 字段所在位置
	ColumnCnt       int            // 字段个数
	ValueList       [][]ast.ExprNode
}

func NewInsertValuesVisitor(ctx *dal_context.DalContext) *InsertValuesVisitor {
	return &InsertValuesVisitor{
		DefaultSchema:  ctx.ServerCtx.DBName,
		ctx:            ctx,
		ShardColPosMap: make(map[string]int),
		ValueList:      make([][]ast.ExprNode, 0),
	}
}

func (this *InsertValuesVisitor) Enter(in ast.Node) (out ast.Node, skipChildren bool) {
	this.CurrNodeLevel++
	// fmt.Printf("%sEnter: %[2]T, %[2]v, %[2]p\n", utils.GetIntend(this.CurrNodeLevel-1, " ", 4), in)

	switch node := in.(type) {
	case *ast.InsertStmt:
		this.Err = this.enterInsertStmt(node)
	}

	return in, true
	// return in, false
}

func (this *InsertValuesVisitor) Leave(in ast.Node) (out ast.Node, ok bool) {
	// fmt.Printf("%sLeave: %T, %[2]p\n", utils.GetIntend(this.CurrNodeLevel-1, " ", 4), in)
	this.CurrNodeLevel--
	return in, true
}

// 检测
func (this *InsertValuesVisitor) enterInsertStmt(node *ast.InsertStmt) error {
	// 设置分表信息
	this.ColumnCnt = len(node.Columns)
	if err := this.setTableSource(node.Table.TableRefs.Left.(*ast.TableSource)); err != nil {
		return err
	}

	// 非分表的就不需要进行下一步了
	if this.CurrVisitorStmt == nil {
		return nil
	}

	// 设置分表字段所在位置
	if err := this.setShardColPos(node.Columns); err != nil {
		return err
	}

	// 检测分表字段是否指定正确
	if err := this.checkShardCol(); err != nil {
		return err
	}

	if err := this.checkValues(node.Lists); err != nil {
		return err
	}

	return nil
}

// 进入 TableSource
func (this *InsertValuesVisitor) setTableSource(node *ast.TableSource) error {
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

			tableName.Name.O += "_%[1]d"
		}
	}

	return nil
}

// 设置分表字段所在位置
func (this *InsertValuesVisitor) setShardColPos(nodes []*ast.ColumnName) error {
	if len(nodes) == 0 {
		return fmt.Errorf("分表 INSERT 必须显示指定需要插入的字段.")
	}

	for i, columnName := range nodes {
		if _, ok := this.CurrVisitorStmt.FirstVisitorTable.ShardTable.ShardColMap[columnName.Name.O]; ok {
			this.ShardColPosMap[columnName.Name.O] = i
		}
	}

	return nil
}

// 检测分表字段是否正确
func (this *InsertValuesVisitor) checkShardCol() error {
	if len(this.ShardColPosMap) != len(this.CurrVisitorStmt.FirstVisitorTable.ShardTable.ShardCols) {
		return fmt.Errorf("指定分表字段个数不匹配: 需要字段个数: %d, 指定字段个数: %d", len(this.CurrVisitorStmt.FirstVisitorTable.ShardTable.ShardCols), len(this.ShardColPosMap))
	}

	for _, col := range this.CurrVisitorStmt.FirstVisitorTable.ShardTable.ShardCols {
		if _, ok := this.ShardColPosMap[col]; !ok {
			return fmt.Errorf("未指定分表字段: %s", col)
		}
	}

	return nil
}

// 检测值
func (this *InsertValuesVisitor) checkValues(lists [][]ast.ExprNode) error {
	for _, list := range lists {
		if err := this.checkRow(list); err != nil {
			return err
		}
	}
	return nil
}

const (
	VALUES_TYPE_NONE             uint8 = 0
	VALUES_TYPE_VALUE_EXPR       uint8 = 1
	VALUES_TYPE_ROW_EXPR         uint8 = 2
	VALUES_TYPE_PARENTHESES_EXPR uint8 = 3
)

// 检测一行数据
func (this *InsertValuesVisitor) checkRow(nodes []ast.ExprNode) error {
	firstType := VALUES_TYPE_NONE
	switch nodes[0].(type) {
	case *driver.ValueExpr:
		firstType = VALUES_TYPE_VALUE_EXPR
	case *ast.RowExpr:
		firstType = VALUES_TYPE_ROW_EXPR
	case *ast.ParenthesesExpr:
		firstType = VALUES_TYPE_PARENTHESES_EXPR
	}
	fmt.Printf("firstTtype: %d, type: %T\n", firstType, nodes[0])
	for _, node := range nodes {
		switch v := node.(type) {
		case *driver.ValueExpr:
			if firstType != VALUES_TYPE_VALUE_EXPR {
				return fmt.Errorf("values 的格式不匹配. 第一个值的类型是: %d, 当前值的类型是: %d. (1.ValueExpr, 2.RowExpr, 3.ParenthesesExpr)", firstType, VALUES_TYPE_VALUE_EXPR)
			}
		case *ast.RowExpr:
			if firstType != VALUES_TYPE_ROW_EXPR {
				return fmt.Errorf("values 的格式不匹配. 第一个值的类型是: %d, 当前值的类型是: %d. (1.ValueExpr, 2.RowExpr, 3.ParenthesesExpr)", firstType, VALUES_TYPE_ROW_EXPR)
			}
			if err := this.checkRow(v.Values); err != nil {
				return err
			}
		case *ast.ParenthesesExpr:
			if firstType != VALUES_TYPE_PARENTHESES_EXPR {
				return fmt.Errorf("values 的格式不匹配. 第一个值的类型是: %d, 当前值的类型是: %d. (1.ValueExpr, 2.RowExpr, 3.ParenthesesExpr)", firstType, VALUES_TYPE_PARENTHESES_EXPR)
			}
			if err := this.checkParenthesesExpr(v.Expr); err != nil {
				return err
			}
		}
	}

	if firstType == VALUES_TYPE_VALUE_EXPR {
		if this.ColumnCnt != len(nodes) {
			return fmt.Errorf("字段个数和指定的values个数不匹配")
		}
		this.ValueList = append(this.ValueList, nodes)
	}

	return nil
}

// 检测多个嵌套的值
func (this *InsertValuesVisitor) checkParenthesesExpr(node ast.ExprNode) error {
	switch v := node.(type) {
	case *ast.ParenthesesExpr:
		if err := this.checkParenthesesExpr(v.Expr); err != nil {
			return err
		}
	case *ast.RowExpr:
		if err := this.checkRow(v.Values); err != nil {
			return err
		}
	}

	return nil
}

// 设置分表值为第几个记录的
func (this *InsertValuesVisitor) SetListValueToShardTable(rowIndex int) {
	for col, pos := range this.ShardColPosMap {
		this.CurrVisitorStmt.FirstVisitorTable.ColValues[col] = this.ValueList[rowIndex][pos].(*driver.ValueExpr).GetValue()
	}
}
