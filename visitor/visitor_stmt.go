package visitor

import (
	"fmt"
	"github.com/cihub/seelog"
	"github.com/daiguadaidai/dal/algorithm"
	"github.com/daiguadaidai/dal/mysqldb/topo"
	"github.com/daiguadaidai/dal/utils"
	"github.com/daiguadaidai/parser/ast"
)

type VisitorStmt struct {
	TableCnt          int                      // 这个语句中有几个需要分库的表
	VisitorTableMap   map[string]*VisitorTable // 本语句中访问的表. key: schema.tableAsName
	FirstVisitorTable *VisitorTable
}

func NewVisitorStmt() *VisitorStmt {
	return &VisitorStmt{
		VisitorTableMap: make(map[string]*VisitorTable),
	}
}

// 添加一个shard table
func (this *VisitorStmt) AddVisitorTable(schema, table, alias string, shardTable *topo.ShardTable) error {
	// 获取表的 临时名称, asName
	var tableAsName string
	if alias != "" {
		tableAsName = alias
	} else {
		tableAsName = table
	}

	key := utils.ConcatTableName(&schema, &tableAsName)
	_, ok := this.VisitorTableMap[key]
	if ok { // 该表已经存在
		return fmt.Errorf("表名/别名: %s, 同一个子句中不能出现相同的(表名/别名)", tableAsName)
	}

	// 创建并添加visitorTable
	visitorTable := NewVisitorTable(shardTable)
	this.VisitorTableMap[key] = visitorTable

	// 语句中shard 表的数量增加1
	this.TableCnt++
	if this.TableCnt == 1 {
		this.FirstVisitorTable = visitorTable
	}

	return nil
}

// 判断分表是否存在
func (this *VisitorStmt) TableExists(defaultSchema, schema, table, alias *string) bool {
	key := utils.GetConcatSchemAndTableKey(defaultSchema, schema, table, alias)
	_, ok := this.VisitorTableMap[key]
	return ok
}

// 判断列是否存在
func (this *VisitorStmt) GetVisitorTableIfIsShardCol(defaultSchema *string, columnNameExpr *ast.ColumnNameExpr) (*VisitorTable, error) {
	if columnNameExpr.Name.Table.O == "" { // 没有指定表名
		if this.TableCnt == 1 { // 如果只有一个表, 直接默认就使用这个表的所有信息
			if _, ok := this.FirstVisitorTable.ShardTable.ShardColMap[columnNameExpr.Name.Name.O]; ok {
				return this.FirstVisitorTable, nil
			}
			return nil, nil
		} else { // 如果有多个表, 字段名前面必须要带上(表名/别名)
			return nil, fmt.Errorf("语句中有多个表, 因此谓词字段中必须带上(表名/别名), 如: WHERE t1.name = 1 AND t2.name=2")
		}
	} else { // 有表别名的情况
		key := utils.GetShardTableKey(defaultSchema, &columnNameExpr.Name.Schema.O, &columnNameExpr.Name.Table.O)
		if visitorTable, ok := this.VisitorTableMap[key]; !ok { // 字段不属于分表
			return nil, nil
		} else { // 字段属于分表, 进一步判断字段是不是分表需要的字段
			if _, ok1 := visitorTable.ShardTable.ShardColMap[columnNameExpr.Name.Name.O]; ok1 {
				return visitorTable, nil
			}
			return nil, nil
		}
	}

	return nil, nil
}

// 判断列是否存在, SET col1 = 1, col2 = 2
func (this *VisitorStmt) GetVisitorTableIfIsShardColByColumnName(defaultSchema *string, columnName *ast.ColumnName) (*VisitorTable, error) {
	if columnName.Table.O == "" { // 没有指定表名
		if this.TableCnt == 1 { // 如果只有一个表, 直接默认就使用这个表的所有信息
			if _, ok := this.FirstVisitorTable.ShardTable.ShardColMap[columnName.Name.O]; ok {
				return this.FirstVisitorTable, nil
			}
			return nil, nil
		} else { // 如果有多个表, 字段名前面必须要带上(表名/别名)
			return nil, fmt.Errorf("语句中有多个表, 因此谓词字段中必须带上(表名/别名), 如: SET t1.name = 1, t2.name=2")
		}
	} else { // 有表别名的情况
		key := utils.GetShardTableKey(defaultSchema, &columnName.Schema.O, &columnName.Table.O)
		if visitorTable, ok := this.VisitorTableMap[key]; !ok { // 字段不属于分表
			return nil, nil
		} else { // 字段属于分表, 进一步判断字段是不是分表需要的字段
			if _, ok1 := visitorTable.ShardTable.ShardColMap[columnName.Name.O]; ok1 {
				return visitorTable, nil
			}
			return nil, nil
		}
	}

	return nil, nil
}

/* 计算出分片号
Params:
	totalShardCnt: 总共有多少个分片
*/
func (this *VisitorStmt) GetShardNo(alt algorithm.Algorithm) (int, bool) {
	for _, visitorTable := range this.VisitorTableMap {
		if len(visitorTable.ShardTable.ShardCols) != len(visitorTable.ColValues) { // 字段个数和对于的值个数不一直
			continue
		}
		var canComputShardNo bool = true // 是否可以计算分片号
		values := make([]interface{}, len(visitorTable.ShardTable.ShardCols))
		for i, col := range visitorTable.ShardTable.ShardCols {
			if value, ok := visitorTable.ColValues[col]; ok {
				values[i] = value
			} else { // 字段没有这个值
				canComputShardNo = false
				break
			}
		}
		if !canComputShardNo { // 不能计算分片好则 跳过当前循环
			continue
		}

		// 计算分片号
		shardNo, err := alt.GetShardNo(values...)
		if err != nil {
			seelog.Errorf("计算分片号出错. %s", err.Error())
			continue
		}

		return shardNo, true
	}

	return 0, false
}
