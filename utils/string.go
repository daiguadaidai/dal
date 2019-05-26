package utils

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"strconv"
	"strings"
)

func GetBytes(data interface{}) ([]byte, error) {
	switch val := data.(type) {
	case int8, int16, int32, int64, uint8, uint16, uint32, uint64:
		return KindIntToByte(data)
	case int:
		return Int64ToByte(int64(val))
	case uint:
		return Uint64ToByte(uint64(val))
	case string:
		return []byte(val), nil
	case []byte:
		return data.([]byte), nil
	default:
	}

	return nil, fmt.Errorf("未知数据类型%T, 转为[]byte. 数据为: %v", data, data)
}

func Int64ToByte(num int64) ([]byte, error) {
	var buffer bytes.Buffer
	if err := binary.Write(&buffer, binary.LittleEndian, num); err != nil {
		return nil, err
	}
	return buffer.Bytes(), nil
}

func Uint64ToByte(num uint64) ([]byte, error) {
	var buffer bytes.Buffer
	if err := binary.Write(&buffer, binary.LittleEndian, num); err != nil {
		return nil, err
	}
	return buffer.Bytes(), nil
}

func KindIntToByte(num interface{}) ([]byte, error) {
	var buffer bytes.Buffer
	if err := binary.Write(&buffer, binary.LittleEndian, num); err != nil {
		return nil, err
	}
	return buffer.Bytes(), nil
}

/* 分片好字符串转化为map
 * Example:
 * str: 1,2,3,4, 10-15, 20-23-20-27
 * return: map[1:{} 2:{} 3:{} 4:{} 10:{} 11:{} 12:{} 13:{} 14:{} 15:{} 20:{} 21:{} 22:{} 23:{} 24:{} 25:{} 26:{} 27:{}]
 */
func ShardNoStrsToIntMap(str string) map[int]struct{} {
	shardNoMap := make(map[int]struct{})

	str = strings.TrimSpace(str)
	if len(str) == 0 {
		return shardNoMap
	}

	items := strings.Split(str, ",")
	for _, item := range items {
		numStrs := strings.Split(strings.TrimSpace(item), "-")
		if len(numStrs) == 1 {
			num, err := strconv.ParseInt(strings.TrimSpace(numStrs[0]), 10, 64)
			if err != nil {
				continue
			}
			shardNoMap[int(num)] = struct{}{}

		} else if len(numStrs) > 1 {
			min, err := strconv.ParseInt(strings.TrimSpace(numStrs[0]), 10, 64)
			if err != nil {
				continue
			}
			var max int64
			for _, numStr := range numStrs {
				num, err := strconv.ParseInt(strings.TrimSpace(numStr), 10, 64)
				if err != nil {
					continue
				}
				if num < min {
					min = num
				}
				if num > max {
					max = num
				}
			}
			for i := int(min); i <= int(max); i++ {
				shardNoMap[i] = struct{}{}
			}
		}
	}

	return shardNoMap
}

/* 获得缩进
Params:
	depth: 缩进多少
	placeholder: 缩进的符号
	multi: 一个缩进要多少个符号
*/
func GetIntend(depth int, placeholder string, multi int) string {
	return strings.Repeat(placeholder, depth*multi)
}

// 组装表名
func ConcatTableName(schema *string, table *string) string {
	if schema != nil && *schema != "" {
		return fmt.Sprintf("%s.%s", *schema, *table)
	}

	return *table
}

// 获取shardtable的key
func GetShardTableKey(defaultSchema *string, schema *string, name *string) string {
	if *schema != "" {
		return ConcatTableName(schema, name)
	}

	return ConcatTableName(defaultSchema, name)
}

// 获取 schema.table
func GetConcatSchemAndTableKey(defaultSchema *string, schema *string, table *string, alias *string) string {
	newSchema, newTable := GetSchemaAndTable(defaultSchema, schema, table, alias)
	return ConcatTableName(&newSchema, &newTable)
}

// 获取schema 和 table
func GetSchemaAndTable(defaultSchema *string, schema *string, table *string, alias *string) (string, string) {
	return GetSchemaName(defaultSchema, schema), GetTableName(table, alias)
}

// 获取 字段 schema.table.col
func GetConcatColumn(schema string, table string, col string) string {
	if schema == "" {
		return GetConcatColumnWitchTableAndCol(table, col)
	}

	return fmt.Sprintf("%s.%s.%s", schema, table, col)
}

// 获取 字段 table.col
func GetConcatColumnWitchTableAndCol(table string, col string) string {
	if table == "" {
		return col
	}

	return fmt.Sprintf("%s.%s", table, col)
}

// 获取 数据库 名
func GetSchemaName(defaultName, dbName *string) string {
	if dbName != nil && *dbName != "" {
		return *dbName
	}
	return *defaultName
}

// 获取表别名
func GetTableName(ori, alias *string) string {
	if alias != nil && *alias != "" {
		return *alias
	}
	return *ori
}
