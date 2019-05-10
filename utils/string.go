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
