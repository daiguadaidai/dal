package utils

import (
	"bytes"
	"encoding/binary"
	"fmt"
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
