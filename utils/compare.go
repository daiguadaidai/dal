package compare_sample

import (
	"strconv"
)

func InterfaceToInt64(val interface{}) (int64, error) {
	switch v := val.(type) {
	case int8:
		return int64(v), nil
	case int16:
		return int64(v), nil
	case int32:
		return int64(v), nil
	case int64:
		return int64(v), nil
	case int:
		return int64(v), nil
	case uint8:
		return int64(v), nil
	case uint16:
		return int64(v), nil
	case uint32:
		return int64(v), nil
	case uint64:
		return int64(v), nil
	case uint:
		return int64(v), nil
	case string:
		return strconv.ParseInt(v, 10, 64)
	case []byte:
		return strconv.ParseInt(string(v), 10, 64)
	}
	return 0, nil
}

func InterfaceToUint64(val interface{}) (uint64, error) {
	switch v := val.(type) {
	case int8:
		return uint64(v), nil
	case int16:
		return uint64(v), nil
	case int32:
		return uint64(v), nil
	case int64:
		return uint64(v), nil
	case int:
		return uint64(v), nil
	case uint8:
		return uint64(v), nil
	case uint16:
		return uint64(v), nil
	case uint32:
		return uint64(v), nil
	case uint64:
		return uint64(v), nil
	case uint:
		return uint64(v), nil
	case string:
		return strconv.ParseUint(v, 10, 64)
	case []byte:
		return strconv.ParseUint(string(v), 10, 64)
	}
	return 0, nil
}

func InterfaceToStr(val interface{}) string {
	switch v := val.(type) {
	case int8:
		return strconv.FormatInt(int64(v), 10)
	case int16:
		return strconv.FormatInt(int64(v), 10)
	case int32:
		return strconv.FormatInt(int64(v), 10)
	case int64:
		return strconv.FormatInt(v, 10)
	case int:
		return strconv.FormatInt(int64(v), 10)
	case uint8:
		return strconv.FormatUint(uint64(v), 10)
	case uint16:
		return strconv.FormatUint(uint64(v), 10)
	case uint32:
		return strconv.FormatUint(uint64(v), 10)
	case uint64:
		return strconv.FormatUint(v, 10)
	case uint:
		return strconv.FormatUint(uint64(v), 10)
	case string:
		return v
	case []byte:
		return string(v)
	}
	return ""
}

// 小于
func Less(v1 interface{}, v2 interface{}) bool {
	if v1 == nil || v2 == nil {
		return false
	}
	switch v := v1.(type) {
	case int8, int16, int32, int64, int:
		data1, err := InterfaceToInt64(v1)
		if err != nil {
			return false
		}
		data2, err := InterfaceToInt64(v2)
		if err != nil {
			return false
		}
		return data1 < data2
	case uint8, uint16, uint32, uint64, uint:
		data1, err := InterfaceToUint64(v1)
		if err != nil {
			return false
		}
		data2, err := InterfaceToUint64(v2)
		if err != nil {
			return false
		}
		return data1 < data2
	case string:
		return v < InterfaceToStr(v2)
	case []byte:
		return string(v) < InterfaceToStr(v2)
	}
	return false
}

// 小于等于
func LessEqual(v1 interface{}, v2 interface{}) bool {
	if v1 == nil || v2 == nil {
		return false
	}
	switch v := v1.(type) {
	case int8, int16, int32, int64, int:
		data1, err := InterfaceToInt64(v1)
		if err != nil {
			return false
		}
		data2, err := InterfaceToInt64(v2)
		if err != nil {
			return false
		}
		return data1 <= data2
	case uint8, uint16, uint32, uint64, uint:
		data1, err := InterfaceToUint64(v1)
		if err != nil {
			return false
		}
		data2, err := InterfaceToUint64(v2)
		if err != nil {
			return false
		}
		return data1 <= data2
	case string:
		return v <= InterfaceToStr(v2)
	case []byte:
		return string(v) <= InterfaceToStr(v2)
	}
	return false
}

// 大于
func Rather(v1 interface{}, v2 interface{}) bool {
	if v1 == nil || v2 == nil {
		return false
	}
	switch v := v1.(type) {
	case int8, int16, int32, int64, int:
		data1, err := InterfaceToInt64(v1)
		if err != nil {
			return false
		}
		data2, err := InterfaceToInt64(v2)
		if err != nil {
			return false
		}
		return data1 > data2
	case uint8, uint16, uint32, uint64, uint:
		data1, err := InterfaceToUint64(v1)
		if err != nil {
			return false
		}
		data2, err := InterfaceToUint64(v2)
		if err != nil {
			return false
		}
		return data1 > data2
	case string:
		return v > InterfaceToStr(v2)
	case []byte:
		return string(v) > InterfaceToStr(v2)
	}
	return false
}

// 大于等于
func RatherEqual(v1 interface{}, v2 interface{}) bool {
	if v1 == nil || v2 == nil {
		return false
	}
	switch v := v1.(type) {
	case int8, int16, int32, int64, int:
		data1, err := InterfaceToInt64(v1)
		if err != nil {
			return false
		}
		data2, err := InterfaceToInt64(v2)
		if err != nil {
			return false
		}
		return data1 >= data2
	case uint8, uint16, uint32, uint64, uint:
		data1, err := InterfaceToUint64(v1)
		if err != nil {
			return false
		}
		data2, err := InterfaceToUint64(v2)
		if err != nil {
			return false
		}
		return data1 >= data2
	case string:
		return v >= InterfaceToStr(v2)
	case []byte:
		return string(v) >= InterfaceToStr(v2)
	}
	return false
}

// 等于
func Equal(v1 interface{}, v2 interface{}) bool {
	if v1 == nil || v2 == nil {
		return false
	}
	switch v := v1.(type) {
	case int8, int16, int32, int64, int:
		data1, err := InterfaceToInt64(v1)
		if err != nil {
			return false
		}
		data2, err := InterfaceToInt64(v2)
		if err != nil {
			return false
		}
		return data1 == data2
	case uint8, uint16, uint32, uint64, uint:
		data1, err := InterfaceToUint64(v1)
		if err != nil {
			return false
		}
		data2, err := InterfaceToUint64(v2)
		if err != nil {
			return false
		}
		return data1 == data2
	case string:
		return v == InterfaceToStr(v2)
	case []byte:
		return string(v) == InterfaceToStr(v2)
	}
	return false
}

// 不等于
func NotEqual(v1 interface{}, v2 interface{}) bool {
	if v1 == nil || v2 == nil {
		return false
	}
	switch v := v1.(type) {
	case int8, int16, int32, int64, int:
		data1, err := InterfaceToInt64(v1)
		if err != nil {
			return false
		}
		data2, err := InterfaceToInt64(v2)
		if err != nil {
			return false
		}
		return data1 != data2
	case uint8, uint16, uint32, uint64, uint:
		data1, err := InterfaceToUint64(v1)
		if err != nil {
			return false
		}
		data2, err := InterfaceToUint64(v2)
		if err != nil {
			return false
		}
		return data1 != data2
	case string:
		return v != InterfaceToStr(v2)
	case []byte:
		return string(v) != InterfaceToStr(v2)
	}
	return false
}

// 为空
func IsNull(v interface{}) bool {
	return v == nil
}
