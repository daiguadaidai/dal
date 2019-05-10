package algorithm

import (
	"fmt"
	"github.com/daiguadaidai/dal/utils"
	"hash/crc32"
)

/* 通过将字段转化为字符串进行哈希算法
 *
 */
type Crc32Hash struct{}

// 一共有分几片计算出 所在分片号
func (this *Crc32Hash) GetShardNo(shardCNT int, cols ...interface{}) (int, error) {
	if len(cols) == 0 {
		return -1, fmt.Errorf("指定字段个数为0, 无法获取到shard值")
	}

	var rawBytes []byte
	var err error
	for i, col := range cols {
		if i == 0 {
			rawBytes, err = utils.GetBytes(col)
			if err != nil {
				return -1, err
			}
		} else {
			raw, err := utils.GetBytes(col)
			if err != nil {
				return -1, err
			}
			rawBytes = append(rawBytes, raw...)
		}
	}

	// 进行crc32
	hash := crc32.ChecksumIEEE(rawBytes)

	// 值去摸
	return int(hash) % shardCNT, nil
}
