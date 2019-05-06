package algorithm

import (
	"fmt"
	"testing"
)

func Test_Crc32Hash_GetShardNum(t *testing.T) {
	var alg Algorithm = new(Crc32Hash)

	col1 := "asdfasdfasdfasdf1234567675432asdfasdfasdfasdfa0sd987fa9s8df70asd78f0asd78f"
	shardNum1, err := alg.GetShardNum(10240, col1)
	if err != nil {
		t.Fatal(err.Error())
	}
	fmt.Printf("col1 crc32 shard num: %d\n", shardNum1)

	col2 := 1
	shardNum2, err := alg.GetShardNum(10240, col2)
	if err != nil {
		t.Fatal(err.Error())
	}
	fmt.Printf("col2 crc32 shard num: %d\n", shardNum2)

	cols3 := []interface{}{int8(1), int16(2), uint8(3), 4, "123123", "231aasdf"}
	shardNum3, err := alg.GetShardNum(10240, cols3...)
	if err != nil {
		t.Fatal(err.Error())
	}
	fmt.Printf("col2 crc32 shard num: %d\n", shardNum3)
}
