package utils

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"testing"
)

func Test_LowConvert(t *testing.T) {
	var a int = 1111111
	b := int8(a)
	fmt.Println(b)
}

func Test_Endian(t *testing.T) {
	datas := []uint8{49, 50, 51}
	b_buf := bytes.NewBuffer(datas)
	var x int32
	binary.Read(b_buf, binary.BigEndian, &x)

	fmt.Println(x)
}

func Test_InterfaceNil(t *testing.T) {
	var inter interface{} = 1
	var inter1 interface{} = nil
	fmt.Printf("inter: %T\n", inter)
	fmt.Printf("inter1: %T\n", inter1)

	if inter1 == nil {
		fmt.Println("inter1:", inter1)
	}
}
