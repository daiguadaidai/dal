package utils

import (
	"fmt"
	"testing"
)

func Test_GetBytes(t *testing.T) {
	str1 := "asdf123dafasdfasdfasdfasdfas9879a8sd7f9a8sd7f9a8sd7f9a7sd98f7a9sd8f7a9sd8f7"
	raw1, err := GetBytes(str1)
	if err != nil {
		t.Fatalf("raw1: %s. %s", str1, err.Error())
	}
	fmt.Println("raw1:", raw1, string(raw1))

	str2 := 123456
	raw2, err := GetBytes(str2)
	if err != nil {
		t.Fatalf("raw2: %d. %s", str2, err.Error())
	}
	fmt.Println("raw2:", raw2)

	str3 := "中文"
	raw3, err := GetBytes(str3)
	if err != nil {
		t.Fatalf("raw3: %s. %s", str3, err.Error())
	}
	fmt.Println("raw3:", raw3)

	str4 := []byte{1, 2, 3, 4, 5}
	raw4, err := GetBytes(str4)
	if err != nil {
		t.Fatalf("raw4: %s. %s", str4, err.Error())
	}
	fmt.Println("raw4:", raw4)

	str5 := []uint8{1, 2, 3, 4, 5}
	raw5, err := GetBytes(str5)
	if err != nil {
		t.Fatalf("raw5: %s. %s", str5, err.Error())
	}
	fmt.Println("raw5:", raw5)
}
