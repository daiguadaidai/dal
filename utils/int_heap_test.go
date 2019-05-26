package utils

import (
	"fmt"
	"testing"
)

func Test_PushAndPop(t *testing.T) {
	intHeap := NewIntHeap()
	for i := 0; i < 1; i++ {
		intHeap.Push(i * i)
	}

	for i := 0; i < 20; i++ {
		data, ok := intHeap.Pop()
		if !ok {
			fmt.Println("空 heap")
			continue
		}

		fmt.Println("Pop数据为: ", data)
	}
}
