package utils

type IntNode struct {
	Data int
	Next *IntNode
}

func NewIntNode(data int) *IntNode {
	return &IntNode{Data: data}
}

type IntHeap struct {
	size int
	top  *IntNode
}

func NewIntHeap() *IntHeap {
	return &IntHeap{}
}

func (this *IntHeap) Push(data int) {
	newNode := NewIntNode(data)
	if this.top == nil { // 空队列
		this.top = newNode
		this.size++
		return
	}

	// 非空队列
	tmpNode := this.top
	this.top = newNode
	newNode.Next = tmpNode
	this.size++
}

func (this *IntHeap) Pop() (int, bool) {
	// 空队列
	if this.size == 0 {
		return 0, false
	}

	// 非空队列
	data := this.top.Data
	this.top = this.top.Next
	this.size--

	return data, true
}

func (this *IntHeap) Size() int {
	return this.size
}
