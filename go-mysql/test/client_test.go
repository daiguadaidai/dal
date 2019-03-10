package test

import (
	"fmt"
	"github.com/daiguadaidai/dal/go-mysql/client"
	"testing"
)

func Test_ClientClose(t *testing.T) {
	host := "10.10.10.21"
	port := 3307
	addr := fmt.Sprintf("%s:%d", host, port)
	username := "HH"
	password := "oracle12"
	db := "employees"

	conn, err := client.Connect(addr, username, password, db)
	if err != nil {
		t.Fatal("链接数据库出错:", err.Error())
	}
	conn.SetAutoCommit()
	conn.SetCharset("utf8mb4")

	if err := conn.Ping(); err != nil {
		t.Fatal("Ping:", err.Error())
	}
	if err := conn.Close(); err != nil {
		t.Fatal("Close:", err.Error())
	}
}
