package pool

import (
	"fmt"
	"github.com/daiguadaidai/dal/go-mysql/client"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

var host string = "10.10.10.21"
var port uint16 = 3307
var username string = "HH"
var password string = "oracle12"
var db string = "employees"
var charset string = "utf8mb4"
var isAutoCommit bool = true
var poolMinOpen int32 = 2
var poolMaxOpen int32 = 100

func Test_Client(t *testing.T) {
	addr := fmt.Sprintf("%s:%d", host, port)

	conn, err := client.Connect(addr, username, password, db)
	if err != nil {
		t.Fatal("链接数据库出错:", err.Error())
	}
	conn.SetAutoCommit(isAutoCommit)
	conn.SetCharset(charset)

	if err := conn.Ping(); err != nil {
		t.Fatal("Ping:", err.Error())
	}
	conn.Close()
}

//  测试mysql pool 的使用
func Test_MySQLPool(t *testing.T) {
	p, err := Open(host, port, username, password, db, charset, isAutoCommit, poolMinOpen, poolMaxOpen)
	if err != nil {
		t.Fatal("创建MySQL连接池失败", err.Error())
	}

	conn, err := p.Get()
	if err != nil {
		t.Fatal("获取链接失败", err.Error())
	}
	fmt.Println("threadID", conn.GetConnectionID())

	fmt.Printf("GET: minOpen:%d, maxOpen:%d, numOpen:%d\n", p.MinOpen(), p.MaxOpen(), p.NumOpen())
	p.Release(conn)
	fmt.Printf("Release: minOpen:%d, maxOpen:%d, numOpen:%d\n", p.MinOpen(), p.MaxOpen(), p.NumOpen())
	p.Close()
	fmt.Printf("Close: minOpen:%d, maxOpen:%d, numOpen:%d\n", p.MinOpen(), p.MaxOpen(), p.NumOpen())
}

var getCount int64

//  测试mysql pool 的并使用
func Test_MySQLPool_Paraller(t *testing.T) {
	p, err := Open(host, port, username, password, db, charset, isAutoCommit, poolMinOpen, poolMaxOpen)
	if err != nil {
		t.Fatal("创建MySQL连接池失败", err.Error())
	}

	wg := new(sync.WaitGroup)
	for i := 0; i < 1000; i++ {
		wg.Add(1)
		go getConnLoop(wg, p, 100000000)
	}
	wg.Wait()

	p.Close()
}

// 循环获取链接
func getConnLoop(wg *sync.WaitGroup, p *MySQLPool, loopCNT int) {
	defer wg.Done()
	for i := 0; i < loopCNT; i++ {
		conn, err := p.Get()
		if err != nil {
			fmt.Println(err.Error())
			continue
		}
		atomic.AddInt64(&getCount, 1)
		fmt.Printf("%d. minOpen:%d, maxOpen:%d, numOpen:%d\n", getCount, p.MinOpen(), p.MaxOpen(), p.NumOpen())
		p.Release(conn)
	}
}

//  测试mysql pool 的并使用
func Test_MySQLPool_SetMaxOpen(t *testing.T) {
	p, err := Open(host, port, username, password, db, charset, isAutoCommit, poolMinOpen, poolMaxOpen)
	if err != nil {
		t.Fatal("创建MySQL连接池失败", err.Error())
	}

	wg := new(sync.WaitGroup)
	for i := 0; i < 1000; i++ {
		wg.Add(1)
		go getConnLoop(wg, p, 100000000)
	}

	wg.Add(1)
	go func(wg *sync.WaitGroup, pool *MySQLPool) {
		defer wg.Done()
		for i := 0; i < 10; i++ {
			time.Sleep(10 * time.Second)
			rand.Seed(time.Now().UnixNano()) // 随机数种子
			num := rand.Int31n(100)
			fmt.Printf("设置打开链接数为:%d\n", num)
			if err := p.SetMaxOpen(num); err != nil {
				fmt.Println(err.Error())
			}
		}
	}(wg, p)

	wg.Wait()

	p.Close()
}
