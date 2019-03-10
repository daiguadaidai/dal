package pool

import (
	"fmt"
	"github.com/cihub/seelog"
	"github.com/daiguadaidai/dal/go-mysql/client"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	DB_CONFIG_USERNAME = "root"
	DB_CONFIG_PASSWORD = ""
	DB_CONFIG_HOST     = "127.0.0.1"
	DB_CONFIG_PORT     = 3306
	DB_CONFIG_CHARSET  = "utf8"
)

type dbConfig struct {
	Username     string
	Password     string
	Host         string
	Port         uint16
	DBName       string
	Charset      string
	IsAutoCommit bool
}

func NewDBConfig(
	host string,
	port uint16,
	username string,
	password string,
	dbName string,
	charset string,
	isAutoCommit bool,
) *dbConfig {
	cfg := new(dbConfig)
	// 初始化 host
	if len(strings.TrimSpace(host)) == 0 { // 没有指定 host
		seelog.Warnf("没有指定host. 使用默认 host:%s", DB_CONFIG_HOST)
		cfg.Host = DB_CONFIG_HOST
	} else {
		cfg.Host = host
	}

	// 初始化 port
	if port < 1 { // port 小于1
		seelog.Warnf("指定的Port:%d, 小于1, 使用默认 port:%d", port, DB_CONFIG_PORT)
		cfg.Port = DB_CONFIG_PORT
	} else {
		cfg.Port = port
	}

	// 初始化 username
	if len(strings.TrimSpace(username)) == 0 {
		seelog.Warnf("没有指定username, 使用默认 username:%s", DB_CONFIG_USERNAME)
		cfg.Username = DB_CONFIG_USERNAME
	} else {
		cfg.Username = username
	}

	// 初始化 password
	if len(strings.TrimSpace(password)) == 0 {
		seelog.Warnf("没有指定password, 使用默认 password:%s", DB_CONFIG_PASSWORD)
		cfg.Password = DB_CONFIG_PASSWORD
	} else {
		cfg.Password = password
	}

	// 初始化 charset
	if len(strings.TrimSpace(charset)) == 0 {
		seelog.Warnf("没有指定charset, 使用默认 charset:%s", DB_CONFIG_CHARSET)
		cfg.Charset = DB_CONFIG_CHARSET
	} else {
		cfg.Charset = charset
	}

	// 初始化 autotommit
	if !isAutoCommit {
		seelog.Warnf("指定 autocommit=0. (一般应用使用的是 autocommit=1, 请谨慎考虑.)")
	}
	cfg.IsAutoCommit = isAutoCommit

	cfg.DBName = dbName

	seelog.Debugf("链接配置为: %s", cfg.String())
	return cfg
}

func (this *dbConfig) addr() string {
	return fmt.Sprintf("%s:%d", this.Host, this.Port)
}

func (this *dbConfig) String() string {
	return fmt.Sprintf("host: %s, port: %d, username: %s, password: %s, charset: %s, isAutoCommit: %t",
		this.Host, this.Port, this.Username, this.Password, this.Charset, this.IsAutoCommit)
}

const (
	MYSQL_POOL_MIN_OPEN       = 1
	MYSQL_POOL_MAX_OPEN       = 1
	MYSQL_POOL_MAX_OPEN_LIMIT = 1000
)

type MySQLPool struct {
	sync.Mutex
	connChan  chan *client.Conn
	threadMap sync.Map // key:thread id. value: 链接使用时间戳
	cfg       *dbConfig
	minOpen   int32
	maxOpen   int32
	numOpen   int32
}

func Open(
	host string,
	port uint16,
	username string,
	password string,
	DBName string,
	charset string,
	isAutoCommit bool,
	minOpen int32,
	maxOpen int32,
) (*MySQLPool, error) {
	cfg := NewDBConfig(host, port, username, password, DBName, charset, isAutoCommit)

	p := new(MySQLPool)
	p.cfg = cfg
	p.connChan = make(chan *client.Conn, MYSQL_POOL_MAX_OPEN_LIMIT)

	// 初始化 链接打开最小值
	if minOpen < 1 {
		p.minOpen = MYSQL_POOL_MIN_OPEN
		seelog.Warnf("最小使用链接数不能小于1, 默认值:%d", MYSQL_POOL_MIN_OPEN)
	} else {
		p.minOpen = minOpen
	}

	// 初始化 链接打开最大值
	if maxOpen < 1 {
		p.maxOpen = MYSQL_POOL_MAX_OPEN
		seelog.Warnf("最大使用链接数不能小于1, 默认值:%d", MYSQL_POOL_MIN_OPEN)
	} else {
		p.maxOpen = maxOpen
	}

	// 判断最大链接数是否大于系统允许的最大链接
	if maxOpen > MYSQL_POOL_MAX_OPEN_LIMIT {
		p.maxOpen = MYSQL_POOL_MAX_OPEN_LIMIT
		seelog.Warnf("指定最大链接数:%d, 大于系统允许最大连接数:%d, 默认设置最大链接数为:%d",
			maxOpen, MYSQL_POOL_MAX_OPEN_LIMIT, MYSQL_POOL_MAX_OPEN_LIMIT)
	}

	// 最小链接数不能大于最大链接数
	if p.minOpen > p.maxOpen {
		p.minOpen = p.maxOpen
		seelog.Warnf("最小链接数:%d 大于 最大链接数:%d. 设置最小链接为:%d",
			p.minOpen, p.maxOpen, p.maxOpen)
	}

	return p, nil
}

// 关闭连接池
func (this *MySQLPool) Close() {
	close(this.connChan)
	for conn := range this.connChan {
		this.Lock()
		if err := this.closeConn(conn); err != nil {
			seelog.Errorf("链接(thread id): %d. 关闭失败. %s", conn.GetConnectionID(), err.Error())
		}
		this.Unlock()
	}
}

func (this *MySQLPool) incrNumOpen() {
	atomic.AddInt32(&this.numOpen, 1)
}

func (this *MySQLPool) decrNumOpen() {
	atomic.AddInt32(&this.numOpen, -1)
}

// 关闭指定链接 // 序号在获取 mutex lock 使用, 不然会出现死锁
func (this *MySQLPool) closeConn(conn *client.Conn) error {
	threadID := conn.GetConnectionID()

	err := conn.Close()

	this.decrNumOpen()
	this.deleteThreadMapItem(threadID)

	return err
}

// 删除 thread id map 元素
func (this *MySQLPool) deleteThreadMapItem(threadID uint32) {
	if val, ok := this.threadMap.Load(threadID); ok {
		startTimestamp := val.(int64)
		currentTimstamp := time.Now().Unix()
		seelog.Infof("%s. thread id:%d, 运行了%d秒",
			this.cfg.addr(), threadID, currentTimstamp-startTimestamp)
	} else {
		seelog.Infof("%s. thread id:%d. 已经不存在")
	}
	this.threadMap.Delete(threadID)
}

// 获取链接
func (this *MySQLPool) Get() (*client.Conn, error) {
	// 先从chan中获取资源
	select {
	case conn, ok := <-this.connChan:
		if ok {
			return conn, nil
		}
	default:
	}

	this.Lock()
	// 等待获取资源
	if this.NumOpen() >= this.maxOpen {
		this.Unlock()
		conn := <-this.connChan
		return conn, nil
	}

	// 新键资源
	this.incrNumOpen() // 添加已经使用资源
	// 新键链接
	conn, err := client.Connect(this.cfg.addr(), this.cfg.Username, this.cfg.Password, this.cfg.DBName)
	if err != nil {
		this.Unlock()
		this.decrNumOpen() // 链接没有成功删除已经使用资源
		return nil, fmt.Errorf("链接数据库出错: %s", err.Error())
	}

	// 设置链接开始使用时间戳
	this.threadMap.Store(conn.GetConnectionID(), time.Now().Unix())

	// 链接设置
	if err = conn.SetAutoCommit(this.cfg.IsAutoCommit); err != nil {
		this.closeConn(conn)
		this.Unlock()
		return nil, fmt.Errorf("(新建链接)执行 set autocommit: %t 出错. %s",
			this.cfg.IsAutoCommit, err.Error())
	}

	// 设置链接的 charset
	if err = conn.SetCharset(this.cfg.Charset); err != nil {
		this.closeConn(conn)
		this.Unlock()
		return nil, fmt.Errorf("(新建链接)执行 set names %s 出错. %s",
			this.cfg.Charset, err.Error())
	}
	this.Unlock()

	return conn, nil
}

// 归还链接
func (this *MySQLPool) Release(conn *client.Conn) error {
	this.Lock()

	if this.NumOpen() > this.maxOpen { // 关闭资源
		this.closeConn(conn)
		this.Unlock()
		return nil
	}

	this.Unlock()

	this.connChan <- conn
	return nil
}

// 获取允许最大打开数
func (this *MySQLPool) MaxOpen() int32 {
	return this.maxOpen
}

// 获取允许最小打开数
func (this *MySQLPool) MinOpen() int32 {
	return this.minOpen
}

// 当前已经打开的数量
func (this *MySQLPool) NumOpen() int32 {
	return atomic.LoadInt32(&this.numOpen)
}

// 设置最大允许的链接数
func (this *MySQLPool) SetMaxOpen(maxOpen int32) error {
	if maxOpen > MYSQL_POOL_MAX_OPEN_LIMIT {
		return fmt.Errorf("设置最大允许链接数:%d, 超过了系统限制:%d", maxOpen, MYSQL_POOL_MAX_OPEN_LIMIT)
	}

	atomic.StoreInt32(&this.maxOpen, maxOpen)
	return nil
}
