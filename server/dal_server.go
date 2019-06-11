package server

import (
	"github.com/cihub/seelog"
	"github.com/daiguadaidai/dal/dal_context"
	"github.com/daiguadaidai/dal/go-mysql/server"
	"github.com/daiguadaidai/dal/handler"
	"net"
)

func StartDal(ctx *dal_context.DalContext) {
	l, _ := net.Listen("tcp", ctx.ServerCtx.Addr())

	// 循环监听 端口, 并且为每一个链接创建处理器
	for i := 0; ; i++ {
		conn, _ := l.Accept()
		go createHandler(conn, ctx)
	}
}

// 创建一个处理器
func createHandler(c net.Conn, ctx *dal_context.DalContext) {
	dalHandler := handler.NewDalHadler(ctx)
	conn, err := server.NewConn(c, ctx.ServerCtx.Username, ctx.ServerCtx.Password, dalHandler)
	if err != nil {
		seelog.Errorf("建立链接失败, 客户端[%s], Dal服务[%s]. %s", c.RemoteAddr(), c.LocalAddr(), err.Error())
		return
	}
	if conn == nil {
		seelog.Errorf("建立链接失败, 客户端[%s], Dal服务[%s] 获取到的链接为空", c.RemoteAddr(), c.LocalAddr())
		return
	}
	seelog.Infof("建立链接成功, 客户端[%s], Dal服务[%s]", c.RemoteAddr(), c.LocalAddr())

	for i := 0; ; i++ {
		if err := conn.HandleCommand(); err != nil {
			seelog.Error(err.Error())
			return
		}
		// fmt.Println("=== ", i, " ===. conn closed:", conn.Closed())
		if conn.Closed() {
			seelog.Infof("关闭链接, 客户端[%s], Dal服务[%s]", c.RemoteAddr(), c.LocalAddr())
			if err = dalHandler.Clean(); err != nil {
				seelog.Error(err.Error())
			}
			break
		}
	}
}
