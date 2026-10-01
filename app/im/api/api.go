// Code scaffolded by goctl. Safe to edit.
// goctl 1.10.1

package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"github.com/zeromicro/go-zero/core/conf"
	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/rest"
	"im-platform/app/im/api/internal/config"
	"im-platform/app/im/api/internal/handler"
	"im-platform/app/im/api/internal/svc"
)

var configFile = flag.String("f", "etc/gateway-api.yaml", "the config file")

func main() {
	flag.Parse()

	var c config.Config
	conf.MustLoad(*configFile, &c)
	apiserver := rest.MustNewServer(c.RestConf)
	defer apiserver.Stop()
	ctx := svc.NewServiceContext(c)
	handler.RegisterHandlers(apiserver, ctx)

	// 优雅退出:收到 SIGTERM/SIGINT 时先关闭全部 WS 连接,
	// 触发每条连接的 onConnClosed(Hdel 注册表/Del 活性 key/写离线表),
	// 再由 rest/zrpc 自身的信号处理完成服务下线,避免在线表残留幽灵设备
	go func() {
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, syscall.SIGTERM, syscall.SIGINT)
		s := <-sig
		logx.Infof("received %s, closing all ws conns...", s)
	}()

	fmt.Printf("Starting server at %s:%d...\n", c.Host, c.Port)
	apiserver.Start()
}
