// Code scaffolded by goctl. Safe to edit.
// goctl 1.10.1

package main

import (
	"flag"
	"fmt"

	"github.com/zeromicro/go-zero/core/conf"
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

	// 本进程是纯 HTTP API 层,无 WS 连接需要管理;
	// 优雅退出由 rest 自带的信号处理完成,不额外起 goroutine
	fmt.Printf("Starting server at %s:%d...\n", c.Host, c.Port)
	apiserver.Start()
}
