package main

import (
	"flag"
	"fmt"

	"im-platform/app/gateway/rpc/gateway"
	"im-platform/app/gateway/rpc/internal/config"
	"im-platform/app/gateway/rpc/internal/handler"
	"im-platform/app/gateway/rpc/internal/server"
	"im-platform/app/gateway/rpc/internal/svc"

	"github.com/zeromicro/go-zero/core/conf"
	"github.com/zeromicro/go-zero/core/service"
	"github.com/zeromicro/go-zero/rest"
	"github.com/zeromicro/go-zero/zrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

var configFile = flag.String("f", "etc/gateway.yaml", "the config file")

func main() {
	flag.Parse()

	var c config.Config
	conf.MustLoad(*configFile, &c)
	ctx := svc.NewServiceContext(c)

	// HTTP/WS 入口:/ws 路由 + 鉴权中间件(routes.go),客户端 WebSocket 连这里
	restServer := rest.MustNewServer(c.RestConf)
	defer restServer.Stop()
	handler.RegisterHandlers(restServer, ctx)

	// gRPC 入口:供 push 服务调 PushToConn/BatchPushToConn
	s := zrpc.MustNewServer(c.GatewayRpc, func(grpcServer *grpc.Server) {
		gateway.RegisterGatewayServer(grpcServer, server.NewGatewayServer(ctx))

		if c.GatewayRpc.Mode == service.DevMode || c.GatewayRpc.Mode == service.TestMode {
			reflection.Register(grpcServer)
		}
	})
	defer s.Stop()

	// 双服务一起起(service group 统一管理生命周期,Ctrl+C 时一起退)
	sg := service.NewServiceGroup()
	defer sg.Stop()
	sg.Add(restServer)
	sg.Add(s)

	fmt.Printf("Starting gateway (ws http on %s:%d, rpc on %s)...\n",
		c.RestConf.Host, c.RestConf.Port, c.GatewayRpc.ListenOn)
	sg.Start()
}
