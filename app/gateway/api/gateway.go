// Code scaffolded by goctl. Safe to edit.
// goctl 1.10.1

package main

import (
	"flag"
	"fmt"
	"github.com/zeromicro/go-zero/core/conf"
	"github.com/zeromicro/go-zero/rest"
	"github.com/zeromicro/go-zero/zrpc"
	"google.golang.org/grpc"
	"im-platform/app/gateway/api/internal/config"
	"im-platform/app/gateway/api/internal/handler"
	"im-platform/app/gateway/api/internal/server"
	"im-platform/app/gateway/api/internal/svc"
	"im-platform/app/gateway/rpc/gateway"
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

	rpcserver := zrpc.MustNewServer(c.GatewayRpc, func(grpcServer *grpc.Server) {
		gateway.RegisterGatewayServer(grpcServer, server.NewGatewayServer(ctx))
	})
	defer rpcserver.Stop()
	go func ()  {
		fmt.Printf("Starting rpc server at %s...\n", c.GatewayRpc.ListenOn)
		rpcserver.Start()
	}()	
	fmt.Printf("Starting server at %s:%d...\n", c.Host, c.Port)
	apiserver.Start()
}
