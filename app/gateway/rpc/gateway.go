package main

import (
	"flag"
	"fmt"

	"im-platform/app/gateway/rpc/gateway"
	"im-platform/app/gateway/rpc/internal/config"
	"im-platform/app/gateway/rpc/internal/server"
	"im-platform/app/gateway/rpc/internal/svc"

	"github.com/zeromicro/go-zero/core/conf"
	"github.com/zeromicro/go-zero/core/service"
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

	s := zrpc.MustNewServer(c.GatewayRpc, func(grpcServer *grpc.Server) {
		gateway.RegisterGatewayServer(grpcServer, server.NewGatewayServer(ctx))

		if c.GatewayRpc.Mode == service.DevMode || c.GatewayRpc.Mode == service.TestMode {
			reflection.Register(grpcServer)
		}
	})
	defer s.Stop()
	go func ()  {
		fmt.Printf("Starting rpc server at %s...\n", c.GatewayRpc.ListenOn)
		s.Start()
	}()

	fmt.Printf("Starting rpc server at %s...\n", c.GatewayRpc.ListenOn)
	s.Start()
}
