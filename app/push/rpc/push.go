package main

import (
	"context"
	"flag"
	"fmt"

	"im-platform/app/push/rpc/internal/config"
	"im-platform/app/push/rpc/internal/logic"
	"im-platform/app/push/rpc/internal/server"
	"im-platform/app/push/rpc/internal/svc"
	"im-platform/app/push/rpc/push"
	"im-platform/common/mq"

	"github.com/zeromicro/go-zero/core/conf"
	"github.com/zeromicro/go-zero/core/service"
	"github.com/zeromicro/go-zero/zrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

var configFile = flag.String("f", "etc/push.yaml", "the config file")

// eventConsumerService 事件广播消费者,适配 service group 生命周期
type eventConsumerService struct {
	Consumer mq.Consumer
	Topics   []string
	Handler  mq.MessageHandler
}

func (s *eventConsumerService) Start() {
	if err := s.Consumer.Subscribe(context.Background(), s.Topics, s.Handler); err != nil {
		fmt.Printf("event consumer exited: %v\n", err)
	}
}

func (s *eventConsumerService) Stop() {
	_ = s.Consumer.Close()
}

func main() {
	flag.Parse()

	var c config.Config
	conf.MustLoad(*configFile, &c)
	ctx := svc.NewServiceContext(c)

	s := zrpc.MustNewServer(c.RpcServerConf, func(grpcServer *grpc.Server) {
		push.RegisterPushServer(grpcServer, server.NewPushServer(ctx))

		if c.Mode == service.DevMode || c.Mode == service.TestMode {
			reflection.Register(grpcServer)
		}
	})
	defer s.Stop()

	// im.event.notify 消费者:上下线/群变更/撤回/大群轻量通知的统一分发口
	sg := service.NewServiceGroup()
	defer sg.Stop()
	sg.Add(s)

	consumers, err := svc.NewEventConsumer(c)
	if err != nil {
		panic(fmt.Sprintf("event consumer init failed: %v", err))
	}
	sg.Add(&eventConsumerService{
		Consumer: *consumers[0],
		Topics:   []string{mq.TopicEventNotify},
		Handler:  logic.NewEventNotifyLogic(ctx).Handle,
	})

	fmt.Printf("Starting rpc server at %s...\n", c.ListenOn)
	sg.Start()
}
