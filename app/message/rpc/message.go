package main

import (
	"flag"
	"fmt"

	"im-platform/app/message/rpc/internal/config"
	"im-platform/app/message/rpc/internal/logic"
	"im-platform/app/message/rpc/internal/server"
	"im-platform/app/message/rpc/internal/svc"
	"im-platform/app/message/rpc/kafka"
	"im-platform/app/message/rpc/message"
	"im-platform/common/mq"

	"github.com/zeromicro/go-zero/core/conf"
	"github.com/zeromicro/go-zero/core/service"
	"github.com/zeromicro/go-zero/zrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

var configFile = flag.String("f", "etc/message.yaml", "the config file")

func main() {
	flag.Parse()

	var c config.Config
	conf.MustLoad(*configFile, &c)
	ctx := svc.NewServiceContext(c)
	s := zrpc.MustNewServer(c.RpcServerConf, func(grpcServer *grpc.Server) {
		message.RegisterMessageServer(grpcServer, server.NewMessageServer(ctx))

		if c.Mode == service.DevMode || c.Mode == service.TestMode {
			reflection.Register(grpcServer)
		}
	})

	// 2. Kafka 消费者：订阅 im.msg.persist 做消息落库
	consumer := &kafka.KafkaConsumerService{
		Consumer: *ctx.KafkaConsumer,
		Topics:   []string{mq.TopicMsgPersist},
		Handler:  logic.NewConsumerHandlerLogic(ctx).PersistMsg,
	}
	// 3. servicegroup：Ctrl+C 时先停消费者再停 rpc
	sg := service.NewServiceGroup()
	sg.Add(s)
	sg.Add(consumer)
	defer sg.Stop()

	fmt.Printf("Starting rpc server at %s...\n", c.ListenOn)
	sg.Start()
}
