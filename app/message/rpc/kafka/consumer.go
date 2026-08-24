package kafka

import (
	"context"
	"im-platform/common/mq"
	"os"
	"os/signal"
	"syscall"

	"github.com/IBM/sarama"
	"github.com/zeromicro/go-zero/core/logx"
)

type KafkaConsumerService struct {
	Consumer mq.Consumer
	Topics   []string
	Handler  mq.MessageHandler
}

func (s *KafkaConsumerService) Start() {
	if err := s.Consumer.Subscribe(context.Background(), s.Topics, s.Handler); err != nil {
		logx.Errorf("kafka consumer exited: %v", err)
	}
}

func (s *KafkaConsumerService) Stop() {
	_ = s.Consumer.Close()
}

// StartConsumer 封装了 Kafka ConsumerGroup 的完整启动逻辑
func StartConsumer(brokers []string, groupID string, topics []string, config *sarama.Config, handler sarama.ConsumerGroupHandler) {
	// 1. 创建消费者组
	client, err := sarama.NewConsumerGroup(brokers, groupID, config)
	if err != nil {
		logx.Errorf("创建消费者组失败: %v", err)
	}
	defer client.Close()

	// 2. 创建支持优雅退出的 Context
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		ch := make(chan os.Signal, 1)
		signal.Notify(ch, os.Interrupt, syscall.SIGTERM) // 增加 SIGTERM 支持容器优雅退出
		<-ch
		logx.Infof("收到退出信号，正在关闭消费者...")
		cancel()
	}()

	// 3. 启动消费循环
	for {
		err := client.Consume(ctx, topics, handler)
		if err != nil {
			logx.Infof("消费过程发生错误: %v", err)
		}
		// 如果 Context 被取消，则退出循环
		if ctx.Err() != nil {
			logx.Infof("消费者已安全退出")
			return
		}
	}
}
