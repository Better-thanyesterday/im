package mq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"im-platform/app/message/rpc/models"
	"time"

	"github.com/IBM/sarama"
	"github.com/zeromicro/go-zero/core/logx"
)

// ============================================================================
// 配置（去掉冗余 Kafka 前缀，包名已是 mq）
// ============================================================================

type Config struct {
	Brokers  []string
	Producer ProducerConfig
	Consumer ConsumerConfig
}

type ProducerConfig struct {
	Acks            int // 0: NoResponse, 1: WaitForLocal, -1: WaitForAll
	RetryMax        int
	Compression     string // "lz4", "snappy", "gzip", "none"
	MaxMessageBytes int
}

type ConsumerConfig struct {
	GroupID           string
	OffsetInitial     string // "oldest" or "newest"
	SessionTimeout    time.Duration
	HeartbeatInterval time.Duration
}

// BuildSaramaConfig 构建 sarama.Config
func BuildSaramaConfig(cfg Config) (*sarama.Config, error) {
	sc := sarama.NewConfig()

	// 生产者
	sc.Producer.Return.Successes = true
	sc.Producer.Return.Errors = true
	sc.Producer.Retry.Max = cfg.Producer.RetryMax
	sc.Producer.MaxMessageBytes = cfg.Producer.MaxMessageBytes
	sc.Producer.Partitioner = sarama.NewHashPartitioner

	switch cfg.Producer.Acks {
	case 0:
		sc.Producer.RequiredAcks = sarama.NoResponse
	case 1:
		sc.Producer.RequiredAcks = sarama.WaitForLocal
	case -1:
		sc.Producer.RequiredAcks = sarama.WaitForAll
	default:
		sc.Producer.RequiredAcks = sarama.WaitForLocal
	}

	switch cfg.Producer.Compression {
	case "lz4":
		sc.Producer.Compression = sarama.CompressionLZ4
	case "snappy":
		sc.Producer.Compression = sarama.CompressionSnappy
	case "gzip":
		sc.Producer.Compression = sarama.CompressionGZIP
	default:
		sc.Producer.Compression = sarama.CompressionNone
	}

	// 消费者
	sc.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{
		sarama.NewBalanceStrategyRange(),
	}
	sc.Consumer.Offsets.AutoCommit.Enable = false // 手动提交
	sc.Consumer.Group.Session.Timeout = cfg.Consumer.SessionTimeout
	sc.Consumer.Group.Heartbeat.Interval = cfg.Consumer.HeartbeatInterval

	if cfg.Consumer.OffsetInitial == "oldest" {
		sc.Consumer.Offsets.Initial = sarama.OffsetOldest
	} else {
		sc.Consumer.Offsets.Initial = sarama.OffsetNewest
	}

	// 网络超时
	sc.Net.DialTimeout = 5 * time.Second
	sc.Net.ReadTimeout = 5 * time.Second
	sc.Net.WriteTimeout = 5 * time.Second

	return sc, nil
}

// ============================================================================
// Producer 封装（同时持有 sync + async，按需使用）
// ============================================================================

type Producer struct {
	brokers []string
	conf    *sarama.Config
	syncP   sarama.SyncProducer  // 接口值，无需 *
	asyncP  sarama.AsyncProducer // 接口值，无需 *
}

// NewProducer 创建生产者（同时初始化 sync + async）。
// 如只需一种，可拆成 NewSyncProducer / NewAsyncProducer。
func NewProducer(brokers []string, conf *sarama.Config) (*Producer, error) {
	syncP, err := sarama.NewSyncProducer(brokers, conf)
	if err != nil {
		return nil, fmt.Errorf("new sync producer: %w", err)
	}

	asyncP, err := sarama.NewAsyncProducer(brokers, conf)
	if err != nil {
		_ = syncP.Close()
		return nil, fmt.Errorf("new async producer: %w", err)
	}

	p := &Producer{
		brokers: brokers,
		conf:    conf,
		syncP:   syncP,
		asyncP:  asyncP,
	}
	go p.watchAsyncErrors()
	return p, nil
}

// 后台监听异步发送的最终失败（已重试后仍失败）
func (p *Producer) watchAsyncErrors() {
	for e := range p.asyncP.Errors() {
		logx.Errorf("kafka async publish failed, topic=%s, err=%v", e.Msg.Topic, e.Err)
	}
}

func (p *Producer) Close() error {
	var firstErr error
	if err := p.syncP.Close(); err != nil {
		firstErr = err
	}
	if err := p.asyncP.Close(); err != nil && firstErr == nil {
		firstErr = err
	}
	return firstErr
}

// -------------------- 生产函数实现 --------------------

// Publish 同步发送，等 Broker ACK。
// 返回的 err 是真实发送结果：网络错误、Broker 宕机、超时都能捕获。
// 适合 IM 消息"Kafka 失败则降级写 PG"的场景。
func (p *Producer) Publish(ctx context.Context, topic string, data []byte) error {
	_, _, err := p.syncP.SendMessage(&sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.ByteEncoder(data),
	})
	if err != nil {
		return fmt.Errorf("kafka sync publish failed: %w", err)
	}
	return nil
}

// PublishAsync 异步发送，不阻塞，立即返回。
// err 仅表示"本地入队失败"（基本不会失败），不表示 Broker 已收到。
func (p *Producer) PublishAsync(ctx context.Context, topic string, data []byte) error {
	p.asyncP.Input() <- &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.ByteEncoder(data),
	}
	return nil
}

// ============================================================================
// Consumer 封装（基于 ConsumerGroup，支持手动提交）
// ============================================================================

type MessageHandler func(ctx context.Context, msg *sarama.ConsumerMessage) error

type Consumer struct {
	group sarama.ConsumerGroup
}

func NewConsumer(brokers []string, groupID string, conf *sarama.Config) (*Consumer, error) {
	g, err := sarama.NewConsumerGroup(brokers, groupID, conf)
	if err != nil {
		return nil, fmt.Errorf("new consumer group: %w", err)
	}
	return &Consumer{group: g}, nil
}

func (c *Consumer) Close() error {
	return c.group.Close()
}

// Subscribe 阻塞式消费，直到 ctx 被取消。
// handler 返回 error 时，该消息不会提交 offset（即会重试）。
func (c *Consumer) Subscribe(ctx context.Context, topics []string, handler MessageHandler) error {
	h := &GroupHandler{Handler: handler}
	for {
		err := c.group.Consume(ctx, topics, h)
		if err != nil {
			if errors.Is(err, sarama.ErrClosedConsumerGroup) {
				return nil
			}
			logx.Errorf("consumer group consume error: %v", err)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(2 * time.Second):
				continue
			}
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
	}
}

// -------------------- sarama.ConsumerGroupHandler 实现 --------------------

type GroupHandler struct {
	Handler MessageHandler
}

func (h *GroupHandler) Setup(_ sarama.ConsumerGroupSession) error   { return nil }
func (h *GroupHandler) Cleanup(_ sarama.ConsumerGroupSession) error { return nil }

func (h *GroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		// 1. 使用带超时的 Context，避免数据库查询等阻塞操作导致心跳超时引发频繁 Rebalance
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		
		// 2. 反序列化与校验
		var message models.Messages
		if err := json.Unmarshal(msg.Value, &message); err != nil {
			logx.Errorf("json unmarshal failed, skip message: %v, raw: %s", err, string(msg.Value))
			
			// 【关键修复】：解析失败的脏数据无法通过重试恢复，必须 MarkMessage 跳过，
			// 否则会导致当前分区消费永久阻塞。生产环境建议此处发送到死信队列(DLQ)。
			session.MarkMessage(msg, "") 
			cancel() // 记得释放 context 资源
			continue 
		}

		// 3. 调用业务处理逻辑
		if err := h.Handler(ctx, msg); err != nil {
			logx.Errorf("handle message failed, topic=%s, partition=%d, offset=%d, err=%v",
				msg.Topic, msg.Partition, msg.Offset, err)
			
			// 处理失败不 Mark，让 Kafka 重试。
			// 注意：如果是不可恢复的致命错误，需引入重试上限或死信队列，防止无限重试。
			cancel() 
			continue 
		}

		// 4. 业务成功，手动标记并提交 offset
		session.MarkMessage(msg, "")
		cancel() // 释放 context 资源
	}
	return nil
}