package svc

import (
	"im-platform/app/group/rpc/groupclient"
	"im-platform/app/push/rpc/internal/config"
	userclient "im-platform/app/user/rpc/userclient"
	"im-platform/common/mq"

	"github.com/zeromicro/go-zero/core/stores/redis"
	"github.com/zeromicro/go-zero/zrpc"
)

const gatewaysSetKey = "im:gateways" // 存活 gateway 地址集合(各实例启动时自注册)

type ServiceContext struct {
	Config config.Config
	Redis  *redis.Redis
	// GatewayPool 直连 gateway:投递目标来自 Redis 在线表里的 gateway 地址,
	// 不走 etcd 服务发现(嵌入式 zrpc gateway client 是死代码,已删)
	GatewayPool *GatewayPool
	userclient.User
	groupclient.Group
}

func NewServiceContext(c config.Config) *ServiceContext {
	rds := redis.MustNewRedis(c.RedisCache)
	return &ServiceContext{
		Config:      c,
		Redis:       rds,
		GatewayPool: NewGatewayPool(),
		User:        userclient.NewUser(zrpc.MustNewClient(c.UserRpc)),
		Group:       groupclient.NewGroup(zrpc.MustNewClient(c.GroupRpc)),
	}
}

// KnownGatewayAddrs 事件扇出的目标 gateway 地址集合:
// 各 gateway 实例建立首条连接/启动时 Sadd 自注册(im:gateways),
// 崩溃残留地址由 push 建连失败 + Evict 自然剔除(仅日志噪音,不影响正确性)
func (s *ServiceContext) KnownGatewayAddrs() []string {
	addrs, err := s.Redis.Smembers(gatewaysSetKey)
	if err != nil || len(addrs) == 0 {
		return nil
	}
	return addrs
}

// RegisterGatewayAddr gateway 自注册(供 gateway 服务调用)
func (s *ServiceContext) RegisterGatewayAddr(addr string) error {
	_, err := s.Redis.Sadd(gatewaysSetKey, addr)
	return err
}

// UnregisterGatewayAddr gateway 优雅退出时移除
func (s *ServiceContext) UnregisterGatewayAddr(addr string) error {
	_, err := s.Redis.Srem(gatewaysSetKey, addr)
	return err
}

// NewEventConsumer 事件广播消费者(由 main 挂到 service group)
func NewEventConsumer(c config.Config) ([]*mq.Consumer, error) {
	saramaCfg, err := mq.BuildSaramaConfig(c.Kafka)
	if err != nil {
		return nil, err
	}
	return mq.NewConsumer(c.Kafka.Brokers, c.Kafka.Consumer.GroupID, saramaCfg)
}
