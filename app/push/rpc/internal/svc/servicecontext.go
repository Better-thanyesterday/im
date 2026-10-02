package svc

import (
	"im-platform/app/push/rpc/internal/config"

	"github.com/zeromicro/go-zero/core/stores/redis"
)

type ServiceContext struct {
	Config config.Config
	Redis  *redis.Redis
	// GatewayPool 直连 gateway:投递目标来自 Redis 在线表里的 gateway 地址,
	// 不走 etcd 服务发现(嵌入式 zrpc gateway client 是死代码,已删)
	GatewayPool *GatewayPool
}

func NewServiceContext(c config.Config) *ServiceContext {
	rds := redis.MustNewRedis(c.RedisCache)
	return &ServiceContext{
		Config:      c,
		Redis:       rds,
		GatewayPool: NewGatewayPool(),
	}
}
