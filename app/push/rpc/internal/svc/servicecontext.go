package svc

import (
	gatewayclient "im-platform/app/gateway/rpc/gatewayclient"
	"im-platform/app/push/rpc/internal/config"

	"github.com/zeromicro/go-zero/core/stores/redis"
	"github.com/zeromicro/go-zero/zrpc"
)

type ServiceContext struct {
	Config config.Config
	Redis  *redis.Redis
	gatewayclient.Gateway
	GatewayPool *GatewayPool
}

func NewServiceContext(c config.Config) *ServiceContext {
	rds := redis.MustNewRedis(c.RedisCache)
	return &ServiceContext{
		Config:      c,
		Redis:       rds,
		Gateway:     gatewayclient.NewGateway(zrpc.MustNewClient(c.GatewayRpc)),
		GatewayPool: NewGatewayPool(),
	}
}
