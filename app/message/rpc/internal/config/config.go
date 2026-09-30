package config

import (
	"im-platform/common/mq"

	"github.com/zeromicro/go-zero/core/stores/cache"
	"github.com/zeromicro/go-zero/core/stores/redis"
	"github.com/zeromicro/go-zero/zrpc"
)

type Config struct {
	zrpc.RpcServerConf
	Postgres struct {
		DataSource string
	}
	Cache     cache.CacheConf
	SnokFlake struct {
		WorkNode int64
	}
	Kafka mq.Config
	PushRpc zrpc.RpcClientConf
	GroupRpc zrpc.RpcClientConf
	UserRpc  zrpc.RpcClientConf
	RedisCache redis.RedisConf

}
