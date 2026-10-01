package config

import (
	"github.com/zeromicro/go-zero/core/stores/redis"
	"github.com/zeromicro/go-zero/zrpc"
)

type Config struct {
	GatewayRpc zrpc.RpcServerConf
	Redis      redis.RedisConf
	Gateway    struct {
		GrpcAddr  string
		BucketNum int // 连接分桶数,0=默认 16
	}
}
