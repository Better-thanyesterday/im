package config

import (
	"github.com/zeromicro/go-zero/core/stores/redis"
	"github.com/zeromicro/go-zero/rest"
	"github.com/zeromicro/go-zero/zrpc"
)

type Config struct {
	rest.RestConf // WS/HTTP 入口(/ws 路由 + 鉴权中间件)
	GatewayRpc    zrpc.RpcServerConf
	MsgRpc        zrpc.RpcClientConf
	Redis         redis.RedisConf
	Gateway       struct {
		GrpcAddr  string // 写入在线表供 push 直连,回环地址启动时自动替换为对外 IP
		BucketNum int    // 连接分桶数,0=默认 16
	}
}
