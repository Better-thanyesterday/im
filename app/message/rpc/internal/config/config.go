package config

import (
	"im-platform/common/mq"

	"github.com/zeromicro/go-zero/core/stores/redis"
	"github.com/zeromicro/go-zero/zrpc"
)

type Config struct {
	zrpc.RpcServerConf
	Postgres struct {
		DataSource string
	}
	Snowflake struct {
		WorkNode int64
	}
	Kafka      mq.Config
	PushRpc    zrpc.RpcClientConf
	GroupRpc   zrpc.RpcClientConf
	UserRpc    zrpc.RpcClientConf
	FileRpc    zrpc.RpcClientConf // 媒体消息发送时校验引用的文件
	RedisCache redis.RedisConf
}
