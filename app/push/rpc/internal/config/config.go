package config

import (
	"im-platform/common/mq"

	"github.com/zeromicro/go-zero/core/stores/redis"
	"github.com/zeromicro/go-zero/zrpc"
)

type Config struct {
	zrpc.RpcServerConf
	RedisCache redis.RedisConf
	GatewayRpc zrpc.RpcClientConf
	UserRpc    zrpc.RpcClientConf // 上下线事件扇出:查好友
	GroupRpc   zrpc.RpcClientConf // 群/撤回事件扇出:查成员
	Kafka      mq.Config          // 仅消费:im.event.notify 事件广播
}
