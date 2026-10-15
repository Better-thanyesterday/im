package config

import (
	"im-platform/common/mq"

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
	Kafka mq.Config // 仅生产:im.event.notify 群变更事件
}
