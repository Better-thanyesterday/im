package config

import "github.com/zeromicro/go-zero/zrpc"

type Config struct {
	zrpc.RpcServerConf
	Postgres struct {
		DataSource string
	}
	Snowflake struct {
		WorkNode int64
	}
}
