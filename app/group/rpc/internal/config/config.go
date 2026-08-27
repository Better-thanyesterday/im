package config

import "github.com/zeromicro/go-zero/zrpc"

type Config struct {
	zrpc.RpcServerConf
	Postgres struct {
		DataSource string
	}
	SnokFlake struct {
		WorkNode int64
	}
}
