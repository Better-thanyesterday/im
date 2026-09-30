package config

import (
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
	Minio MinioConf
}

// MinioConf MinIO 对象存储配置
type MinioConf struct {
	Endpoint  string // 如 127.0.0.1:9000
	AccessKey string
	SecretKey string
	Bucket    string // 默认存储桶，如 im-files
	UseSSL    bool   `json:",default=false"`
}
