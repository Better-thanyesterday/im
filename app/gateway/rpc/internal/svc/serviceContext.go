package svc

import (
	"im-platform/app/gateway/rpc/internal/config"
	"im-platform/app/gateway/rpc/internal/conn"
	"im-platform/app/message/rpc/messageclient"
	"im-platform/common/utils"
	"net"
	"time"

	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/core/stores/redis"
)

type ServiceContext struct {
	Config       config.Config
	TokenManager *utils.TokenManager
	ConnManager  *conn.ConnManager
	Redis        *redis.Redis
	messageclient.Message
}

func NewServiceContext(c config.Config) *ServiceContext {
	rds := redis.MustNewRedis(c.Redis)
	c.Gateway.GrpcAddr = normalizeAdvertiseAddr(c.Gateway.GrpcAddr)
	return &ServiceContext{
		Config:       c,
		TokenManager: utils.NewTokenManager(rds, 7*24*time.Hour),
		ConnManager:  conn.NewManager(c.Gateway.BucketNum),
		Redis:        rds,
	}
}

// normalizeAdvertiseAddr 回环/通配地址替换为本机非回环 IP
func normalizeAdvertiseAddr(addr string) string {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		logx.Errorf("invalid gateway grpc addr %q: %v", addr, err)
		return addr
	}
	switch host {
	case "", "0.0.0.0", "127.0.0.1", "::1", "localhost":
		ip := utils.AdvertiseHost()
		if ip == "" {
			logx.Errorf("grpc addr %q is loopback and no external ip detected, push 直连将不可达", addr)
			return addr
		}
		normalized := net.JoinHostPort(ip, port)
		logx.Infof("advertise gateway grpc addr: %s -> %s", addr, normalized)
		return normalized
	}
	return addr
}
