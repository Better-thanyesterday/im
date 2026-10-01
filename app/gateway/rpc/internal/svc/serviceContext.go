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
	"github.com/zeromicro/go-zero/zrpc"
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
		// 嵌入接口必须显式初始化,否则 nil 接口编译期无提示、第一帧上行就 panic
		Message: messageclient.NewMessage(zrpc.MustNewClient(c.MsgRpc)),
	}
}

// normalizeAdvertiseAddr 回环/通配地址替换为本机非回环 IP
func normalizeAdvertiseAddr(addr string) string {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		logx.Errorf("gateway gRPC 地址非法 | addr=%q err=%v", addr, err)
		return addr
	}
	switch host {
	case "", "0.0.0.0", "127.0.0.1", "::1", "localhost":
		ip := utils.AdvertiseHost()
		if ip == "" {
			logx.Errorf("gRPC 地址为回环且未探测到对外 IP,push 直连将不可达 | addr=%q", addr)
			return addr
		}
		normalized := net.JoinHostPort(ip, port)
		logx.Infof("对外宣告 gRPC 地址 | from=%s to=%s", addr, normalized)
		return normalized
	}
	return addr
}
