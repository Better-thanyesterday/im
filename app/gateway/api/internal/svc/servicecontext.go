// Code scaffolded by goctl. Safe to edit.
// goctl 1.10.1

package svc

import (
	"im-platform/app/gateway/api/conn"
	"im-platform/app/gateway/api/internal/config"
	"im-platform/app/group/rpc/groupclient"
	"im-platform/app/message/rpc/messageclient"
	userclient "im-platform/app/user/rpc/userclient"
	"im-platform/common/utils"
	"net"
	"time"

	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/core/stores/redis"
	"github.com/zeromicro/go-zero/zrpc"
)

type ServiceContext struct {
	Config config.Config
	userclient.User
	groupclient.Group
	TokenManager *utils.TokenManager
	ConnManager  *conn.ConnManager
	messageclient.Message
	Redis *redis.Redis
}

func NewServiceContext(c config.Config) *ServiceContext {
	rds := redis.MustNewRedis(c.Redis)
	// 在线表里写的地址必须能被其他机器的 push 服务直连:
	// 配置成回环/通配地址时替换为本机对外 IP,否则多机部署 push 直连不可达
	c.Gateway.GrpcAddr = normalizeAdvertiseAddr(c.Gateway.GrpcAddr)
	return &ServiceContext{
		Config:       c,
		User:         userclient.NewUser(zrpc.MustNewClient(c.UserRpc)),
		Message:      messageclient.NewMessage(zrpc.MustNewClient(c.MsgRpc)),
		TokenManager: utils.NewTokenManager(rds, 7*24*time.Hour),
		ConnManager:  conn.NewManager(c.Gateway.BucketNum),
		Redis:        rds,
		Group:        groupclient.NewGroup(zrpc.MustNewClient(c.GroupRpc)),
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
