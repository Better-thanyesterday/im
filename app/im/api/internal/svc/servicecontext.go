// Code scaffolded by goctl. Safe to edit.
// goctl 1.10.1

package svc

import (
	"github.com/zeromicro/go-zero/core/stores/redis"
	"im-platform/app/im/api/internal/config"

	"github.com/zeromicro/go-zero/zrpc"
	"im-platform/app/group/rpc/groupclient"
	"im-platform/app/message/rpc/messageclient"
	userclient "im-platform/app/user/rpc/userclient"
	"im-platform/common/utils"
	"time"
)

type ServiceContext struct {
	Config config.Config
	userclient.User
	groupclient.Group
	TokenManager *utils.TokenManager
	messageclient.Message
	Redis *redis.Redis
}

func NewServiceContext(c config.Config) *ServiceContext {
	rds := redis.MustNewRedis(c.Redis)
	// 在线表里写的地址必须能被其他机器的 push 服务直连:
	// 配置成回环/通配地址时替换为本机对外 IP,否则多机部署 push 直连不可达
	return &ServiceContext{
		Config:       c,
		User:         userclient.NewUser(zrpc.MustNewClient(c.UserRpc)),
		Message:      messageclient.NewMessage(zrpc.MustNewClient(c.MsgRpc)),
		TokenManager: utils.NewTokenManager(rds, 7*24*time.Hour),
		Redis:        rds,
		Group:        groupclient.NewGroup(zrpc.MustNewClient(c.GroupRpc)),
	}
}
