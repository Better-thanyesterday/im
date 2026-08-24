// Code scaffolded by goctl. Safe to edit.
// goctl 1.10.1

package svc

import (
	"im-platform/app/gateway/api/conn"
	"im-platform/app/gateway/api/internal/config"
	"im-platform/app/message/rpc/messageclient"
	"im-platform/app/user/rpc/userclient"
	"im-platform/common/utils"
	"time"

	"github.com/zeromicro/go-zero/core/stores/redis"
	"github.com/zeromicro/go-zero/zrpc"
)

type ServiceContext struct {
	Config config.Config
	userclient.User
	TokenManager *utils.TokenManager
	ConnManager *conn.ConnManager
	messageclient.Message
	Redis redis.Redis
}

func NewServiceContext(c config.Config) *ServiceContext {
	rds:=redis.MustNewRedis(c.Redis)
	return &ServiceContext{
		Config: c,
		User: userclient.NewUser(zrpc.MustNewClient(c.UserRpc)),
		Message: messageclient.NewMessage(zrpc.MustNewClient(c.MsgRpc)),
		TokenManager: utils.NewTokenManager(rds,7*24*time.Hour),
		ConnManager: conn.NewManager(16),
		Redis:*rds,
	}
}
