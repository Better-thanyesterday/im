// Code scaffolded by goctl. Safe to edit.
// goctl 1.10.1

package svc

import (
	"time"

	"im-platform/app/file/rpc/fileclient"
	"im-platform/app/group/rpc/groupclient"
	"im-platform/app/im/api/internal/config"
	messageclient "im-platform/app/message/rpc/messageclient"
	userclient "im-platform/app/user/rpc/userclient"

	"im-platform/common/utils"

	"github.com/zeromicro/go-zero/core/stores/redis"
	"github.com/zeromicro/go-zero/zrpc"
)

type ServiceContext struct {
	Config config.Config
	Redis  *redis.Redis
	User   userclient.User
	Group  groupclient.Group
	File   fileclient.File
	// Message 仅用于消息撤回这类需要服务端仲裁的 REST 操作;实时收发走 WS
	Message messageclient.Message
	// TokenManager 与 user 服务共享 Redis token 表:签发/校验/吊销同源
	TokenManager *utils.TokenManager
}

func NewServiceContext(c config.Config) *ServiceContext {
	rds := redis.MustNewRedis(c.Redis)
	return &ServiceContext{
		Config:       c,
		Redis:        rds,
		User:         userclient.NewUser(zrpc.MustNewClient(c.UserRpc)),
		Group:        groupclient.NewGroup(zrpc.MustNewClient(c.GroupRpc)),
		File:         fileclient.NewFile(zrpc.MustNewClient(c.FileRpc)),
		Message:      messageclient.NewMessage(zrpc.MustNewClient(c.MsgRpc)),
		TokenManager: utils.NewTokenManager(rds, time.Duration(c.Token.DefaultTTLHours)*time.Hour),
	}
}
