// Code scaffolded by goctl. Safe to edit.
// goctl 1.10.1

package config

import (
	"github.com/zeromicro/go-zero/core/stores/redis"
	"github.com/zeromicro/go-zero/rest"
	"github.com/zeromicro/go-zero/zrpc"
)

type Config struct {
	rest.RestConf
	Redis    redis.RedisConf
	UserRpc  zrpc.RpcClientConf
	GroupRpc zrpc.RpcClientConf
	FileRpc  zrpc.RpcClientConf
	MsgRpc   zrpc.RpcClientConf // 消息撤回等需要触达 message 服务的能力

	Token struct {
		// 用小时整数而非 time.Duration:go-zero conf 对 duration 字符串解析不稳定。
		// 登录签发 token 与 TokenManager 默认 TTL 都从这里取,与 user 服务共享 Redis token 表
		DefaultTTLHours int64 `json:",default=24"`
	}
}
