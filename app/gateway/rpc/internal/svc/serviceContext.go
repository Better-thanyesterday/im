package svc

import (
	"im-platform/app/gateway/rpc/internal/config"
	"im-platform/app/gateway/rpc/internal/conn"
	"im-platform/app/message/rpc/messageclient"
	"im-platform/common/mq"
	"im-platform/common/utils"
	"net"
	"time"

	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/core/stores/redis"
	"github.com/zeromicro/go-zero/zrpc"
)

type ServiceContext struct {
	Config        config.Config
	TokenManager  *utils.TokenManager
	ConnManager   *conn.ConnManager
	Redis         *redis.Redis
	KafkaProducer *mq.Producer // 上下线事件广播(初始化失败只降级,不影响连接生命周期)
	messageclient.Message
}

func NewServiceContext(c config.Config) *ServiceContext {
	rds := redis.MustNewRedis(c.Redis)
	c.Gateway.GrpcAddr = normalizeAdvertiseAddr(c.Gateway.GrpcAddr)
	// 自注册到存活 gateway 地址集合,push 事件扇出按此集合遍历实例
	if _, err := rds.Sadd("im:gateways", c.Gateway.GrpcAddr); err != nil {
		logx.Errorf("gateway 自注册失败,事件扇出可能遗漏本实例 | err=%v", err)
	}
	var producer *mq.Producer
	if saramaCfg, err := mq.BuildSaramaConfig(c.Kafka); err == nil {
		if p, perr := mq.NewProducer(c.Kafka.Brokers, saramaCfg); perr == nil {
			producer = p
		} else {
			logx.Errorf("kafka producer 初始化失败,上下线事件不可用 | err=%v", perr)
		}
	} else {
		logx.Errorf("kafka 配置构建失败,上下线事件不可用 | err=%v", err)
	}
	return &ServiceContext{
		Config:        c,
		TokenManager:  utils.NewTokenManager(rds, 7*24*time.Hour),
		ConnManager:   conn.NewManager(c.Gateway.BucketNum),
		Redis:         rds,
		KafkaProducer: producer,
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
