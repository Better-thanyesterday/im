package logic

import (
	"context"
	"encoding/json"
	"fmt"
	"im-platform/app/gateway/rpc/internal/conn"
	"im-platform/app/gateway/rpc/internal/protocol"
	"im-platform/app/gateway/rpc/internal/svc"
	"time"

	"github.com/zeromicro/go-zero/core/logx"
)

type WsConnectLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewWsConnectLogic(ctx context.Context, svcCtx *svc.ServiceContext) *WsConnectLogic {
	return &WsConnectLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *WsConnectLogic) Register(ctx context.Context, c *conn.Conn) error {
	userID := c.UserId()
	deviceType := c.DeviceType()
	// 注册 + 踢旧收敛到 manager.Add 一处,避免两套桶操作漂移;
	// 旧连接由这里在锁外"先通知(FrameKick)后 Close"
	oldConn := l.svcCtx.ConnManager.Add(c)
	if oldConn != nil {
		logx.Infof("检测到重连,踢出旧连接 | user=%d device=%d", userID, deviceType)
		// 踢之前先推 FrameKick(0x11):旧设备据此区分"被顶号"和"网络断",
		// 否则客户端看到的只是 1006 异常断开
		kickPayload, _ := json.Marshal(map[string]any{
			"event":     "kicked",
			"reason":    "relogin", // 同设备重新登录顶号
			"device":    deviceType,
			"timestamp": time.Now().UnixMilli(),
		})
		oldConn.Send(protocol.EncodeFrame(protocol.FrameKick, kickPayload))
		// 给旧连接的 WritePump 一个短暂缓冲把踢下线帧刷出去再断开;
		// AfterFunc 不阻塞新连接的握手
		time.AfterFunc(200*time.Millisecond, oldConn.Close)
	}
	// 4. 写 Redis 在线状态:
	onlineKey := fmt.Sprintf("im:online:%d:%d", userID, deviceType)
	gwKey := fmt.Sprintf("im:online:gw:%d", userID)
	gwAddr := l.svcCtx.Config.Gateway.GrpcAddr
	expireAt := 90
	script := `
    redis.call('SET', KEYS[1], "1", "EX", ARGV[2])
	redis.call('HSET', KEYS[2], ARGV[1], ARGV[3])
	return 1
	`
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	keys := []string{onlineKey, gwKey}
	argv := []any{deviceType, expireAt, gwAddr} // 调整顺序！删掉多余的1

	if _, err := l.svcCtx.Redis.EvalCtx(ctx, script, keys, argv...); err != nil {
		logx.Errorf("在线态写入 Redis 失败 | user=%d device=%d err=%v", userID, deviceType, err)
		return err
	}
	// 5. 启动读写协程
	go c.WritePump()
	go c.OnMessage(func(data []byte) {
		// TODO: 上行消息分发（聊天消息、心跳上报等），先记录日志
		logx.Infof("收到上行帧 | user=%d device=%d len=%d", userID, deviceType, len(data))
		HandleFrame(l.svcCtx, c, data)
	})
	// 离线补拉一律由客户端驱动:建连(首连/重连/顶号)成功后客户端主动发
	// FrameSyncRequest(带各会话本地 last_seq),由 forward.go 承接转发 message rpc。
	// 服务端不做建连自动补拉,原因:
	// 1) inbox 投递水位是用户级,多设备下服务端猜不准客户端本地进度,补拉范围必然失真;
	// 2) 重连落在另一台 gateway 实例时旧连接不在本进程,oldConn==nil,自动补拉会静默漏掉;
	// 3) 弱网重连瞬间打大同步请求易再次超时,退避时机应由客户端掌握
	c.ReadPump(nil)
	return nil
}
