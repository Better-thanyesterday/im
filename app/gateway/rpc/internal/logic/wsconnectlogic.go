package logic

import (
	"context"
	"encoding/json"
	"fmt"
	"im-platform/app/gateway/rpc/internal/conn"
	"im-platform/app/gateway/rpc/internal/protocol"
	"im-platform/app/gateway/rpc/internal/svc"
	"im-platform/app/message/rpc/messageclient"
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
	bucket := l.svcCtx.ConnManager.BucketOf(userID)
	bucket.Mu.Lock()
	oldConn := bucket.Conns[c.Key()]
	// 3. 注册新连接
	bucket.Conns[c.Key()] = c
	bucket.Mu.Unlock() // 先解锁，Close 里的回调要重新拿锁
	if oldConn != nil && oldConn != c {
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
	// 仅顶号/重连(同 key 旧连接被顶替)才做离线补拉;
	// 首次连接由客户端主动发 FrameSyncRequest 拉取
	var onReconnect func(*conn.Conn)
	if oldConn != nil {
		onReconnect = func(c *conn.Conn) { l.OnReconnect(c) }
	}
	c.ReadPump(onReconnect)
	return nil
}

func (l *WsConnectLogic) OnReconnect(c *conn.Conn) {
	// 断线重连补拉:ConvList 传空,由 message 服务组装该用户的全部会话
	// (单聊=好友列表,群聊=所在群)并按 inbox 投递水位逐个补
	userID := c.UserId()
	in := &messageclient.SyncMessageReq{UserId: userID}
	// parent 是 Background,本就不受连接生命周期影响;
	// 不能再 WithoutCancel——它会把 WithTimeout 的 deadline 一并丢掉,变成无超时调用
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	resp, err := l.svcCtx.SyncMessage(ctx, in)
	if err != nil {
		logx.Errorf("离线同步失败 | user=%d err=%v", userID, err)
		return
	}
	// 把离线消息推给客户端(FrameSyncRequest 帧型下行,与上行同步请求对称)
	for _, msg := range resp.ConvSyncs {
		payload, _ := json.Marshal(msg)
		c.Send(protocol.EncodeFrame(protocol.FrameSyncRequest, payload))
	}
}
