package logic

import (
	"context"
	"encoding/json"
	"fmt"
	"im-platform/app/gateway/api/conn"
	"im-platform/app/gateway/api/internal/svc"
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

func (l *WsConnectLogic) Register(c *conn.Conn) {
	bucket := l.svcCtx.ConnManager.BucketOf(c.UserId())
	bucket.Mu.Lock()
	oldConn := bucket.Conns[c.Key()]
	// 3. 注册新连接
	bucket.Conns[c.Key()] = c
	bucket.Mu.Unlock() // 先解锁，Close 里的回调要重新拿锁
	if oldConn != nil && oldConn != c {
		logx.Infof("reconnect detected | user=%d device=%d, kick old conn", c.UserId(), c.DeviceType())
		oldConn.Close() // 锁外踢，onClose → onConnClosed 正常加锁
	}

	// 4. 写 Redis 在线状态:
	//    Hash 是设备注册表(field=设备类型, value=gateway 地址,无 key 级 TTL);
	//    每设备独立活性 key 带 90s TTL,由心跳续期,崩溃后 90s 自动失效
	l.svcCtx.Redis.Hset(fmt.Sprintf("im:online:%d", c.UserId()),
		fmt.Sprintf("%d", c.DeviceType()), l.svcCtx.Config.Gateway.GrpcAddr)
	liveKey := fmt.Sprintf("im:online:%d:%d", c.UserId(), c.DeviceType())
	_ = l.svcCtx.Redis.SetexCtx(context.Background(), liveKey, l.svcCtx.Config.Gateway.GrpcAddr, 90)
	if _, err := l.svcCtx.Redis.Hdel(fmt.Sprintf("im:offline:%d", c.UserId()), fmt.Sprintf("%d", c.DeviceType())); err != nil {
		logx.Errorf("redis hdel failed | user=%d device=%d err=%v", c.UserId(), c.DeviceType(), err)
	}
	// 5. 启动读写协程
	go c.WritePump()
	c.ReadPump(func(data []byte) {
		// TODO: 上行消息分发（聊天消息、心跳上报等），先记录日志
		logx.Infof("ws message | user=%d device=%d len=%d", c.UserId(), c.DeviceType(), len(data))
		HandleFrame(l.svcCtx, c, data)
	}, func(c *conn.Conn) { l.OnReconnect(c) })

}

func (l *WsConnectLogic) OnReconnect(c *conn.Conn) {
	// 调用 Message RPC 拉取离线消息
	// 这里走 gRPC 调用 message.SyncMessages

	conseq := &messageclient.SyncMessageReq_ConSeq{
		ConvId:  "746262395202048000:746263247023247360",
		LastSeq: 200,
	}
	convlist := []*messageclient.SyncMessageReq_ConSeq{conseq}
	in := &messageclient.SyncMessageReq{
		UserId:   c.UserId(),
		ConvList: convlist,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	safeCtx := context.WithoutCancel(ctx)
	defer cancel()
	resp, err := l.svcCtx.SyncMessage(safeCtx, in)
	if err != nil {
		logx.Errorf("sync messages failed | user=%d err=%v", c.UserId(), err)
		return
	}
	// 把离线消息推给客户端
	for _, msg := range resp.ConvSyncs {
		payload, _ := json.Marshal(msg)
		c.Send(payload)
	}
}
