package logic

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"

	"im-platform/app/gateway/rpc/gateway"
	"im-platform/app/push/rpc/internal/svc"
	"im-platform/app/push/rpc/push"

	"github.com/zeromicro/go-zero/core/logx"
)

type DeliverLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewDeliverLogic(ctx context.Context, svcCtx *svc.ServiceContext) *DeliverLogic {
	return &DeliverLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

// 在线实时推送 + 离线存储 + 未读计数：按 Redis 在线状态决定走 Gateway.PushToConn 还是写 im:offlineinbox
func (l *DeliverLogic) Deliver(ctx context.Context, in *push.DeliverReq) (*push.DeliverResp, error) {
	return l.deliver(ctx, in, nil)
}

// deliver 投递主体。body 是预序列化的下行 payload(Full=完整 PushMessage,Notify=轻量帧):
// BatchDeliver 对同一 Message 推 N 个用户时由调用方序列化一次复用,传 nil 则在此序列化。
// 下行 payload 不带 DeliverReq 包装层(UserId/PushType)——
// 那是 push 的内部 DTO,透传给客户端等于把内部字段重构变成客户端兼容性事故
func (l *DeliverLogic) deliver(ctx context.Context, in *push.DeliverReq, body []byte) (*push.DeliverResp, error) {
	if in.Message == nil {
		return nil, fmt.Errorf("deliver req has no message")
	}
	if body == nil {
		b, err := marshalPayload(in.PushType, in.Message)
		if err != nil {
			return nil, fmt.Errorf("marshal push payload failed: %w", err)
		}
		body = b
	}
	onlineKey := fmt.Sprintf("im:online:%d", in.UserId)
	// 1. 查询用户全端在线状态（Hash: field=设备类型, value=gateway地址）
	onlineDevices, err := l.svcCtx.Redis.HgetallCtx(l.ctx, onlineKey)
	if err != nil {
		logx.Errorf("redis hgetall failed, key=%s, err=%v", onlineKey, err)
		// Redis 故障，保守降级：全部按离线处理，保证消息不丢
	}
	// 2. 完全离线：无在线设备
	if len(onlineDevices) == 0 {
		// Notify(轻量通知/回执)是易逝的派生态,不进离线信箱也不计未读:
		// 客户端重新上线后按已读/水位 Sync 自然补齐,离线的通知只会留下垃圾信箱条目
		if in.PushType == push.PushType_PushTypeNotify {
			return &push.DeliverResp{Success: false}, nil
		}
		l.storeOffline(l.ctx, in)
		l.incrUnread(l.ctx, in)
		return &push.DeliverResp{Success: false}, nil
	}
	// 3. 有在线设备，逐个推送。
	//    Hash 只是设备注册表,可能残留崩溃设备的 field;
	//    每设备活性 key(im:online:{uid}:{device},心跳续期 90s TTL)才是存活依据
	anySuccess := false
	for deviceTypeStr, gatewayAddr := range onlineDevices {
		deviceType, _ := strconv.ParseInt(deviceTypeStr, 10, 32)
		live, err := l.svcCtx.Redis.ExistsCtx(l.ctx, fmt.Sprintf("im:online:%d:%d", in.UserId, deviceType))
		if err != nil {
			logx.Errorf("check live key failed, uid=%d device=%d err=%v", in.UserId, deviceType, err)
			continue // 活性未知,不向可疑地址投递
		}
		if !live {
			logx.Infof("device not live, skip | uid=%d device=%d", in.UserId, deviceType)
			continue
		}
		if err := l.pushToDevice(l.ctx, in, deviceType, gatewayAddr, body); err != nil {
			logx.Errorf("push to device failed, uid=%d, device=%d, err=%v", in.UserId, deviceType, err)
		} else {
			anySuccess = true
		}
	}
	// 4. 只要有任意设备推送成功，认为用户在线可达，不写离线
	if anySuccess {
		return &push.DeliverResp{Success: true}, nil
	}
	// 5. 所有在线设备都推送失败，降级为离线存储
	l.storeOffline(l.ctx, in)
	l.incrUnread(l.ctx, in)
	return &push.DeliverResp{}, nil
}

// marshalPayload 按 PushType 生成下行 payload(DeliverReq/BatchDeliverReq 通用):
// Full: 完整 PushMessage(pb 的 snake_case json tag);
// Notify: 仅 {conv_id, seq_id} 轻量帧,proto 语义是"客户端收到后主动 SyncMessages 拉取",
// 内部 DTO 的其余字段(SendTime/SenderId 等)一律不外泄
func marshalPayload(pushType push.PushType, msg *push.PushMessage) ([]byte, error) {
	if pushType == push.PushType_PushTypeNotify {
		return json.Marshal(struct {
			ConvId string `json:"conv_id"`
			SeqId  int64  `json:"seq_id"`
		}{ConvId: msg.ConvId, SeqId: msg.SeqId})
	}
	return json.Marshal(msg)
}

func (l *DeliverLogic) pushToDevice(ctx context.Context, in *push.DeliverReq, deviceType int64, gatewayAddr string, body []byte) error {
	// 在线表 Hash 的 value 本身就是权威 gateway 地址,直接用
	// 维护 grpc.ClientConn 连接池，避免频繁 Dial
	conn, err := l.svcCtx.GatewayPool.Get(gatewayAddr)
	if err != nil {
		return err
	}
	client := gateway.NewGatewayClient(conn)
	_, err = client.PushToConn(ctx, &gateway.PushToConnReq{
		UserId:     in.UserId,
		DeviceType: int32(deviceType),
		Payload:    body,
	})
	if err != nil {
		// 推送失败大概率是连接失效(gateway 重启/换实例),上报失败计数,连续超阈值才逐出重建
		l.svcCtx.GatewayPool.ReportFail(gatewayAddr)
	} else {
		l.svcCtx.GatewayPool.MarkOK(gatewayAddr)
	}
	return err
}

// storeOffline 写入 Redis 离线信箱（Sorted Set，每会话独立 key）
// key 约定与 message 侧 syncFromOffline 一致：im:offlineinbox:{uid}:{convId}
// 按会话拆 key 后，同步侧按 score 区间清理只影响本会话，不会误删其他会话条目
func (l *DeliverLogic) storeOffline(ctx context.Context, in *push.DeliverReq) {
	offlineKey := fmt.Sprintf("im:offlineinbox:%d:%s", in.UserId, in.Message.ConvId)
	score := in.Message.SeqId
	_, err := l.svcCtx.Redis.ZaddCtx(ctx, offlineKey, score, fmt.Sprintf("%d", in.Message.MsgId))
	if err != nil {
		logx.Errorf("zadd offline box failed, key=%s, err=%v", offlineKey, err)
		return
	}
	_ = l.svcCtx.Redis.ExpireCtx(ctx, offlineKey, 86400*7)
}

func (l *DeliverLogic) incrUnread(ctx context.Context, in *push.DeliverReq) {
	unreadKey := fmt.Sprintf("im:unread:%d", in.UserId)
	_, err := l.svcCtx.Redis.HincrbyCtx(ctx, unreadKey, in.Message.ConvId, 1)
	if err != nil {
		logx.Errorf("hincrby unread failed, key=%s, err=%v", unreadKey, err)
		return
	}
	_ = l.svcCtx.Redis.ExpireCtx(ctx, unreadKey, 86400*30)
}
