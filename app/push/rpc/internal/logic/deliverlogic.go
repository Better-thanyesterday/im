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

// 在线实时推送 + 离线存储 + 未读计数：按 Redis 在线状态决定走 Gateway.PushToConn 还是写 im:offline
func (l *DeliverLogic) Deliver(in *push.DeliverReq) (*push.DeliverResp, error) {
	// todo: add your logic here and delete this line
	onlineKey := fmt.Sprintf("im:online:%d", in.UserId)
	// 1. 查询用户全端在线状态（Hash: field=设备类型, value=gateway地址）
	onlineDevices, err := l.svcCtx.Redis.HgetallCtx(l.ctx, onlineKey)
	if err != nil {
		logx.Errorf("redis hgetall failed, key=%s, err=%v", onlineKey, err)
		// Redis 故障，保守降级：全部按离线处理，保证消息不丢
	}
	// 2. 完全离线：无在线设备
	if len(onlineDevices) == 0 {
		l.storeOffline(l.ctx,in)
		l.incrUnread(l.ctx,in)
		return &push.DeliverResp{Success: false},nil
	}
	// 3. 有在线设备，逐个推送
	anySuccess := false
	for deviceType_str, gatewayAddr := range onlineDevices {
		deviceType,_:=strconv.ParseInt(deviceType_str,10,32)
		if err:=l.pushToDevice(l.ctx,in,deviceType,gatewayAddr);err != nil {
			logx.Errorf("push to device failed, uid=%d, device=%d, err=%v", in.UserId, deviceType, err)
		} else {
			anySuccess = true
		}
	}
	// 4. 只要有任意设备推送成功，认为用户在线可达，不写离线
	if anySuccess == true {
		return &push.DeliverResp{Success: true}, nil
	}
	// 5. 所有在线设备都推送失败，降级为离线存储
	l.storeOffline(l.ctx, in)
	l.incrUnread(l.ctx, in)
	return &push.DeliverResp{}, nil
}

func (l *DeliverLogic) pushToDevice(ctx context.Context, in *push.DeliverReq, deviceType int64, gatewayAddr string) error {
	// 优先查路由缓存 im:route:{uid}:{device_type}，60s TTL，避免频繁查 etcd
	routeKey := fmt.Sprintf("im:route:%d:%d", in.UserId, deviceType)
	routeCache, err := l.svcCtx.Redis.GetCtx(ctx, routeKey)
	if err != nil || routeCache == " " {
		// 缓存未命中，使用在线状态中的地址（假设可直接 dial）
		// 实际生产应从 etcd 解析 gatewayAddr 对应的 gRPC endpoint
		routeCache = gatewayAddr
		_ = l.svcCtx.Redis.SetexCtx(ctx, routeKey, routeCache, 60)
	}
	body, _ := json.Marshal(in)
	// 直连 Gateway 实例
	// 维护 grpc.ClientConn 连接池，避免频繁 Dial
	conn, err := l.svcCtx.GatewayPool.Get(gatewayAddr)
	if err!=nil {
		return err	
	}
	client := gateway.NewGatewayClient(conn)
	_, err = client.PushToConn(ctx, &gateway.PushToConnReq{
		UserId:     in.UserId,
		DeviceType: int32(deviceType),
		Payload:       body,
	})
	return err
}

// storeOffline 写入 Redis 离线信箱（Sorted Set）
func (l *DeliverLogic) storeOffline(ctx context.Context, in *push.DeliverReq) {
	offlineKey := fmt.Sprintf("im:offlineinbox:%d", in.UserId)
	score := in.Message.SeqId
	_, err := l.svcCtx.Redis.ZaddCtx(ctx, offlineKey, score, fmt.Sprintf("%d", in.Message.MsgId))
	if err != nil {
		logx.Errorf("zadd offline box failed, key=%s, err=%v", offlineKey, err)
		return
	}
	_ = l.svcCtx.Redis.ExpireCtx(ctx, offlineKey, 86400*7)
}

func (l *DeliverLogic) incrUnread(ctx context.Context, in *push.DeliverReq) {
	unreadKey:= fmt.Sprintf("im:unread:%d", in.UserId)
	_,err:=l.svcCtx.Redis.HincrbyCtx(ctx,unreadKey,in.Message.ConvId,1)
	if err!=nil {
		logx.Errorf("hincrby unread failed, key=%s, err=%v", unreadKey, err)
		return
	}
	_ = l.svcCtx.Redis.ExpireCtx(ctx, unreadKey, 86400*30)
}