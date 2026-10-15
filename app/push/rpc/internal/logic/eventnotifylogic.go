package logic

import (
	"context"
	"strconv"
	"strings"
	"sync"
	"time"

	"im-platform/app/gateway/rpc/gateway"
	"im-platform/app/group/rpc/groupclient"
	userclient "im-platform/app/user/rpc/userclient"
	"im-platform/app/push/rpc/internal/svc"
	"im-platform/common/mq"

	"github.com/IBM/sarama"
	"github.com/zeromicro/go-zero/core/logx"
)

// fanoutWorkers 事件扇出的并发上限
const fanoutWorkers = 8

// EventNotifyLogic im.event.notify 消费者:
// 统一负责"查目标用户 → 直连各 gateway 实例批量下发"。
// 事件是实时性信号,不转离线信箱(撤回/群变更离线端由 Sync 兜底,轻量通知靠 Seq 补拉)
type EventNotifyLogic struct {
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewEventNotifyLogic(svcCtx *svc.ServiceContext) *EventNotifyLogic {
	return &EventNotifyLogic{
		svcCtx: svcCtx,
		Logger: logx.WithContext(context.Background()),
	}
}

// Handle Kafka 消费入口
func (l *EventNotifyLogic) Handle(ctx context.Context, msg *sarama.ConsumerMessage) error {
	ev, err := mq.UnmarshalEvent(msg.Value)
	if err != nil {
		logx.Errorf("事件信封解析失败,跳过 | topic=%s offset=%d err=%v", msg.Topic, msg.Offset, err)
		return nil // 坏载荷不重试,重试也没用
	}
	if ev.Event == "" {
		return nil
	}
	switch ev.Event {
	case mq.EventGroupNewMsgLite:
		l.dispatchGroupEvent(ev, true) // exclude sender
	case mq.EventGroupChanged:
		l.dispatchGroupEvent(ev, false)
	case mq.EventMsgRecall:
		l.dispatchRecall(ev)
	case mq.EventUserOnline, mq.EventUserOffline:
		l.dispatchPresence(ev)
	default:
		logx.Infof("未知事件类型,跳过 | event=%s", ev.Event)
	}
	return nil
}

// dispatchGroupEvent 群事件扇出:分页拉成员,除发送方(轻量通知)外全量下发
func (l *EventNotifyLogic) dispatchGroupEvent(ev *mq.Event, excludeSender bool) {
	memberIds := l.groupMemberIds(ev.GroupID)
	targets := memberIds[:0:0]
	for _, uid := range memberIds {
		if excludeSender && uid == ev.SenderID {
			continue
		}
		targets = append(targets, uid)
	}
	l.fanout(targets, ev)
}

// dispatchRecall 撤回事件:单聊除操作者外发给对方;群聊发给除操作者外的成员。
// 离线端不上行此事件——撤回已落库,客户端上线 Sync 时按 status=2 兜底感知
func (l *EventNotifyLogic) dispatchRecall(ev *mq.Event) {
	var targets []int64
	if gid, ok := parseGroupConv(ev.ConvID); ok {
		for _, uid := range l.groupMemberIds(gid) {
			if uid != ev.OperatorID {
				targets = append(targets, uid)
			}
		}
	} else if a, b, ok := parseSingleConv(ev.ConvID); ok {
		for _, uid := range []int64{a, b} {
			if uid != ev.OperatorID {
				targets = append(targets, uid)
			}
		}
	}
	l.fanout(targets, ev)
}

// dispatchPresence 上下线事件:扇出给该用户的好友(MVP 不做订阅关系,好友即订阅)
func (l *EventNotifyLogic) dispatchPresence(ev *mq.Event) {
	resp, err := l.svcCtx.User.GetFriends(context.Background(), &userclient.GetFriendsReq{
		UserId:   ev.UserID,
		Page:     1,
		PageSize: 500,
	})
	if err != nil {
		logx.Errorf("上下线事件查好友失败 | user=%d err=%v", ev.UserID, err)
		return
	}
	targets := make([]int64, 0, len(resp.Friends))
	for _, f := range resp.Friends {
		targets = append(targets, f.UserId)
	}
	l.fanout(targets, ev)
}

// fanout 批量下发事件帧到各目标用户的在线设备。
// 走 gateway BatchPushToConn(事件 payload 原样字节,由 gateway 封 FramePush);
// 无在线设备的用户直接忽略——事件不转离线箱
func (l *EventNotifyLogic) fanout(userIds []int64, ev *mq.Event) {
	if len(userIds) == 0 {
		return
	}
	payload, err := mq.MarshalEvent(ev)
	if err != nil {
		logx.Errorf("事件序列化失败 | event=%s err=%v", ev.Event, err)
		return
	}
	// 分桶并发:每批 200 人,上限 fanoutWorkers
	const batchSize = 200
	sem := make(chan struct{}, fanoutWorkers)
	var wg sync.WaitGroup
	for start := 0; start < len(userIds); start += batchSize {
		end := start + batchSize
		if end > len(userIds) {
			end = len(userIds)
		}
		batch := userIds[start:end]
		wg.Add(1)
		sem <- struct{}{}
		go func(batch []int64) {
			defer wg.Done()
			defer func() { <-sem }()
			l.pushBatch(batch, payload, ev.Event)
		}(batch)
	}
	wg.Wait()
}

// pushBatch 推一批用户:地址来自事件消费场景下的任意可用 gateway。
// 在线表地址按用户分布,这里没有逐用户路由——直接遍历已知 gateway 地址集合,
// 由各 gateway 自己判断哪些用户连在本机(BatchPushToConn 返回 failed 即不在线本机)
func (l *EventNotifyLogic) pushBatch(userIds []int64, payload []byte, event string) {
	for _, addr := range l.svcCtx.KnownGatewayAddrs() {
		conn, err := l.svcCtx.GatewayPool.Get(addr)
		if err != nil {
			logx.Errorf("事件扇出建连失败 | addr=%s event=%s err=%v", addr, event, err)
			continue
		}
		client := gateway.NewGatewayClient(conn)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		resp, err := client.BatchPushToConn(ctx, &gateway.BatchPushToConnReq{
			UserIds: userIds,
			Payload: payload,
		})
		cancel()
		if err != nil {
			l.svcCtx.GatewayPool.ReportFail(addr)
			logx.Errorf("事件扇出推送失败 | addr=%s event=%s err=%v", addr, event, err)
			continue
		}
		if len(resp.FailedUserIds) > 0 {
			logx.Infof("事件扇出部分用户不在线本机 | addr=%s event=%s failed=%d",
				addr, event, len(resp.FailedUserIds))
		}
	}
}

// groupMemberIds 分页拉全量群成员(带 5s 调用超时)
func (l *EventNotifyLogic) groupMemberIds(groupId int64) []int64 {
	var ids []int64
	lastId := int64(1 << 62)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		resp, err := l.svcCtx.Group.GetMembers(ctx, &groupclient.GetMembersReq{
			GroupId:  groupId,
			PageSize: 500,
			LastId:   lastId,
		})
		cancel()
		if err != nil {
			logx.Errorf("群事件拉成员失败 | group=%d err=%v", groupId, err)
			return ids
		}
		for _, m := range resp.Members {
			ids = append(ids, m.UserId)
		}
		lastId = resp.LastId
		if !resp.HasMore {
			break
		}
	}
	return ids
}

// parseGroupConv "group_123" → (123, true)
func parseGroupConv(convId string) (int64, bool) {
	if !strings.HasPrefix(convId, "group_") {
		return 0, false
	}
	gid, err := strconv.ParseInt(strings.TrimPrefix(convId, "group_"), 10, 64)
	if err != nil || gid <= 0 {
		return 0, false
	}
	return gid, true
}

// parseSingleConv "min:max" → (min, max, true)
func parseSingleConv(convId string) (int64, int64, bool) {
	parts := strings.Split(convId, ":")
	if len(parts) != 2 {
		return 0, 0, false
	}
	a, err1 := strconv.ParseInt(parts[0], 10, 64)
	b, err2 := strconv.ParseInt(parts[1], 10, 64)
	if err1 != nil || err2 != nil || a <= 0 || b <= 0 {
		return 0, 0, false
	}
	return a, b, true
}
