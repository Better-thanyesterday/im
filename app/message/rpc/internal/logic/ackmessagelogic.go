package logic

import (
	"context"
	"fmt"
	"im-platform/app/message/rpc/internal/svc"
	"im-platform/app/message/rpc/message"
	"im-platform/app/push/rpc/push"
	"im-platform/common/constants"
	"time"

	"github.com/zeromicro/go-zero/core/logx"
)

type AckMessageLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewAckMessageLogic(ctx context.Context, svcCtx *svc.ServiceContext) *AckMessageLogic {
	return &AckMessageLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

// AckMessage 统一 ACK 入口：已送达 / 已读
// 从 context metadata 获取阅读者 user_id（Gateway 注入）
func (l *AckMessageLogic) AckMessage(in *message.AckMessageReq) (*message.AckMessageResp, error) {
	// todo: add your logic here and delete this line
	// 安全获取阅读者身份（Gateway 通过 metadata 透传，客户端不可伪造）
	readerId := l.getUserIdFromCtx()
	if readerId <= 0 {
		return nil, constants.NewMsgError(constants.ErrCodeMsgInValidParam)
	}
	switch in.AckType {
	case message.AckType(constants.AckTypeDelivered):
		return l.handleDelivered(in, readerId)
	case message.AckType(constants.AckTypeRead):
		return l.handleRead(in, readerId)
	default:
		return nil, constants.NewMsgError(constants.ErrCodeMsgInValidParam)
	}
}

func (l *AckMessageLogic) handleDelivered(in *message.AckMessageReq, readerId int64) (*message.AckMessageResp, error) {
	// 已送达是轻量操作：通知发送方即可，可选写 Redis 状态
	// MVP 阶段不持久化"已送达"到 PG（避免写压力），只推送给发送方
	// 查消息获取发送者（需要 conv_id 路由到正确分片）
	msg, err := l.svcCtx.MessagesModel.FindOne(l.ctx, in.MsgId)
	if err != nil {
		logx.Errorf("find msg failed, conv=%s msg=%d: %v", in.ConvId, in.MsgId, err)
		// 消息不存在也返回成功，避免客户端死循环重试
		return &message.AckMessageResp{}, nil
	}
	// 防伪造：阅读者必须是接收方（不能自己给自己发已送达）
	if msg.Senderid == readerId {
		return &message.AckMessageResp{}, nil
	}

	// 推送给发送方：你的消息对方已收到
	// 这里复用 Push.Deliver，payload 带状态标识
	_ = l.notifySender(msg.Senderid, msg.Convid, in.MsgId)

	return &message.AckMessageResp{}, nil
}

func (l *AckMessageLogic) handleRead(in *message.AckMessageReq, readerId int64) (*message.AckMessageResp, error) {
	msg, err := l.svcCtx.MessagesModel.FindOne(l.ctx, in.MsgId)
	if err != nil {
		logx.Errorf("find msg failed: %v", err)
		return &message.AckMessageResp{}, nil
	}
	// 自己读自己的消息？忽略
	if msg.Senderid == readerId {
		return &message.AckMessageResp{}, nil
	}
	// 单聊：更新 inbox 已读状态（如果单聊走了 inbox）
	// 如果单聊没写 inbox，用 Redis 做轻量标记
	_ = l.markSingleRead(in.ConvId, readerId, in.MsgId)
	_ = l.notifySender(msg.Senderid, msg.Convid, in.MsgId)
	return &message.AckMessageResp{}, nil
}

func (l *AckMessageLogic) markSingleRead(convId string, readerId, msgId int64) error {
	// 方案 A：如果单聊也写了 inbox（写扩散模式）
	// return l.svcCtx.InboxModel.MarkRead(l.ctx, readerId, convId, msgId)

	// 方案 B：单聊未写 inbox，用 Redis 做已读标记（30 天过期）
	key := fmt.Sprintf("im:read:%s:%d", convId, msgId)
	err := l.svcCtx.Redis.SetexCtx(l.ctx, key, fmt.Sprintf("%d", readerId), 86400*30)
	return err
}
func (l *AckMessageLogic) notifySender(senderId int64, convId string, msgId int64) error {
	msg := push.PushMessage{
		SenderId: senderId,
		ConvId: convId,
		MsgId: msgId,
		SendTime: time.Now().UnixMilli(),
	}
	_, err := l.svcCtx.Push.Deliver(l.ctx, &push.DeliverReq{
		UserId:   senderId,
		PushType: push.PushType_PushTypeNotify,
		Message:  &msg,
	})
	return err
}

func (l *AckMessageLogic) getUserIdFromCtx() int64 {
	// go-zero 从 gRPC metadata 取 user_id（Gateway 注入）
	// 实际项目中根据你的 metadata key 调整
	return l.ctx.Value("x-user-id").(int64)
}
