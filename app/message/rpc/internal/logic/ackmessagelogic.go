package logic

import (
	"context"
	"fmt"
	"im-platform/app/message/rpc/internal/svc"
	"im-platform/app/message/rpc/message"
	"im-platform/app/push/rpc/push"
	"im-platform/common/constants"
	"strconv"

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
func (l *AckMessageLogic) AckMessage(in *message.AckMessageReq) (*message.AckMessageResp, error) {
	// 身份以 gateway 注入的 metadata 为准,请求字段仅作内部调用回退,防止伪造他人已读回执
	readerId := userIDFromCtx(l.ctx)
	if readerId <= 0 {
		readerId = in.UserId
	}
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
	// conv_id 由客户端携带,以消息实际归属会话为准,防止跨会话伪造
	if msg.Convid != in.ConvId {
		logx.Errorf("ack conv mismatch: msg=%d expect=%s got=%s reader=%d", in.MsgId, msg.Convid, in.ConvId, readerId)
		return &message.AckMessageResp{}, nil
	}

	// 推送给发送方：你的消息对方已收到(PushType_Notify 轻量帧,只带 conv_id + seq_id)
	_ = l.notifySender(msg.Senderid, msg.Convid, msg.Seqid)

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
	// conv_id 由客户端携带,以消息实际归属会话为准,防止跨会话伪造
	if msg.Convid != in.ConvId {
		logx.Errorf("read conv mismatch: msg=%d expect=%s got=%s reader=%d", in.MsgId, msg.Convid, in.ConvId, readerId)
		return &message.AckMessageResp{}, nil
	}
	// 已读水位:客户端上报的 read_seq 优先,缺省用本条消息的 seq
	readSeq := in.ReadSeq
	if readSeq <= 0 {
		readSeq = msg.Seqid
	}

	// 1. Redis 已读水位:每 (conv, reader) 一个 key 存已读最大 seq,
	//    替代原来每条消息一个 im:read:{conv}:{msg} key 的爆炸式设计
	if err := l.markReadWatermark(in.ConvId, readerId, readSeq); err != nil {
		logx.Errorf("mark read watermark failed: conv=%s reader=%d err=%v", in.ConvId, readerId, err)
	}
	// 2. PG 收件箱:该会话 <= 水位的行批量置已读(未读数持久层真源)
	if err := l.svcCtx.InboxesModel.MarkConvRead(l.ctx, readerId, in.ConvId, readSeq); err != nil {
		logx.Errorf("mark conv read failed: conv=%s reader=%d err=%v", in.ConvId, readerId, err)
	}
	// 3. 清 push 服务的 Redis 未读计数(闭环 ClearUnread 空实现)
	if _, err := l.svcCtx.Push.ClearUnread(l.ctx, &push.ClearUnreadReq{
		UserId: readerId,
		ConvId: in.ConvId,
		SeqId:  readSeq,
	}); err != nil {
		logx.Errorf("clear unread failed: conv=%s reader=%d err=%v", in.ConvId, readerId, err)
	}
	_ = l.notifySender(msg.Senderid, msg.Convid, readSeq)
	return &message.AckMessageResp{}, nil
}

// notifySender 已读/送达回执推送:PushType_Notify,下行只有 {conv_id, seq_id} 轻量帧,
// 客户端据此更新本会话的已读/送达水位,不携带任何消息内容
func (l *AckMessageLogic) notifySender(senderId int64, convId string, seqId int64) error {
	msg := push.PushMessage{
		ConvId: convId,
		SeqId:  seqId,
	}
	_, err := l.svcCtx.Push.Deliver(l.ctx, &push.DeliverReq{
		UserId:   senderId,
		PushType: push.PushType_PushTypeNotify,
		Message:  &msg,
	})
	return err
}

// markReadWatermark 已读水位只前进不回退,key 为 im:read:{conv}:{reader}(30 天 TTL)
func (l *AckMessageLogic) markReadWatermark(convId string, readerId, seq int64) error {
	key := fmt.Sprintf("im:read:%s:%d", convId, readerId)
	lua := `
		local cur = tonumber(redis.call('GET', KEYS[1]) or '0')
		local target = tonumber(ARGV[1])
		if cur < target then
			redis.call('SET', KEYS[1], target)
			redis.call('EXPIRE', KEYS[1], 2592000)
		end
		return 1
	`
	_, err := l.svcCtx.Redis.EvalCtx(l.ctx, lua, []string{key}, strconv.FormatInt(seq, 10))
	return err
}
