package logic

import (
	"context"
	"encoding/json"
	"fmt"
	"im-platform/app/message/rpc/internal/svc"
	"im-platform/app/message/rpc/message"
	"im-platform/app/message/rpc/models"
	"im-platform/app/push/rpc/push"
	"im-platform/common/mq"
	"time"

	"github.com/zeromicro/go-zero/core/logx"
)

type SingleChatLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewSingleChatLogic(ctx context.Context, svcCtx *svc.ServiceContext) *SingleChatLogic {
	return &SingleChatLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *SingleChatLogic) Send(in *message.SendMessageReq, convid string) (*message.SendMessageResp, error) {
	//1.检查是否为好友和黑名单
	// isFriend, err := l.svcCtx.UserRpc.CheckFriend(l.ctx, &user.CheckFriendReq{
	// 	UserId:   in.SenderId,
	// 	FriendId: in.ReceiverId,
	// })
	// if err != nil {
	// 	return nil, err
	// }
	// if !isFriend.IsFriend {
	// 	return nil, constants.NewErrCode(constants.ErrNotFriend) // 300003
	// }
	// blocked, err := l.svcCtx.UserRpc.IsBlocked(l.ctx, &user.IsBlockedReq{
	// 	UserId:   in.ReceiverId, // 看接收方是否拉黑了发送方
	// 	TargetId: in.SenderId,
	// })
	// if err != nil {
	// 	return nil, err
	// }
	// if blocked.IsBlocked {
	// 	return nil, constants.NewErrCode(constants.ErrBlocked)
	// }

	//2.分配seq_id
	seqId, err, needAsync := NewSeqIdLogic(l.ctx, l.svcCtx).AllocateSeq(convid)
	if err != nil {
		return nil, fmt.Errorf("allocate seq failed: %w", err)
	}
	//4.seq异步持久化：写 Kafka
	//如果 Kafka 失败，同步降级写 PG
	if needAsync {
		seq := &models.Seqs{
			MaxSeq: seqId,
			ConvId: convid,
		}
		payload, _ := json.Marshal(seq)
		if err := l.svcCtx.KafkaProducer.Publish(l.ctx, mq.TopicSeqPersist, payload); err != nil {
			logx.Errorf("kafka send failed, fallback to pg: %v", err)
			if _, err := l.svcCtx.SeqModel.CustomQueryRowCtx(l.ctx, convid); err != nil {
				logx.Errorf("update failed, fallback to pg: %v", err)
				return nil, err
			}
		}
	}
	//3.生成msg_id
	msgId := l.svcCtx.Snokflake.NextID()
	content, _ := json.Marshal(in.Body.Content)
	fmt.Println(in.Body)
	msg := &models.Messages{
		Id:          msgId,
		Convid:      convid,
		Clientmsgid: in.ClientMsgId,
		Senderid:    in.SenderId,
		Msgtype:     int64(in.MsgType),
		Content:     string(content), // 或 JSONB 序列化
		Seqid:       seqId,
		Sendtime:    time.Now(),
		Status:      1,
	}
	//4.异步持久化：写 Kafka（削峰）
	//如果 Kafka 失败，同步降级写 PG
	payload, _ := json.Marshal(msg)
	if err := l.svcCtx.KafkaProducer.Publish(l.ctx, mq.TopicMsgPersist, payload); err != nil {
		logx.Errorf("kafka send failed, fallback to pg: %v", err)
		if _, err := l.svcCtx.MessagesModel.Insert(l.ctx, msg); err != nil {
			logx.Errorf("insert failed, fallback to pg: %v", err)
			return nil, err
		}
	}

	//5. 调用 Push 服务投递给接收方
	pmsg := &push.PushMessage{
		MsgId:    msgId,
		ConvId:   convid,
		ConvType: in.Isgroup,
		MsgType:  int64(in.MsgType),
		Content:  string(content),
		SeqId:    seqId,
		SenderId: in.SenderId,
		SendTime: in.Body.SendTime,
	}
	_, err = l.svcCtx.Push.Deliver(l.ctx, &push.DeliverReq{
		UserId:   in.ToUid,
		PushType: 1,
		Message:  pmsg,
	})
	if err != nil {
		// Push 失败不阻断发送，消息已入队列/库，由离线机制兜底
		logx.Errorf("push deliver failed: %v", err)
	}

	// 8. 返回服务端 ACK（携带 msg_id + seq_id，客户端用于匹配）
	return &message.SendMessageResp{
		MsgId:    msgId,
		SeqId:    seqId,
		ConvId:   convid,
		SendTime: msg.Sendtime.UnixMilli(),
	}, nil
}
