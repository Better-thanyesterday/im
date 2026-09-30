package logic

import (
	"context"
	"encoding/json"
	"fmt"
	"im-platform/app/message/rpc/internal/svc"
	"im-platform/app/message/rpc/message"
	"im-platform/app/message/rpc/models"
	"im-platform/app/push/rpc/push"
	userclient "im-platform/app/user/rpc/userclient"
	"im-platform/common/constants"
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
	isFriend, err := l.svcCtx.User.CheckFriend(l.ctx, &userclient.CheckFriendReq{
		UserId:   in.SenderId,
		FriendId: in.ToUid,
	})
	if err != nil {
		return nil, err
	}
	if !isFriend.IsFriend {
		return nil, constants.NewMsgError(constants.ErrCodeMsgNotFriend)
	}
	blocked, err := l.svcCtx.User.IsBlocked(l.ctx, &userclient.IsBlockedReq{
		UserId:   in.ToUid, // 看接收方是否拉黑了发送方
		TargetId: in.SenderId,
	})
	if err != nil {
		return nil, err
	}
	if blocked.IsBlocked {
		return nil, constants.NewMsgError(constants.ErrCodeMsgBlocked)
	}
	wdb := WriteDiffBundle{}
	//2.分配seq_id
	seqId, needAsync, err := NewSeqIdLogic(l.ctx, l.svcCtx).AllocateSeq(convid)
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
		wdb.Seq=seq
	}
	//3.生成msg_id
	msgId := l.svcCtx.Snowflake.NextID()
	content, _ := json.Marshal(in.Body.Content)
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
	// isread 恒为 false:已读状态由 AckMessage 的 read_seq 水位统一推进,
	// "发送时对方是否在线"不是已读语义
	inboxmsg:=models.Inboxes{
		Userid: in.ToUid,
		Msgid: msgId,
		Isread: false,
		Convid: convid,
		Status: 1,
		Seqid: seqId,
	}
	wdb.Inboxes=append(wdb.Inboxes, &inboxmsg)
	// 发送方也写一行 inbox(isread=true,不计未读):
	// 发送方的其他设备靠这行做离线补发/会话状态同步,原实现只写接收方导致缺位
	senderInbox:=models.Inboxes{
		Userid: in.SenderId,
		Msgid: msgId,
		Isread: true,
		Convid: convid,
		Status: 1,
		Seqid: seqId,
	}
	wdb.Inboxes=append(wdb.Inboxes, &senderInbox)
	wdb.Msg=msg
	// WriteDiffPersistMsg 内部已含"Kafka 失败降级同步写 PG"兜底;
	// 两者都失败说明消息彻底没落库,必须返回错误让客户端重发,
	// 否则收方实时收到了但 Sync 永远拉不到
	err=NewAsyncPersistMsg(l.ctx,l.svcCtx).WriteDiffPersistMsg(l.ctx,&wdb)
	if err != nil {
		logx.Errorf("persist msg failed, conv=%s msgId=%d: %v", convid, msgId, err)
		return nil, fmt.Errorf("persist msg failed: %w", err)
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
