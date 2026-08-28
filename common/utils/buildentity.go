package utils

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"im-platform/app/message/rpc/message"
	"im-platform/app/message/rpc/models"
	"im-platform/app/push/rpc/push"
	"im-platform/common/constants"
	"time"
)

func BuildMsgEntity(msgId, seqId int64, convId string, in *message.SendMessageReq) (*models.Messages, error) {
    content, err := json.Marshal(in.Body.Content)
    if err != nil {
        return nil, fmt.Errorf("marshal content: %w", err)
    }
    var extra sql.NullString
    if in.Extra != nil {
        e, err := json.Marshal(in.Extra)
        if err != nil {
            return nil, fmt.Errorf("marshal extra: %w", err)
        }
        extra = sql.NullString{String: string(e), Valid: true}
    }

    // 如果希望发送时间由客户端决定，可改为 in.GetSendTime()
    now := time.Now()
    return &models.Messages{
        Id:          msgId,
        Convid:      convId,
        Senderid:    in.SenderId,
        Clientmsgid: in.ClientMsgId,
        Content:     string(content),
        Msgtype:     int64(in.MsgType),
        Status:      constants.MsgStatusNormal,
        Extra:       extra,
        CreatedAt:   now,
        Sendtime:    now,
        Seqid:       seqId,
    }, nil
}

func BuildSeqEntity(ConvId string, MaxSeq int64) *models.Seqs {
	return &models.Seqs{
		ConvId: ConvId,
		MaxSeq: MaxSeq,
	}
}

func BuildInboxEntity(userId, msgId, seqId int64, convId string, isRead bool, status int64) *models.Inboxes {
    readTime := sql.NullTime{Valid: false}
    return &models.Inboxes{
        Userid:    userId,
        Msgid:     msgId,
        Convid:    convId,
        Seqid:     seqId,
        Isread:    isRead,
        Readtime:  readTime,
        Status:    status,
        CreatedAt: time.Now(),   // 服务端落库时间
    }
}


// buildPushMessage 构造 PushMessage（与 push.proto 对齐）
func BuildPushMessage(msg *models.Messages, in *message.SendMessageReq) *push.PushMessage {
	return &push.PushMessage{
		MsgId:    msg.Id,
		ConvId:   msg.Convid,
		SeqId:    msg.Seqid,
		SenderId: msg.Senderid,
		MsgType:  msg.Msgtype,
		Content:  msg.Content,
		SendTime: msg.Sendtime.UnixMilli(),
		ConvType: true, // 1-群聊
	}
}