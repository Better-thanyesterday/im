package logic

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"im-platform/app/group/rpc/group"
	"im-platform/app/message/rpc/internal/svc"
	"im-platform/app/message/rpc/message"
	"im-platform/app/message/rpc/models"
	"im-platform/app/push/rpc/push"
	"im-platform/common/constants"
	"im-platform/common/mq"
	"sync"
	"time"

	"github.com/zeromicro/go-zero/core/logx"
)

type GroupChatLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewGroupChatLogic(ctx context.Context, svcCtx *svc.ServiceContext) *GroupChatLogic {
	return &GroupChatLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *GroupChatLogic) Send(in *message.SendMessageReq, convid string) (*message.SendMessageResp, error) {
	// ========== 2. 群成员与权限校验 ==========
	if err := l.validateSender(in.SenderId, in.ToUid); err != nil {
		return nil, err
	}
	groupInfo, err := l.svcCtx.Group.GetGroupInfo(l.ctx, &group.GetGroupInfoReq{
		GroupId: in.ToUid,
	})
	if err != nil {
		l.Errorf("GetGroup failed: %v", err)
		return nil, fmt.Errorf("get group info failed")
	}
	if groupInfo.Info.Status != 1 {
		return nil, fmt.Errorf("group not available")
	}
	// ========== 4. 分配 SeqID（Redis INCR，失败则 PG 兜底） & MsgID（雪花） ==========
	seqId, err ,_:= NewSeqIdLogic(l.ctx, l.svcCtx).AllocateSeq(convid)
	if err != nil {
		l.Errorf("alloc seqId failed: %v", err)
		return nil, fmt.Errorf("alloc seq failed")
	}
	msgId := l.svcCtx.Snokflake.NextID()
	// ========== 5. 构造消息体并异步持久化到 Kafka ==========
	msg := l.buildMessageEntity(msgId, seqId, convid, in)
	if err := l.persistMessageAsync(msg); err != nil {
		l.Errorf("persist async failed: %v", err)
		// Kafka 侧有重试机制，不阻断主流程
	}
	// ========== 6. 注册幂等键（24h TTL） ==========
	l.svcCtx.DedupModel.Set(l.ctx, convid, in.ClientMsgId)
	// ========== 7. 自适应扩散策略 ==========
	if groupInfo.Info.MemberCount < 500 { // 小群 < 500人：写扩散
		if err := l.writeDiffusion(msg, in, groupInfo.Info); err != nil {
			l.Errorf("writeDiffusion error: %v", err)
		}
	} else { // 大群 > 500人:读扩散
		if err := l.readDiffusion(msg, in, groupInfo.Info); err != nil {
			l.Errorf("readDiffusion error: %v", err)
		}
	}
	// ========== 8. 返回服务端 ACK ==========
	return &message.SendMessageResp{
		ClientMsgId: in.ClientMsgId,
		MsgId:       msgId,
		SeqId:       seqId,
		ConvId:      convid,
		SendTime:    msg.Sendtime.UnixMilli(),
		Isdup:       false,
	}, nil
}

func (l *GroupChatLogic) validateSender(senderId, groupId int64) error {
	checkResp, err := l.svcCtx.Group.CheckMember(l.ctx, &group.CheckMemberReq{
		GroupId: groupId,
		UserId:  senderId,
	})
	if err != nil {
		return fmt.Errorf("check member rpc failed: %w", err)
	}
	if !checkResp.IsMember {
		return fmt.Errorf("sender is not group member")
	}
	if checkResp.MuteUntil > 0 && time.Now().Unix() < checkResp.MuteUntil {
		return fmt.Errorf("sender is muted")
	}
	return nil
}

// buildMessageEntity 构造消息数据库实体
func (l *GroupChatLogic) buildMessageEntity(msgId, seqId int64, convId string, in *message.SendMessageReq) *models.Messages {
	content, _ := json.Marshal(in.Body)
	extra, _ := json.Marshal(in.Extra)
	now := time.Now()
	return &models.Messages{
		Id:          msgId,
		Convid:      convId,
		Senderid:    in.SenderId,
		Clientmsgid: in.ClientMsgId,
		Content:     string(content),
		Msgtype:     int64(in.MsgType),
		Status:      constants.MsgStatusNormal,
		Extra:       sql.NullString{String: string(extra), Valid: true},
		CreatedAt:   now,
		Sendtime:    now,
		Seqid:       seqId,
	}
}

// persistMessageAsync 写入 Kafka Topic `im.msg.persist` 异步落库
func (l *GroupChatLogic) persistMessageAsync(msg *models.Messages) error {
	payload, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	return l.svcCtx.KafkaProducer.Publish(l.ctx, mq.TopicMsgPersist,payload)
}

// writeDiffusion 小群写扩散（< 500人）
// 为每个成员（排除发送方、免打扰成员）生成 inbox，批量写入 Kafka；调用 Push.Deliver 投递
func (l *GroupChatLogic) writeDiffusion(msg *models.Messages, in *message.SendMessageReq, groupinfo *group.GroupInfo) error {
	mentions := l.extractMentions(in.Extra)
	mentionSet := make(map[int64]struct{}, len(mentions))
	for _, uid := range mentions {
		mentionSet[uid] = struct{}{}
	}
	// 分页拉取全量成员（每页 200，避免单次 RPC 过大）
	pageSize := int64(200)
	var allMember []*group.MemberInfo
	for page := 1; ; page++ {
		resp, err := l.svcCtx.Group.GetMembers(l.ctx, &group.GetMembersReq{
			GroupId:  in.ToUid,
			PageSize: int32(pageSize),
		})
		if err != nil {
			return fmt.Errorf("get members failed: %w", err)
		}
		allMember = append(allMember, resp.Members...)
		if len(resp.Members) < int(pageSize) {
			break
		}
	}
	now := time.Now()
	var inboxBatch []*models.Inboxes
	var wg sync.WaitGroup
	errCh := make(chan error, len(allMember))
	pushMsg := l.buildPushMessage(msg, in)
	for _, member := range allMember {
		if member.UserId == in.SenderId {
			continue
		}
		// 免打扰成员跳过，除非被 @ 提及 可以补全

		inboxBatch = append(inboxBatch, &models.Inboxes{
			Userid:    member.UserId,
			Msgid:     msg.Id,
			Convid:    msg.Convid,
			Isread:    false,
			Status:    constants.InboxStatusNormal,
			CreatedAt: now,
			Seqid:     msg.Seqid,
		})
		// 每 500 条刷一次 inbox Kafka
		if len(inboxBatch) >= 500 {
			if err := l.batchSendInbox(inboxBatch); err != nil {
				logx.Errorf("batch send inbox failed: %v", err)
			}
			inboxBatch = inboxBatch[:0]
		}
		wg.Add(1)
		go func(uid int64) {
			defer wg.Done()
			_, err := l.svcCtx.Push.Deliver(l.ctx, &push.DeliverReq{
				UserId:   uid,
				PushType: push.PushType_PushTypeFull,
				Message:  pushMsg,
			})
			if err != nil {
				errCh <- fmt.Errorf("push to %d failed: %w", uid, err)
			}

		}(member.UserId)
	}
	// 刷入剩余 inbox
	if len(inboxBatch) > 0 {
		if err := l.batchSendInbox(inboxBatch); err != nil {
			logx.Errorf("batch send inbox failed: %v", err)
		}
	}
	wg.Wait()
	close(errCh)

	// 收集推送错误（仅日志，不阻断）
	for err := range errCh {
		l.Errorf("push error: %v", err)
	}
	return nil
}

// readDiffusion 大群读扩散（>= 500人）
// 1. 对被 @ 成员强推完整消息（逐个 Deliver）
// 2. 普通在线成员通过 Kafka 广播轻量通知，由 Push 服务消费后广播（避免 Message 服务遍历大群做 RPC）
func (l *GroupChatLogic) readDiffusion(msg *models.Messages, in *message.SendMessageReq, groupinfo *group.GroupInfo) error {
	mentions := l.extractMentions(in.Extra)
	pushMsg := l.buildPushMessage(msg, in)
	// Push 服务内部负责：查询在线成员 → 推送轻量通知 {group_id, new_seq}
	// 对 @ 成员无论免打扰均强推完整消息
	for _, uid := range mentions {
		_, err := l.svcCtx.Push.Deliver(l.ctx, &push.DeliverReq{
			UserId:   uid,
			PushType: push.PushType_PushTypeFull,
			Message:  pushMsg,
		})
		if err != nil {
			l.Errorf("push mention to %d failed: %v", uid, err)
		}

	}
	// 2. 轻量通知：写入 Kafka，由 Push 服务消费后广播给所有在线成员
	//    通知体只包含群 ID 和最新 Seq，客户端收到后主动 SyncMessages 拉取
	notify := map[string]interface{}{
		"event":    "group_new_msg_lite",
		"group_id":  in.ToUid,
		"conv_id":    msg.Convid,
		"seq_id":    msg.Seqid,
		"sender_id": msg.Senderid,
		"timestamp": msg.Sendtime.UnixMilli(),
	}
	payload, _ := json.Marshal(notify)
	return l.svcCtx.KafkaProducer.Publish(l.ctx,mq.TopicMsgPersist, payload)
}

// batchSendInbox 批量发送 inbox 到 Kafka
func (l *GroupChatLogic) batchSendInbox(inboxes []*models.Inboxes) error {
	payload, err := json.Marshal(inboxes)
	if err != nil {
		return err
	}

	return l.svcCtx.KafkaProducer.Publish(l.ctx,mq.TopicMsgPersist, payload)
}

// extractMentions 从 Extra 中提取 @ 列表
func (l *GroupChatLogic) extractMentions(extra *message.MessageExtra) []int64 {
	if extra == nil || len(extra.Mentions) == 0 {
		return nil
	}
	return extra.Mentions
}

// buildPushMessage 构造 PushMessage（与 push.proto 对齐）
func (l *GroupChatLogic) buildPushMessage(msg *models.Messages, in *message.SendMessageReq) *push.PushMessage {
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
