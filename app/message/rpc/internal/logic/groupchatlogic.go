package logic

import (
	"context"
	"encoding/json"
	"fmt"
	"im-platform/app/group/rpc/group"
	"im-platform/app/message/rpc/internal/svc"
	"im-platform/app/message/rpc/message"
	"im-platform/app/message/rpc/models"
	"im-platform/app/push/rpc/push"
	"im-platform/common/constants"
	"im-platform/common/mq"
	"im-platform/common/utils"
	"math"
	"strconv"
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
	// ==========  群成员与权限校验 ==========
	wdb := WriteDiffBundle{}
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
	seqId, needAsync, err := NewSeqIdLogic(l.ctx, l.svcCtx).AllocateSeq(convid)
	if err != nil {
		return nil, fmt.Errorf("allocate seq failed: %w", err)
	}
	//seq异步持久化：写 Kafka
	if needAsync {
		wdb.Seq = utils.BuildSeqEntity(convid, seqId)
	}
	msgId := l.svcCtx.Snowflake.NextID()
	// ========== 5. 构造消息体并异步持久化到 Kafka ==========
	msg, _ := utils.BuildMsgEntity(msgId, seqId, convid, in)
	wdb.Msg = msg
	// ========== 5.5 持久化消息主表与 seq(Kafka 异步,失败降级同步写 PG) ==========
	// 必须先于扩散执行:inbox 消费依赖 messages 已落库;
	// Kafka 与 PG 兜底都失败说明消息没落库,必须返回错误让客户端重发
	if err := NewAsyncPersistMsg(l.ctx, l.svcCtx).WriteDiffPersistMsg(l.ctx, &wdb); err != nil {
		l.Errorf("persist group msg failed: msgId=%d conv=%s err=%v", msgId, convid, err)
		return nil, fmt.Errorf("persist msg failed: %w", err)
	}
	// ========== 6. 自适应扩散策略 ==========
	// 幂等键已在 SendMessageLogic 分发前统一占坑(TryAcquire),此处不再写
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

// memberCheckState CheckMember 的缓存载荷
type memberCheckState struct {
	IsMember  bool  `json:"is_member"`
	MuteUntil int64 `json:"mute_until"`
}

func (l *GroupChatLogic) validateSender(senderId, groupId int64) error {
	// 结果按 (群,用户) 缓存 60s:原实现每条消息一次 CheckMember RPC + DB EXISTS;
	// 禁言时间戳随缓存下发,陈旧窗口(≤60s)远小于典型禁言时长,可接受
	cacheKey := fmt.Sprintf("im:gck:%d:%d", groupId, senderId)
	if cached, err := l.svcCtx.Redis.GetCtx(l.ctx, cacheKey); err == nil && cached != "" {
		var st memberCheckState
		if json.Unmarshal([]byte(cached), &st) == nil {
			return l.applyMemberCheck(st)
		}
	}
	checkResp, err := l.svcCtx.Group.CheckMember(l.ctx, &group.CheckMemberReq{
		GroupId: groupId,
		UserId:  senderId,
	})
	if err != nil {
		return fmt.Errorf("check member rpc failed: %w", err)
	}
	st := memberCheckState{
		IsMember:  checkResp.IsMember,
		MuteUntil: checkResp.MuteUntil,
	}
	if b, jerr := json.Marshal(&st); jerr == nil {
		_ = l.svcCtx.Redis.SetexCtx(l.ctx, cacheKey, string(b), 60)
	}
	return l.applyMemberCheck(st)
}

func (l *GroupChatLogic) applyMemberCheck(st memberCheckState) error {
	if !st.IsMember {
		return fmt.Errorf("sender is not group member")
	}
	if st.MuteUntil > 0 && time.Now().Unix() < st.MuteUntil {
		return fmt.Errorf("sender is muted")
	}
	return nil
}

// writeDiffusion 小群写扩散（< 500人）
// 成员列表走 member_version 缓存;实时推送合并为一次 BatchDeliver(push 内部受限并发),
// 替代原来 O(N) 次 Push.Deliver(499 个 goroutine 打爆 push)
func (l *GroupChatLogic) writeDiffusion(msg *models.Messages, in *message.SendMessageReq, groupinfo *group.GroupInfo) error {
	// mentions := l.extractMentions(in.Extra)
	// mentionSet := make(map[int64]struct{}, len(mentions))
	// for _, uid := range mentions {
	// 	mentionSet[uid] = struct{}{}
	// }
	memberIds, err := l.groupMemberIds(groupinfo)
	if err != nil {
		return err
	}
	now := time.Now()
	var inboxBatch []*models.Inboxes
	var pushUserIds []int64
	pushMsg := utils.BuildPushMessage(msg, in)
	for _, uid := range memberIds {
		if uid == in.SenderId {
			continue
		}
		// 免打扰成员跳过，除非被 @ 提及 可以补全
		inboxBatch = append(inboxBatch, &models.Inboxes{
			Userid:    uid,
			Msgid:     msg.Id,
			Convid:    msg.Convid,
			Isread:    false,
			Status:    constants.InboxStatusNormal,
			CreatedAt: now,
			Seqid:     msg.Seqid,
		})
		pushUserIds = append(pushUserIds, uid)
		// 每 500 条刷一次 inbox Kafka
		if len(inboxBatch) >= 500 {
			if err := NewAsyncPersistMsg(l.ctx, l.svcCtx).persistInboxes(l.ctx, inboxBatch); err != nil {
				logx.Errorf("batch send inbox failed, submit compensation: %v", err)
				// 必须拷贝:下方 [:0] 复用底层数组,直接提交会被后续 append 改写
				submitBundleCompensation(&WriteDiffBundle{Inboxes: append([]*models.Inboxes(nil), inboxBatch...)})
			}
			inboxBatch = inboxBatch[:0]
		}
	}
	// 发送方也写一行 inbox(isread=true,不计未读),与单聊对齐:
	// 发送方的其他设备靠这行做会话水位同步,缺了会让 buildUserConvList 对本群的水位偏低,重连重复补拉
	inboxBatch = append(inboxBatch, &models.Inboxes{
		Userid:    in.SenderId,
		Msgid:     msg.Id,
		Convid:    msg.Convid,
		Isread:    true,
		Status:    constants.InboxStatusNormal,
		CreatedAt: now,
		Seqid:     msg.Seqid,
	})
	// 刷入剩余 inbox
	if len(inboxBatch) > 0 {
		if err := NewAsyncPersistMsg(l.ctx, l.svcCtx).persistInboxes(l.ctx, inboxBatch); err != nil {
			logx.Errorf("batch send inbox failed, submit compensation: %v", err)
			submitBundleCompensation(&WriteDiffBundle{Inboxes: inboxBatch})
		}
	}
	// 单次批量投递:实时推送失败/离线的用户由 push 内部走离线兜底
	if len(pushUserIds) > 0 {
		if _, err := l.svcCtx.Push.BatchDeliver(l.ctx, &push.BatchDeliverReq{
			UserIds:  pushUserIds,
			PushType: push.PushType_PushTypeFull,
			Message:  pushMsg,
		}); err != nil {
			l.Errorf("batch deliver failed: conv=%s err=%v", msg.Convid, err)
		}
	}
	return nil
}

// groupMemberIds 群成员列表缓存:cache key 携带 member_version,
// 成员变更时版本递增(IncrMembers),缓存自动失效;TTL 30min 兜底兜住版本泄漏场景
func (l *GroupChatLogic) groupMemberIds(groupInfo *group.GroupInfo) ([]int64, error) {
	cacheKey := fmt.Sprintf("im:gmem:%d:v%d", groupInfo.Id, groupInfo.MemberVersion)
	if members, err := l.svcCtx.Redis.Smembers(cacheKey); err == nil && len(members) > 0 {
		ids := make([]int64, 0, len(members))
		for _, s := range members {
			if id, e := strconv.ParseInt(s, 10, 64); e == nil && id > 0 {
				ids = append(ids, id)
			}
		}
		if len(ids) > 0 {
			return ids, nil
		}
	}
	// miss:分页拉全量（每页 200，避免单次 RPC 过大）
	pageSize := int64(200)
	var ids []int64
	lastId := int64(math.MaxInt64)
	for {
		resp, err := l.svcCtx.Group.GetMembers(l.ctx, &group.GetMembersReq{
			GroupId:  groupInfo.Id,
			PageSize: int32(pageSize),
			LastId:   lastId,
		})
		if err != nil {
			return nil, fmt.Errorf("get members failed: %w", err)
		}
		for _, m := range resp.Members {
			ids = append(ids, m.UserId)
		}
		lastId = resp.LastId
		if !resp.HasMore {
			break
		}
	}
	// 回填缓存
	if len(ids) > 0 {
		members := make([]any, 0, len(ids))
		for _, id := range ids {
			members = append(members, strconv.FormatInt(id, 10))
		}
		if _, err := l.svcCtx.Redis.Sadd(cacheKey, members...); err == nil {
			_ = l.svcCtx.Redis.Expire(cacheKey, 1800)
		}
	}
	return ids, nil
}

// readDiffusion 大群读扩散（>= 500人）
// 1. 对被 @ 成员强推完整消息（逐个 Deliver）
// 2. 普通在线成员通过 Kafka 广播轻量通知，由 Push 服务消费后广播（避免 Message 服务遍历大群做 RPC）
func (l *GroupChatLogic) readDiffusion(msg *models.Messages, in *message.SendMessageReq, groupinfo *group.GroupInfo) error {
	// mentions := l.extractMentions(in.Extra)
	// pushMsg := l.buildPushMessage(msg, in)
	// Push 服务内部负责：查询在线成员 → 推送轻量通知 {group_id, new_seq}
	// 对 @ 成员无论免打扰均强推完整消息
	// for _, uid := range mentions {
	// 	_, err := l.svcCtx.Push.Deliver(l.ctx, &push.DeliverReq{
	// 		UserId:   uid,
	// 		PushType: push.PushType_PushTypeFull,
	// 		Message:  pushMsg,
	// 	})
	// 	if err != nil {
	// 		l.Errorf("push mention to %d failed: %v", uid, err)
	// 	}

	// }
	// 2. 轻量通知：写入 Kafka，由 Push 服务消费后广播给所有在线成员
	//    通知体只包含群 ID 和最新 Seq，客户端收到后主动 SyncMessages 拉取
	notify := map[string]interface{}{
		"event":     "group_new_msg_lite",
		"group_id":  in.ToUid,
		"conv_id":   msg.Convid,
		"seq_id":    msg.Seqid,
		"sender_id": msg.Senderid,
		"timestamp": msg.Sendtime.UnixMilli(),
	}
	payload, _ := json.Marshal(notify)
	// 必须发事件广播 topic:TopicMsgPersist 的消费者会把 payload 反序列化成
	// Messages 插库,发错 topic 会产生全零垃圾行;key=conv_id 保同会话通知有序
	return l.svcCtx.KafkaProducer.PublishWithKey(l.ctx, mq.TopicEventNotify, msg.Convid, payload)
}

// extractMentions 从 Extra 中提取 @ 列表
func (l *GroupChatLogic) extractMentions(extra *message.MessageExtra) []int64 {
	if extra == nil || len(extra.Mentions) == 0 {
		return nil
	}
	return extra.Mentions
}
