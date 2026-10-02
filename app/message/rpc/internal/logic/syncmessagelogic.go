package logic

import (
	"context"
	"encoding/json"
	"fmt"

	"im-platform/app/group/rpc/groupclient"
	"im-platform/app/message/rpc/internal/svc"
	"im-platform/app/message/rpc/message"
	"im-platform/app/message/rpc/models"
	userclient "im-platform/app/user/rpc/userclient"
	"im-platform/common/constants"
	"strconv"

	"github.com/zeromicro/go-zero/core/logx"
)

type SyncMessageLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewSyncMessageLogic(ctx context.Context, svcCtx *svc.ServiceContext) *SyncMessageLogic {
	return &SyncMessageLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

// SyncMessages 断线补发 + 消息漫游入口
func (l *SyncMessageLogic) SyncMessage(in *message.SyncMessageReq) (*message.SyncMessageResp, error) {
	// 身份以 gateway 注入的 metadata 为准,请求字段仅作内部调用回退,防止拉取他人离线消息
	userId := userIDFromCtx(l.ctx)
	if userId <= 0 {
		userId = in.UserId
	}
	if userId <= 0 {
		return nil, constants.NewMsgError(constants.ErrCodeMsgInValidParam)
	}
	//构造ConvSyncs包含多条离线信息
	resp := &message.SyncMessageResp{
		ConvSyncs: make([]*message.SyncMessageResp_ConvSync, 0, len(in.ConvList)),
	}
	// ConvList 为空 = 断线重连全量补拉:服务端组装该用户的会话列表,
	// 水位取 inbox 已投递到的最大 seq(inboxes 里收发双方都有行)
	convList := in.ConvList
	if len(convList) == 0 {
		var err error
		if convList, err = l.buildUserConvList(userId); err != nil {
			return nil, err
		}
	}
	//遍历req包含拉取多个Conversation未读信息
	for _, conv := range convList {
		if conv.ConvId == "" {
			continue
		}
		cs, err := l.SyncSingleConv(userId, conv)
		if err != nil {
			logx.Errorf("sync conv %s err: %v", conv.ConvId, err)
			continue
		}
		if cs != nil && (len(cs.Message) > 0 || cs.HasMore) {
			resp.ConvSyncs = append(resp.ConvSyncs, cs)
		}
	}
	return resp, nil
}

// syncSingleConv 处理单个会话的同步
func (l *SyncMessageLogic) SyncSingleConv(userId int64, conv *message.SyncMessageReq_ConSeq) (*message.SyncMessageResp_ConvSync, error) {
	serverSeq, err := l.getServerSeq(conv.ConvId)
	if err != nil {
		return nil, err
	}
	clientSeq := conv.LastSeq
	if clientSeq >= serverSeq {
		return nil, nil
	}
	diff := serverSeq - clientSeq
	startSeq := clientSeq + 1
	cs := &message.SyncMessageResp_ConvSync{
		ConvId:    conv.ConvId,
		ServerSeq: serverSeq,
	}
	const limit int64 = 100
	var msgs []*message.MessageBody
	var hasMore bool
	if diff <= 1000 {
		// 小差值：优先 Redis 离线信箱，失败/为空降级 PG
		msgs, hasMore, err = l.syncFromOffline(userId, conv.ConvId, startSeq, serverSeq, limit)
		// 优先级修复:原条件 err == nil && hasMore == false || err != nil 因运算符优先级,
		// 成功取到数据也会误打错误日志并白查一次 DB;正确语义是"失败或没取到"才降级
		if err != nil {
			logx.Errorf("offline sync failed, fallback to db: %v", err)
		}
		if err != nil || len(msgs) == 0 {
			msgs, hasMore, err = l.syncFromDB(conv.ConvId, startSeq, serverSeq, limit)
		}
	} else {
		msgs, hasMore, err = l.syncFromDB(conv.ConvId, startSeq, serverSeq, limit)
	}
	if err != nil {
		return nil, err
	}
	cs.Message = msgs
	cs.HasMore = hasMore
	return cs, nil
}

// getServerSeq 获取会话当前最大 Seq。
// 必须以 messages 表 max(seqid) 为准:Redis im:seq 是 +100 批量预分配计数器,
// 值含未分配的 padding(会话只有 1 条消息时计数器已是 100,diff 虚高反复空拉)
func (l *SyncMessageLogic) getServerSeq(convId string) (int64, error) {
	return l.svcCtx.MessagesModel.MaxSeq(l.ctx, convId)
}

// buildUserConvList 组装用户的全部会话(单聊=好友列表,群聊=所在群),
// 每个会话的 last_seq 取 inbox 已投递水位,重连补拉不重发也不漏
func (l *SyncMessageLogic) buildUserConvList(userId int64) ([]*message.SyncMessageReq_ConSeq, error) {
	convIds := make([]string, 0, 32)
	// 单聊:好友列表
	friendResp, err := l.svcCtx.User.GetFriends(l.ctx, &userclient.GetFriendsReq{
		UserId:   userId,
		Page:     1,
		PageSize: 500,
	})
	if err != nil {
		return nil, fmt.Errorf("get friends for sync failed: %w", err)
	}
	for _, f := range friendResp.Friends {
		convIds = append(convIds, constants.BuildSingleConvID(userId, f.UserId))
	}
	// 群聊:所在群
	groupResp, err := l.svcCtx.Group.GetUserGroups(l.ctx, &groupclient.GetUserGroupsReq{UserId: userId})
	if err != nil {
		return nil, fmt.Errorf("get user groups for sync failed: %w", err)
	}
	for _, gid := range groupResp.GroupIds {
		convIds = append(convIds, constants.BuildGroupConvID(gid))
	}
	convs := make([]*message.SyncMessageReq_ConSeq, 0, len(convIds))
	for _, convId := range convIds {
		lastSeq, err := l.svcCtx.InboxesModel.DeliveredMaxSeq(l.ctx, userId, convId)
		if err != nil {
			logx.Errorf("获取已投递水位失败 | user=%d conv=%s err=%v", userId, convId, err)
			lastSeq = 0
		}
		convs = append(convs, &message.SyncMessageReq_ConSeq{
			ConvId:  convId,
			LastSeq: lastSeq,
		})
	}
	return convs, nil
}

// syncFromOffline 从 Redis 离线信箱拉取
// 离线信箱: 每会话独立 ZSet key=im:offlineinbox:{uid}:{convId}, member=msg_id, score=seq_id
// 按会话拆 key 后,本函数的 score 区间拉取与清理都只作用于本会话,不会误删/混入其他会话的条目
// (key 由 push 侧 storeOffline 写入,两端约定必须一致)
func (l *SyncMessageLogic) syncFromOffline(userId int64, convId string, startSeq, endSeq, limit int64) ([]*message.MessageBody, bool, error) {
	offlineKey := fmt.Sprintf("im:offlineinbox:%d:%s", userId, convId)
	// 带 LIMIT:大区间(如长离线)一次性拉取会打爆内存,超出的部分标记 has_more 走下轮
	members, err := l.svcCtx.Redis.ZrevrangebyscoreWithScoresAndLimitCtx(l.ctx, offlineKey, startSeq, endSeq, 0, int(limit))
	if err != nil {
		return nil, false, err
	}
	if len(members) == 0 {
		return nil, false, nil
	}
	// 提取 msg_id 与实际取到的最高水位
	msgIds := make([]int64, 0, len(members))
	var maxFetched int64
	for _, m := range members {
		id, _ := strconv.ParseInt(m.Key, 10, 64)
		if id > 0 {
			msgIds = append(msgIds, id)
		}
		if m.Score > maxFetched {
			maxFetched = m.Score
		}
	}
	// 批量反查 PG 获取完整消息(信箱 key 已按会话隔离,Convid 过滤仅作防御)
	dbMsgs, err := l.svcCtx.MessagesModel.BatchGetByIds(l.ctx, convId, msgIds)
	if err != nil {
		return nil, false, err
	}
	result := make([]*message.MessageBody, 0, len(dbMsgs))
	for _, m := range dbMsgs {
		if m.Convid != convId || m.Seqid < startSeq || m.Seqid > endSeq {
			continue
		}
		result = append(result, l.toMessage(m))
		if int64(len(result)) >= limit {
			return result, true, nil
		}
	}
	// 已消费区间清理:信箱是缓存(PG 兜底),不清会无限增长;
	// 只清实际取到的 [startSeq, maxFetched],hasMore 的剩余部分留给下一轮
	if maxFetched > 0 {
		if _, err := l.svcCtx.Redis.ZremrangebyscoreCtx(l.ctx, offlineKey, startSeq, maxFetched); err != nil {
			logx.Errorf("cleanup offline inbox failed: key=%s err=%v", offlineKey, err)
		}
	}
	// 如果 Redis 取出的消息没能覆盖到 endSeq，说明中间有缺失（过期/清理）
	// 返回已有部分，并标记 has_more，客户端会再次 sync，下次走 PG 补全
	if len(result) > 0 && result[len(result)-1].SeqId < endSeq {
		return result, true, nil
	}
	return result, false, nil
}

// syncFromDB 从 PG 直接分页拉取（messages 表）
// 单聊/大群读扩散直接查 messages；小群写扩散也可查 messages（因为 messages 有 conv_id 索引）

func (l *SyncMessageLogic) syncFromDB(convId string, startSeq, endSeq, limit int64) ([]*message.MessageBody, bool, error) {
	msgs, err := l.svcCtx.MessagesModel.FindBySeqRange(l.ctx, convId, startSeq, endSeq, limit+1)
	if err != nil {
		return nil, false, err
	}
	hasMore := int64(len(msgs)) > limit
	if hasMore {
		msgs = msgs[:limit]
	}
	result := make([]*message.MessageBody, 0, len(msgs))
	for _, m := range msgs {
		msg := l.toMessage(m)
		if msg == nil {
			continue
		}
		result = append(result, msg)
	}
	return result, hasMore, nil
}

func (l *SyncMessageLogic) toMessage(m *models.Messages) *message.MessageBody {
	msgType := message.MsgType(m.Msgtype) // 需要从int32转回MsgType也要强转
	var msgContent *message.MessageContent
	if m.Content != "" {
		msgContent = new(message.MessageContent)
		err := json.Unmarshal([]byte(m.Content), msgContent)
		if err != nil {
			logx.Errorf("unmarshal message content: %w", err)
			return nil
		}
	}
	return &message.MessageBody{
		Id:          m.Id,
		SeqId:       m.Seqid,
		SenderId:    m.Senderid,
		ConvId:      m.Convid,
		MsgType:     msgType,
		Content:     msgContent,
		SendTime:    m.Sendtime.UnixMilli(),
		ClientMsgId: m.Clientmsgid,
	}
}
