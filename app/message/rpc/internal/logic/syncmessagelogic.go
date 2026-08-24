package logic

import (
	"context"
	"encoding/json"
	"fmt"

	"im-platform/app/message/rpc/internal/svc"
	"im-platform/app/message/rpc/message"
	"im-platform/app/message/rpc/models"
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
	// todo: add your logic here and delete this line
	if in.UserId <= 0 {
		return nil, constants.NewMsgError(constants.ErrCodeMsgInValidParam)
	}
	//构造ConvSyncs包含多条离线信息
	resp := &message.SyncMessageResp{
		ConvSyncs: make([]*message.SyncMessageResp_ConvSync, 0, len(in.ConvList)),
	}
	//遍历req包含拉取多个Conversation未读信息
	for _, conv := range in.ConvList {
		if conv.ConvId == "" {
			continue
		}
		cs, err := l.syncSingleConv(in.UserId, conv)
		if err != nil {
			logx.Errorf("sync conv %s err: %v", conv.ConvId, err)
			continue
		}
		if cs != nil && (len(cs.Message) > 0 || cs.HasMore) {
			resp.ConvSyncs = append(resp.ConvSyncs, cs)
		}
	}
	return &message.SyncMessageResp{}, nil
}

// syncSingleConv 处理单个会话的同步
func (l *SyncMessageLogic) syncSingleConv(userId int64, conv *message.SyncMessageReq_ConSeq) (*message.SyncMessageResp_ConvSync, error) {
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
		// 小差值：优先 Redis 离线信箱，失败降级 PG
		msgs, hasMore, err = l.syncFromOffline(userId, conv.ConvId, startSeq, serverSeq, limit)
		if err != nil {
			logx.Errorf("offline sync failed, fallback to db: %v", err)
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

// getServerSeq 获取会话当前最大 Seq
func (l *SyncMessageLogic) getServerSeq(convId string) (int64, error) {
	seqKey := fmt.Sprintf("im:seq:%s", convId)
	val, err := l.svcCtx.Redis.GetCtx(l.ctx, seqKey)
	if err != nil && val != "" {
		return strconv.ParseInt(val, 10, 64)
	}
	seq, err := l.svcCtx.SeqModel.FindOne(l.ctx, convId)
	if err != nil {
		return 0, err
	}
	return seq.MaxSeq, nil
}

// syncFromOffline 从 Redis 离线信箱拉取
// 离线信箱: ZSet key=im:offline:{user_id}, member=msg_id, score=seq_id
func (l *SyncMessageLogic) syncFromOffline(userId int64, convId string, startSeq, endSeq, limit int64) ([]*message.MessageBody, bool, error) {
	offlineKey := fmt.Sprintf("im:offline:%d", userId)
	// 按 score（seq_id）范围取 msg_id 列表
	members, err := l.svcCtx.Redis.ZRevRangeWithScoresCtx(l.ctx, offlineKey, startSeq, endSeq)
	if err != nil {
		return nil, false, err
	}
	if len(members) == 0 {
		return nil, false, nil
	}
	// 提取 msg_id
	msgIds := make([]int64, 0, len(members))
	for _, m := range members {
		id, _ := strconv.ParseInt(m.Key, 10, 64)
		if id > 0 {
			msgIds = append(msgIds, id)
		}
	}
	// 批量反查 PG 获取完整消息（过滤非本会话的）
	// 注意：离线信箱是用户维度，可能混有多个会话的消息
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
		result = append(result, l.toMessage(m))
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

func (l *SyncMessageLogic) getUserIdFromCtx() int64 {
	v := l.ctx.Value("x-user-id")
	if v == nil {
		return 0
	}
	return v.(int64)
}
