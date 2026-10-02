package logic

import (
	"context"
	"encoding/json"
	"fmt"
	"im-platform/app/message/rpc/internal/svc"
	"im-platform/app/message/rpc/models"
	"im-platform/common/mq"

	"github.com/zeromicro/go-zero/core/logx"
)

type AsyncPersistMsg struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewAsyncPersistMsg(ctx context.Context, svcCtx *svc.ServiceContext) *AsyncPersistMsg {
	return &AsyncPersistMsg{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

// WriteDiffBundle 写扩散持久化单元（一包数据）
type WriteDiffBundle struct {
	Msg     *models.Messages  // 消息主表（1条）
	Seq     *models.Seqs      // 序号更新（1条）
	Inboxes []*models.Inboxes // 收件箱（N条，写扩散）
}

// WriteDiffPersistMsg 写扩散批量持久化
func (l *AsyncPersistMsg) WriteDiffPersistMsg(ctx context.Context, bundles ...*WriteDiffBundle) error {
	for i, b := range bundles {
		if err := l.persistBundle(ctx, b); err != nil {
			logx.WithContext(ctx).Errorf("persist bundle[%d] failed: %v", i, err)
			return fmt.Errorf("bundle[%d] persist failed: %w", i, err)
		}
	}
	return nil
}

// persistBundle 把整个 bundle 作为单条载荷发往 TopicMsgPersist,key=conv_id。
// 必须单 topic 单载荷:原来拆成 msg/seq/inbox 三个 topic、三个消费组并行消费,
// inbox 可能先于 msg 落库;而重连补拉的水位取自 inbox(DeliveredMaxSeq),
// 会把客户端水位推过尚未落库的 seq,这个空洞永远不会再补发。
// 单 topic + key=conv_id 保证同会话的 msg bundle 先于 inbox bundle 被消费;
// Kafka 失败时整体降级同步写 PG,顺序仍是 msg → seq → inbox。
func (l *AsyncPersistMsg) persistBundle(ctx context.Context, b *WriteDiffBundle) error {
	if b.Msg == nil && len(b.Inboxes) == 0 {
		return fmt.Errorf("bundle is empty")
	}
	key := ""
	if b.Msg != nil {
		key = b.Msg.Convid
	}
	if key == "" && len(b.Inboxes) > 0 {
		key = b.Inboxes[0].Convid
	}
	payload, err := json.Marshal(b)
	if err != nil {
		return fmt.Errorf("marshal bundle failed: %w", err)
	}

	if err := l.svcCtx.KafkaProducer.PublishWithKey(ctx, mq.TopicMsgPersist, key, payload); err != nil {
		logx.WithContext(ctx).Errorf("kafka publish bundle failed, fallback to pg: %v", err)
		// Fallback：同步按 msg → seq → inbox 顺序写 PG,与消费端 PersistBundle 同序
		return l.persistBundleToPG(ctx, b)
	}
	return nil
}

// persistBundleToPG 同步兜底,msg 用裸 Insert(依赖 client_msg_id 唯一索引,
// 23505 由上游 sendmessagelogic 捕获后反查返回原消息),inbox/seq 与消费端同语义幂等
func (l *AsyncPersistMsg) persistBundleToPG(ctx context.Context, b *WriteDiffBundle) error {
	if b.Msg != nil {
		if _, err := l.svcCtx.MessagesModel.Insert(ctx, b.Msg); err != nil {
			return fmt.Errorf("pg insert msg fallback failed: %w", err)
		}
	}
	if b.Seq != nil {
		// 防御:与消费端 PersistBundle 对齐,conv_id 为空或 MaxSeq 非法不写库
		if b.Seq.ConvId == "" || b.Seq.MaxSeq <= 0 {
			logx.WithContext(ctx).Errorf("pg upsert seq skip invalid seq: conv=%q maxSeq=%d", b.Seq.ConvId, b.Seq.MaxSeq)
		} else if err := l.svcCtx.SeqModel.UpsertMaxSeq(ctx, b.Seq.ConvId, b.Seq.MaxSeq); err != nil {
			return fmt.Errorf("pg upsert seq fallback failed: %w", err)
		}
	}
	if len(b.Inboxes) > 0 {
		// 依赖 (user_id, msg_id) 或 (user_id, conv_id, seq_id) 唯一索引幂等
		if err := l.svcCtx.InboxesModel.BatchInsert(ctx, b.Inboxes); err != nil {
			return fmt.Errorf("pg batch insert inboxes fallback failed: %w", err)
		}
	}
	return nil
}

// persistInboxes 写扩散收件箱:包装成 inbox-only bundle 发往同一 topic。
// 与 msg bundle 同 key(会话)同分区,消费端天然先 msg 后 inbox
func (l *AsyncPersistMsg) persistInboxes(ctx context.Context, inboxes []*models.Inboxes) error {
	if len(inboxes) == 0 {
		return nil
	}
	return l.persistBundle(ctx, &WriteDiffBundle{Inboxes: inboxes})
}
