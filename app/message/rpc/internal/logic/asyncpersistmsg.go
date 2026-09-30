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
	Msg     *models.Messages   // 消息主表（1条）
	Seq     *models.Seqs       // 序号更新（1条）
	Inboxes []*models.Inboxes  // 收件箱（N条，写扩散）
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

// persistBundle 保证一个 bundle 内：msg -> seq -> inbox 顺序执行
func (l *AsyncPersistMsg) persistBundle(ctx context.Context, b *WriteDiffBundle) error {
	// 1. 先持久化消息主表（根基，必须先落）
	if err := l.persistMsg(ctx, b.Msg); err != nil {
		return fmt.Errorf("persist msg failed: %w", err)
	}

	// 2. 更新会话序号
	if err := l.persistSeq(ctx, b.Seq); err != nil {
		return fmt.Errorf("persist seq failed: %w", err)
	}

	// 3. 最后写扩散收件箱（依赖 msg 已存在）
	if err := l.persistInboxes(ctx, b.Inboxes); err != nil {
		return fmt.Errorf("persist inboxes failed: %w", err)
	}

	return nil
}

// persistMsg 消息主表：先 Kafka，失败则同步幂等写入 PG
func (l *AsyncPersistMsg) persistMsg(ctx context.Context, msg *models.Messages) error {
	if msg == nil {
		return fmt.Errorf("msg is nil")
	}

	payload, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshal msg failed: %w", err)
	}

	// 尝试发 Kafka(key=conv_id,保证同会话消息同分区有序)
	if err := l.svcCtx.KafkaProducer.PublishWithKey(ctx, mq.TopicMsgPersist, msg.Convid, payload); err != nil {
		logx.WithContext(ctx).Errorf("kafka publish msg failed, fallback to pg: %v", err)

		// Fallback：同步写 PG。依赖 messages 表的 client_msg_id 唯一索引幂等
		if _, err := l.svcCtx.MessagesModel.Insert(ctx, msg); err != nil {
			return fmt.Errorf("pg insert msg fallback failed: %w", err)
		}
	}
	return nil
}

// persistSeq 序号：先 Kafka，失败则同步 Upsert PG
func (l *AsyncPersistMsg) persistSeq(ctx context.Context, seq *models.Seqs) error {
	if seq == nil {
		return nil // seq 可能由 Redis 维护，PG 只是兜底，允许 nil
	}

	payload, err := json.Marshal(seq)
	if err != nil {
		return fmt.Errorf("marshal seq failed: %w", err)
	}

	if err := l.svcCtx.KafkaProducer.PublishWithKey(ctx, mq.TopicSeqPersist, seq.ConvId, payload); err != nil {
		logx.WithContext(ctx).Errorf("kafka publish seq failed, fallback to pg: %v", err)

		// Fallback：Upsert（INSERT ... ON CONFLICT UPDATE）
		if _,err := l.svcCtx.SeqModel.CustomQueryRowCtx(ctx, seq.ConvId); err != nil {
			return fmt.Errorf("pg upsert seq fallback failed: %w", err)
		}
	}
	return nil
}

// persistInboxes 收件箱：先 Kafka，失败则同步批量写入 PG
func (l *AsyncPersistMsg) persistInboxes(ctx context.Context, inboxes []*models.Inboxes) error {
	if len(inboxes) == 0 {
		return nil
	}
	payload, err := json.Marshal(inboxes)
	if err != nil {
		return fmt.Errorf("marshal inboxes failed: %w", err)
	}

	if err := l.svcCtx.KafkaProducer.PublishWithKey(ctx, mq.TopicMsgInbox, inboxes[0].Convid, payload); err != nil {
		logx.WithContext(ctx).Errorf("kafka publish inboxes failed, fallback to pg: %v", err)

		// Fallback：批量插入。依赖 (user_id, msg_id) 或 (user_id, conv_id, seq_id) 唯一索引幂等
		if err := l.svcCtx.InboxesModel.BatchInsert(ctx, inboxes); err != nil {
			return fmt.Errorf("pg batch insert inboxes fallback failed: %w", err)
		}
	}
	return nil
}