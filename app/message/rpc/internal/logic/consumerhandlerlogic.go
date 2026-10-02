package logic

import (
	"context"
	"encoding/json"
	"im-platform/app/message/rpc/internal/svc"

	"github.com/IBM/sarama"
	"github.com/zeromicro/go-zero/core/logx"
)

type ConsumerHandlerLogic struct {
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewConsumerHandlerLogic(svcCtx *svc.ServiceContext) *ConsumerHandlerLogic {
	return &ConsumerHandlerLogic{
		svcCtx: svcCtx,
		Logger: logx.WithContext(context.Background()),
	}
}

// PersistBundle 消费 WriteDiffBundle 单载荷,同一 handler 内按 msg → seq → inbox 顺序落库。
// 原来三个 topic 三个消费组并行消费,inbox 可能先于 msg 落库,而重连水位取自 inbox
// (DeliveredMaxSeq),会把水位推过尚未落库的 seq,空洞永不补发;合并后同 key(conv_id)同分区严格有序。
// 各写操作幂等(InsertIgnore/UpsertMaxSeq/BatchInsertIgnore),Kafka 重投安全。
func (l *ConsumerHandlerLogic) PersistBundle(ctx context.Context, msg *sarama.ConsumerMessage) error {
	var bundle WriteDiffBundle
	if err := json.Unmarshal(msg.Value, &bundle); err != nil {
		return err
	}
	// 防御:非 bundle 载荷(如历史遗留的裸 Messages JSON)反序列化后全零,跳过避免垃圾行
	if bundle.Msg == nil && bundle.Seq == nil && len(bundle.Inboxes) == 0 {
		logx.Errorf("persist bundle skip invalid payload: topic=%s partition=%d offset=%d", msg.Topic, msg.Partition, msg.Offset)
		return nil
	}
	if bundle.Msg != nil {
		if bundle.Msg.Id <= 0 || bundle.Msg.Convid == "" {
			logx.Errorf("persist bundle skip invalid msg: topic=%s partition=%d offset=%d", msg.Topic, msg.Partition, msg.Offset)
			return nil
		}
		// 幂等插入:Kafka 重投命中唯一键时静默跳过
		if err := l.svcCtx.MessagesModel.InsertIgnore(ctx, bundle.Msg); err != nil {
			return err
		}
	}
	if bundle.Seq != nil {
		// 防御:conv_id 为空或 MaxSeq 非法的载荷跳过,避免插出垃圾 seqs 行(与 Msg 的全零防御对齐)
		if bundle.Seq.ConvId == "" || bundle.Seq.MaxSeq <= 0 {
			logx.Errorf("persist bundle skip invalid seq: conv=%q maxSeq=%d topic=%s partition=%d offset=%d",
				bundle.Seq.ConvId, bundle.Seq.MaxSeq, msg.Topic, msg.Partition, msg.Offset)
		} else if err := l.svcCtx.SeqModel.UpsertMaxSeq(ctx, bundle.Seq.ConvId, bundle.Seq.MaxSeq); err != nil {
			return err
		}
	}
	if len(bundle.Inboxes) > 0 {
		// 幂等批量插入:重投时命中唯一键的行跳过
		return l.svcCtx.InboxesModel.BatchInsertIgnore(ctx, bundle.Inboxes)
	}
	return nil
}
