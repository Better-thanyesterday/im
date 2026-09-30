package logic

import (
	"context"
	"encoding/json"
	"im-platform/app/message/rpc/internal/svc"
	"im-platform/app/message/rpc/models"

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

func (l *ConsumerHandlerLogic)PersistMsg(ctx context.Context,msg *sarama.ConsumerMessage) error{
	var  message models.Messages
        if err := json.Unmarshal(msg.Value, &message); err != nil {
            return err
        }
        // 防御:非消息体载荷(如误发到本 topic 的轻量通知)反序列化后是全零结构,
        // 直接跳过,避免插出垃圾行
        if message.Id <= 0 || message.Convid == "" {
            logx.Errorf("persist msg skip invalid payload: topic=%s partition=%d offset=%d", msg.Topic, msg.Partition, msg.Offset)
            return nil
        }
        // 幂等插入:Kafka 重投命中唯一键时静默跳过,不再唯一键冲突风暴
        return l.svcCtx.MessagesModel.InsertIgnore(ctx, &message)
}


func (l *ConsumerHandlerLogic)PersistSeq(ctx context.Context,msg *sarama.ConsumerMessage) error{
	var  seqs models.Seqs
        if err := json.Unmarshal(msg.Value, &seqs); err != nil {
            return err
        }
        // 只推进不回退的 upsert;原来误用"+1 分配器"(CustomQueryRowCtx),
        // 每消费一条 PG 就 +1,与 Redis 永久脱节
        return l.svcCtx.SeqModel.UpsertMaxSeq(ctx, seqs.ConvId, seqs.MaxSeq)
}


func (l *ConsumerHandlerLogic)PersistToInbox(ctx context.Context,msg *sarama.ConsumerMessage) error{
	logx.Infof("inbox consumer invoked, topic=%s, offset=%d", msg.Topic, msg.Offset)
    var  inboxMsg []*models.Inboxes
        if err := json.Unmarshal(msg.Value, &inboxMsg); err != nil {
            return err
        }
        // 幂等批量插入:重投时命中唯一键的行跳过
        return l.svcCtx.InboxesModel.BatchInsertIgnore(ctx, inboxMsg)
}