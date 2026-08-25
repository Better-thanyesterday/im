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
        // 调用你的 go-zero 业务逻辑
        if _, err := l.svcCtx.MessagesModel.Insert(ctx, &message); err != nil {
            return err
        }
        return nil
}


func (l *ConsumerHandlerLogic)PersistSeq(ctx context.Context,msg *sarama.ConsumerMessage) error{
	var  seqs models.Seqs
        if err := json.Unmarshal(msg.Value, &seqs); err != nil {
            return err
        }
        // 调用你的 go-zero 业务逻辑
        if _, err := l.svcCtx.SeqModel.CustomQueryRowCtx(ctx, seqs.ConvId); err != nil {
            return err
        }
        return nil
}