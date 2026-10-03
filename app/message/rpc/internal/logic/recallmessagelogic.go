package logic

import (
	"context"
	"errors"
	"fmt"

	"im-platform/app/message/rpc/internal/svc"
	"im-platform/app/message/rpc/message"
	"im-platform/common/constants"

	"github.com/zeromicro/go-zero/core/logx"
)

type RecallMessageLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewRecallMessageLogic(ctx context.Context, svcCtx *svc.ServiceContext) *RecallMessageLogic {
	return &RecallMessageLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

// RecallMessage 撤回消息:仅发送者本人可撤;对方通过下次 Sync 拿到 status=2 感知撤回
func (l *RecallMessageLogic) RecallMessage(in *message.RecallMessageReq) (*message.RecallMessageResp, error) {
	if in.MsgId <= 0 || in.OperatorId <= 0 || in.ConvId == "" {
		return nil, constants.NewMsgError(constants.ErrCodeMsgInValidParam)
	}
	msg, err := l.svcCtx.MessagesModel.FindOne(l.ctx, in.MsgId)
	if err != nil {
		return nil, err
	}
	// 归属校验:消息必须真的在该会话(防伪造 conv_id 撤别的会话的消息)
	if msg.Convid != in.ConvId {
		return nil, fmt.Errorf("消息不属于该会话")
	}
	// 只有发送者可以撤回
	if msg.Senderid != in.OperatorId {
		return nil, errors.New("只有发送者可以撤回消息")
	}
	// 已撤回的重复撤回幂等成功
	if msg.Status == constants.MsgStatusRecalled {
		return &message.RecallMessageResp{}, nil
	}
	if err := l.svcCtx.MessagesModel.RecallMessage(l.ctx, in.MsgId, in.OperatorId); err != nil {
		return nil, err
	}
	return &message.RecallMessageResp{}, nil
}
