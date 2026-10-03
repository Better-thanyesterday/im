// Code scaffolded by goctl. Safe to edit.
// goctl 1.10.1

package logic

import (
	"context"
	"fmt"

	"im-platform/app/im/api/internal/svc"
	"im-platform/app/im/api/internal/types"
	messageclient "im-platform/app/message/rpc/messageclient"
	"im-platform/common/middleware"

	"github.com/zeromicro/go-zero/core/logx"
)

type RecallMessageLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewRecallMessageLogic(ctx context.Context, svcCtx *svc.ServiceContext) *RecallMessageLogic {
	return &RecallMessageLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

// RecallMessage 撤回消息:撤回者身份以 token 为准(仅发送者可撤,校验在 message rpc)
func (l *RecallMessageLogic) RecallMessage(req *types.RecallMessageReq) (*types.SuccessResp, error) {
	uid, ok := middleware.GetUserID(l.ctx)
	if !ok || uid <= 0 {
		return nil, fmt.Errorf("unauthorized")
	}
	_, err := l.svcCtx.Message.RecallMessage(l.ctx, &messageclient.RecallMessageReq{
		MsgId:      req.MsgId,
		ConvId:     req.ConvId,
		OperatorId: uid,
	})
	if err != nil {
		return nil, err
	}
	return &types.SuccessResp{Success: true}, nil
}
