package logic

import (
	"context"

	"im-platform/app/gateway/rpc/gateway"
	"im-platform/app/gateway/rpc/internal/svc"

	"github.com/zeromicro/go-zero/core/logx"
)

type PushToConnLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewPushToConnLogic(ctx context.Context, svcCtx *svc.ServiceContext) *PushToConnLogic {
	return &PushToConnLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *PushToConnLogic) PushToConn(in *gateway.PushToConnReq) (*gateway.PushToConnResp, error) {
	// todo: add your logic here and delete this line

	return &gateway.PushToConnResp{}, nil
}
