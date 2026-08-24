package logic

import (
	"context"

	"github.com/zeromicro/go-zero/core/logx"
	"im-platform/app/group/rpc/group"
	"im-platform/app/group/rpc/internal/svc"
)

type DissolveGroupLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewDissolveGroupLogic(ctx context.Context, svcCtx *svc.ServiceContext) *DissolveGroupLogic {
	return &DissolveGroupLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *DissolveGroupLogic) DissolveGroup(in *group.DissolveGroupReq) (*group.DissolveGroupResp, error) {
	// todo: add your logic here and delete this line

	return &group.DissolveGroupResp{}, nil
}
