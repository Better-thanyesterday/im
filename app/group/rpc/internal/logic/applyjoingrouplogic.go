package logic

import (
	"context"

	"im-platform/app/group/rpc/group"
	"im-platform/app/group/rpc/internal/svc"

	"github.com/zeromicro/go-zero/core/logx"
)

type ApplyJoinGroupLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewApplyJoinGroupLogic(ctx context.Context, svcCtx *svc.ServiceContext) *ApplyJoinGroupLogic {
	return &ApplyJoinGroupLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *ApplyJoinGroupLogic) ApplyJoinGroup(in *group.ApplyJoinGroupReq) (*group.ApplyJoinGroupResp, error) {
	// todo: add your logic here and delete this line

	return &group.ApplyJoinGroupResp{}, nil
}
