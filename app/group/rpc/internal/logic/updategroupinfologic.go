package logic

import (
	"context"

	"github.com/zeromicro/go-zero/core/logx"
	"im-platform/app/group/rpc/group"
	"im-platform/app/group/rpc/internal/svc"
)

type UpdateGroupInfoLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewUpdateGroupInfoLogic(ctx context.Context, svcCtx *svc.ServiceContext) *UpdateGroupInfoLogic {
	return &UpdateGroupInfoLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *UpdateGroupInfoLogic) UpdateGroupInfo(in *group.UpdateGroupInfoReq) (*group.UpdateGroupInfoResp, error) {
	// todo: add your logic here and delete this line

	return &group.UpdateGroupInfoResp{}, nil
}
