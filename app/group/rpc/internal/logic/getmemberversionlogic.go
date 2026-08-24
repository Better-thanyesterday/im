package logic

import (
	"context"

	"im-platform/app/group/rpc/group"
	"im-platform/app/group/rpc/internal/svc"

	"github.com/zeromicro/go-zero/core/logx"
)

type GetMemberVersionLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewGetMemberVersionLogic(ctx context.Context, svcCtx *svc.ServiceContext) *GetMemberVersionLogic {
	return &GetMemberVersionLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *GetMemberVersionLogic) GetMemberVersion(in *group.GetMemberVersionReq) (*group.GetMemberVersionResp, error) {
	// todo: add your logic here and delete this line

	return &group.GetMemberVersionResp{}, nil
}
