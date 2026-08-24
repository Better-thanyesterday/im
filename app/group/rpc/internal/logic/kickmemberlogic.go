package logic

import (
	"context"

	"im-platform/app/group/rpc/group"
	"im-platform/app/group/rpc/internal/svc"

	"github.com/zeromicro/go-zero/core/logx"
)

type KickMemberLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewKickMemberLogic(ctx context.Context, svcCtx *svc.ServiceContext) *KickMemberLogic {
	return &KickMemberLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *KickMemberLogic) KickMember(in *group.KickMemberReq) (*group.KickMemberResp, error) {
	// todo: add your logic here and delete this line

	return &group.KickMemberResp{}, nil
}
