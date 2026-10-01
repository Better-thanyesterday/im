// Code scaffolded by goctl. Safe to edit.
// goctl 1.10.1

package logic

import (
	"context"

	"im-platform/app/im/api/internal/svc"
	"im-platform/app/im/api/internal/types"
	"im-platform/app/group/rpc/group"

	"github.com/zeromicro/go-zero/core/logx"
)

type InviteMemberLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewInviteMemberLogic(ctx context.Context, svcCtx *svc.ServiceContext) *InviteMemberLogic {
	return &InviteMemberLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *InviteMemberLogic) InviteMember(req *types.InviteMemberReq) (resp *types.InviteMemberResp, err error) {
	res, err := l.svcCtx.Group.InviteMember(l.ctx, &group.InviteMemberReq{
		GroupId: req.GroupId,
		InviterId: req.InviterId,
		UserIds: req.UserIds,
	})
	if err != nil {
		return nil, err
	}
	return &types.InviteMemberResp{
		FailedUserIds:       res.FailedUserIds,
		MemberVersion: res.MemberVersion,
	}, nil
}
