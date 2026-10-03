// Code scaffolded by goctl. Safe to edit.
// goctl 1.10.1

package logic

import (
	"context"
	"fmt"

	"im-platform/app/group/rpc/group"
	"im-platform/app/im/api/internal/svc"
	"im-platform/app/im/api/internal/types"
	"im-platform/common/middleware"

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

func (l *InviteMemberLogic) InviteMember(req *types.InviteMemberReq) (*types.InviteMemberResp, error) {
	// 邀请人身份以 token 为准,不信任请求体(防把任意人拉进任意群)
	inviterId, ok := middleware.GetUserID(l.ctx)
	if !ok || inviterId <= 0 {
		return nil, fmt.Errorf("unauthorized")
	}
	res, err := l.svcCtx.Group.InviteMember(l.ctx, &group.InviteMemberReq{
		GroupId:   req.GroupId,
		InviterId: inviterId,
		UserIds:   req.UserIds,
	})
	if err != nil {
		return nil, err
	}
	return &types.InviteMemberResp{
		FailedUserIds: res.FailedUserIds,
		MemberVersion: res.MemberVersion,
	}, nil
}
