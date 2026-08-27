package logic

import (
	"context"

	"im-platform/app/group/rpc/internal/svc"
	"im-platform/app/group/rpc/group"
	"github.com/zeromicro/go-zero/core/logx"
)

type InviteMemberLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewInviteMemberLogic(ctx context.Context, svcCtx *svc.ServiceContext) *InviteMemberLogic {
	return &InviteMemberLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

// ==================== 成员管理 ====================
func (l *InviteMemberLogic) InviteMember(in *group.InviteMemberReq) (*group.InviteMemberResp, error) {
	// todo: add your logic here and delete this line
	
	return &group.InviteMemberResp{}, nil
}
