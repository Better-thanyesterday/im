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

type MuteMemberLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewMuteMemberLogic(ctx context.Context, svcCtx *svc.ServiceContext) *MuteMemberLogic {
	return &MuteMemberLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

// MuteMember 禁言/解除(mute_until=0 解除):操作者身份以 token 为准
func (l *MuteMemberLogic) MuteMember(req *types.MuteMemberReq) (*types.SuccessResp, error) {
	uid, ok := middleware.GetUserID(l.ctx)
	if !ok || uid <= 0 {
		return nil, fmt.Errorf("unauthorized")
	}
	_, err := l.svcCtx.Group.MuteMember(l.ctx, &group.MuteMemberReq{
		GroupId:    req.GroupId,
		OperatorId: uid,
		TargetId:   req.TargetId,
		MuteUntil:  req.MuteUntil,
	})
	if err != nil {
		return nil, err
	}
	return &types.SuccessResp{Success: true}, nil
}
