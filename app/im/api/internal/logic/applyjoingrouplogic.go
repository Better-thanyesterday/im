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

type ApplyJoinGroupLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewApplyJoinGroupLogic(ctx context.Context, svcCtx *svc.ServiceContext) *ApplyJoinGroupLogic {
	return &ApplyJoinGroupLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

// ApplyJoinGroup 入群申请:申请人身份以 token 为准;免审批群直接入群
func (l *ApplyJoinGroupLogic) ApplyJoinGroup(req *types.ApplyJoinGroupReq) (*types.ApplyJoinGroupResp, error) {
	uid, ok := middleware.GetUserID(l.ctx)
	if !ok || uid <= 0 {
		return nil, fmt.Errorf("unauthorized")
	}
	resp, err := l.svcCtx.Group.ApplyJoinGroup(l.ctx, &group.ApplyJoinGroupReq{
		GroupId:     req.GroupId,
		ApplicantId: uid,
		ApplyReason: req.ApplyReason,
	})
	if err != nil {
		return nil, err
	}
	return &types.ApplyJoinGroupResp{ApplyId: resp.ApplyId, Status: resp.Status}, nil
}
