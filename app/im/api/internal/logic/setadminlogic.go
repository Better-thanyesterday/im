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

type SetAdminLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewSetAdminLogic(ctx context.Context, svcCtx *svc.ServiceContext) *SetAdminLogic {
	return &SetAdminLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

// SetAdmin 设/撤管理员(仅群主):操作者身份以 token 为准
func (l *SetAdminLogic) SetAdmin(req *types.SetAdminReq) (*types.GroupOpResp, error) {
	uid, ok := middleware.GetUserID(l.ctx)
	if !ok || uid <= 0 {
		return nil, fmt.Errorf("unauthorized")
	}
	resp, err := l.svcCtx.Group.SetAdmin(l.ctx, &group.SetAdminReq{
		GroupId:    req.GroupId,
		OperatorId: uid,
		TargetId:   req.TargetId,
		IsAdmin:    req.IsAdmin,
	})
	if err != nil {
		return nil, err
	}
	return &types.GroupOpResp{MemberVersion: resp.MemberVersion}, nil
}
