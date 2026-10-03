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

type UpdateGroupInfoLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewUpdateGroupInfoLogic(ctx context.Context, svcCtx *svc.ServiceContext) *UpdateGroupInfoLogic {
	return &UpdateGroupInfoLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

// UpdateGroupInfo 修改群资料:操作者身份以 token 为准;空串/0 表示不修改
func (l *UpdateGroupInfoLogic) UpdateGroupInfo(req *types.UpdateGroupInfoReq) (*types.SuccessResp, error) {
	uid, ok := middleware.GetUserID(l.ctx)
	if !ok || uid <= 0 {
		return nil, fmt.Errorf("unauthorized")
	}
	_, err := l.svcCtx.Group.UpdateGroupInfo(l.ctx, &group.UpdateGroupInfoReq{
		GroupId:          req.GroupId,
		OperatorId:       uid,
		Name:             req.Name,
		Avatar:           req.Avatar,
		Notice:           req.Notice,
		InvitePermission: req.InvitePermission,
		JoinApproval:     req.JoinApproval,
		MaxMember:        req.MaxMember,
	})
	if err != nil {
		return nil, err
	}
	return &types.SuccessResp{Success: true}, nil
}
