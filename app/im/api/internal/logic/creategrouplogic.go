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

type CreateGroupLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewCreateGroupLogic(ctx context.Context, svcCtx *svc.ServiceContext) *CreateGroupLogic {
	return &CreateGroupLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *CreateGroupLogic) CreateGroup(req *types.CreateGroupReq) (*types.CreateGroupResp, error) {
	// 创建者身份以 token 为准,不信任请求体(防冒充任意人建群)
	creatorId, ok := middleware.GetUserID(l.ctx)
	if !ok || creatorId <= 0 {
		return nil, fmt.Errorf("unauthorized")
	}
	res, err := l.svcCtx.Group.CreateGroup(l.ctx, &group.CreateGroupReq{
		CreatorId:        creatorId,
		Name:             req.Name,
		Avatar:           req.Avatar,
		InitialMembers:   req.InitialMembers,
		JoinApproval:     req.JoinApproval,
		GroupType:        req.GroupType,
		InvitePermission: req.InvitePermission,
		MaxMember:        req.MaxMember,
	})
	if err != nil {
		return nil, err
	}
	return &types.CreateGroupResp{
		GroupId:       res.GroupId,
		MemberVersion: res.MemberVersion,
	}, nil
}
