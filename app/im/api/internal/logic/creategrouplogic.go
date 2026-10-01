// Code scaffolded by goctl. Safe to edit.
// goctl 1.10.1

package logic

import (
	"context"
	"github.com/zeromicro/go-zero/core/logx"
	"im-platform/app/im/api/internal/svc"
	"im-platform/app/im/api/internal/types"
	"im-platform/app/group/rpc/group"
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

func (l *CreateGroupLogic) CreateGroup(req *types.CreateGroupReq) (resp *types.CreateGroupResp, err error) {
	// todo: add your logic here and delete this line
	res, err := l.svcCtx.Group.CreateGroup(l.ctx, &group.CreateGroupReq{
		CreatorId:        req.CreatorId,
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
