// Code scaffolded by goctl. Safe to edit.
// goctl 1.10.1

package logic

import (
	"context"

	"im-platform/app/group/rpc/group"
	"im-platform/app/im/api/internal/svc"
	"im-platform/app/im/api/internal/types"

	"github.com/zeromicro/go-zero/core/logx"
)

type GetGroupInfoLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewGetGroupInfoLogic(ctx context.Context, svcCtx *svc.ServiceContext) *GetGroupInfoLogic {
	return &GetGroupInfoLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *GetGroupInfoLogic) GetGroupInfo(req *types.GetGroupInfoReq) (*types.GetGroupInfoResp, error) {
	resp, err := l.svcCtx.Group.GetGroupInfo(l.ctx, &group.GetGroupInfoReq{GroupId: req.GroupId})
	if err != nil {
		return nil, err
	}
	g := resp.Info
	return &types.GetGroupInfoResp{Info: types.GroupInfo{
		Id:               g.Id,
		Name:             g.Name,
		Avatar:           g.Avatar,
		OwnerId:          g.OwnerId,
		Notice:           g.Notice,
		MemberCount:      g.MemberCount,
		MaxMember:        g.MaxMember,
		MemberVersion:    g.MemberVersion,
		GroupType:        g.GroupType,
		Status:           g.Status,
		InvitePermission: g.InvitePermission,
		JoinApproval:     g.JoinApproval,
		CreatedAt:        g.CreatedAt,
		UpdatedAt:        g.UpdatedAt,
	}}, nil
}
