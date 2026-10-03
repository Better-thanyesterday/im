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

type GetMembersLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewGetMembersLogic(ctx context.Context, svcCtx *svc.ServiceContext) *GetMembersLogic {
	return &GetMembersLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

// GetMembers 游标分页拉群成员:首页 last_id 传 0,配合 member_version 做客户端缓存校验
func (l *GetMembersLogic) GetMembers(req *types.GetMembersReq) (*types.GetMembersResp, error) {
	resp, err := l.svcCtx.Group.GetMembers(l.ctx, &group.GetMembersReq{
		GroupId:  req.GroupId,
		LastId:   req.LastId,
		PageSize: req.PageSize,
	})
	if err != nil {
		return nil, err
	}
	members := make([]types.Member, 0, len(resp.Members))
	for _, m := range resp.Members {
		members = append(members, types.Member{
			UserId:        m.UserId,
			Role:          m.Role,
			GroupNickname: m.GroupNickname,
		})
	}
	return &types.GetMembersResp{
		Members:       members,
		HasMore:       resp.HasMore,
		MemberVersion: resp.MemberVersion,
		LastId:        resp.LastId,
	}, nil
}
