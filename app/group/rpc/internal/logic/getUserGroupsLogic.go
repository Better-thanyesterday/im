package logic

import (
	"context"
	"fmt"

	"im-platform/app/group/rpc/group"
	"im-platform/app/group/rpc/internal/svc"

	"github.com/zeromicro/go-zero/core/logx"
)

type GetUserGroupsLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewGetUserGroupsLogic(ctx context.Context, svcCtx *svc.ServiceContext) *GetUserGroupsLogic {
	return &GetUserGroupsLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

// 用户所在的群 ID 列表(断线重连补拉时组装会话列表用)
func (l *GetUserGroupsLogic) GetUserGroups(in *group.GetUserGroupsReq) (*group.GetUserGroupsResp, error) {
	// todo: add your logic here and delete this line
	if in.UserId <= 0 {
		return nil, fmt.Errorf("invalid get user groups req: user=%d", in.UserId)
	}
	ids, err := l.svcCtx.GroupMembersModel.GetUserGroupIds(l.ctx, in.UserId)
	if err != nil {
		return nil, err
	}
	return &group.GetUserGroupsResp{
		GroupIds: ids,
	}, nil
}
