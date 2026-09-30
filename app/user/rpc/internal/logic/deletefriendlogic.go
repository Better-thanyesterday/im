package logic

import (
	"context"
	"fmt"

	"im-platform/app/user/rpc/internal/svc"
	"im-platform/app/user/rpc/user"

	"github.com/zeromicro/go-zero/core/logx"
)

type DeleteFriendLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewDeleteFriendLogic(ctx context.Context, svcCtx *svc.ServiceContext) *DeleteFriendLogic {
	return &DeleteFriendLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *DeleteFriendLogic) DeleteFriend(in *user.DeleteFriendReq) (*user.DeleteFriendResp, error) {
	// todo: add your logic here and delete this line
	if in.UserId <= 0 || in.FriendUserId <= 0 {
		return nil, fmt.Errorf("invalid delete friend req: user=%d friend=%d", in.UserId, in.FriendUserId)
	}
	// 定向删除本人与对方的双向关系,避免生成版 Delete 单列 where 误删其他用户与该好友的关系
	err := l.svcCtx.FriendsModel.DeleteRelation(l.ctx, in.UserId, in.FriendUserId)
	if err != nil {
		return nil, err
	}
	return &user.DeleteFriendResp{
		Success: true,
	}, nil
}
