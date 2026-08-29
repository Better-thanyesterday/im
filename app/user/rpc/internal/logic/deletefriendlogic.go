package logic

import (
	"context"

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
	err := l.svcCtx.FriendsModel.Delete(l.ctx, in.FriendUserId)
	if err != nil {
		return nil, err
	}
	return &user.DeleteFriendResp{
		Success: true,
	}, nil
}
