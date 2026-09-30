package logic

import (
	"context"
	"fmt"

	"im-platform/app/user/rpc/internal/svc"
	"im-platform/app/user/rpc/user"

	"github.com/zeromicro/go-zero/core/logx"
)

type CheckFriendLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewCheckFriendLogic(ctx context.Context, svcCtx *svc.ServiceContext) *CheckFriendLogic {
	return &CheckFriendLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

// 关系校验（message rpc 单聊前置校验用）
func (l *CheckFriendLogic) CheckFriend(in *user.CheckFriendReq) (*user.CheckFriendResp, error) {
	if in.UserId <= 0 || in.FriendId <= 0 || in.UserId == in.FriendId {
		return nil, fmt.Errorf("invalid check friend req: user=%d friend=%d", in.UserId, in.FriendId)
	}
	isFriend, err := l.svcCtx.FriendsModel.ExistsFriend(l.ctx, in.UserId, in.FriendId)
	if err != nil {
		return nil, err
	}
	return &user.CheckFriendResp{
		IsFriend: isFriend,
	}, nil
}
