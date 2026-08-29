package logic

import (
	"context"

	"im-platform/app/user/rpc/internal/svc"
	"im-platform/app/user/rpc/user"

	"github.com/zeromicro/go-zero/core/logx"
)

type GetFriendsLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewGetFriendsLogic(ctx context.Context, svcCtx *svc.ServiceContext) *GetFriendsLogic {
	return &GetFriendsLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *GetFriendsLogic) GetFriends(in *user.GetFriendsReq) (*user.GetFriendsResp, error) {
	// todo: add your logic here and delete this line
	rows, err := l.svcCtx.FriendsModel.FindFriendsByUserId(
		l.ctx,
		in.UserId,
		in.Keyword,
		in.Page,
		in.PageSize,
	)
	if err != nil {
        return nil, err
    }

	friends := make([]*user.FriendInfo, 0, len(rows))
	for _, r := range rows {
		friends = append(friends, &user.FriendInfo{
			UserId:        r.UserId,
			Avatar:        r.Avatar,
			Remark:        r.Remark,
			FriendGroupId: r.FriendGroupId,
			Nickname:      r.Nickname,
			CreatedAt: r.CreatedAt,
		})
	}
	return &user.GetFriendsResp{
		Total: int32(len(rows)),
		Friends: friends,
	}, nil
}
