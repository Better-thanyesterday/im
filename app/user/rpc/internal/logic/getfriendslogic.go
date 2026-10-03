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
			CreatedAt:     r.CreatedAt,
		})
	}
	// Total 用同过滤条件的 count 查询,不再返回当前页行数
	total, err := l.svcCtx.FriendsModel.CountFriends(l.ctx, in.UserId, in.Keyword)
	if err != nil {
		return nil, err
	}
	return &user.GetFriendsResp{
		Total:   int32(total),
		Friends: friends,
	}, nil
}
