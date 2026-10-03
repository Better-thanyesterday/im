// Code scaffolded by goctl. Safe to edit.
// goctl 1.10.1

package logic

import (
	"context"
	"fmt"

	"im-platform/app/im/api/internal/svc"
	"im-platform/app/im/api/internal/types"
	"im-platform/app/user/rpc/user"
	"im-platform/common/middleware"

	"github.com/zeromicro/go-zero/core/logx"
)

type FriendsLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewFriendsLogic(ctx context.Context, svcCtx *svc.ServiceContext) *FriendsLogic {
	return &FriendsLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

// GetFriends 查本人好友列表(支持分组过滤与昵称/备注模糊搜索)
func (l *FriendsLogic) GetFriends(req *types.GetFriendsReq) (*types.GetFriendsResp, error) {
	uid, ok := middleware.GetUserID(l.ctx)
	if !ok || uid <= 0 {
		return nil, fmt.Errorf("unauthorized")
	}
	resp, err := l.svcCtx.User.GetFriends(l.ctx, &user.GetFriendsReq{
		UserId:        uid,
		FriendGroupId: req.FriendGroupId,
		Keyword:       req.Keyword,
		Page:          req.Page,
		PageSize:      req.PageSize,
	})
	if err != nil {
		return nil, err
	}
	friends := make([]types.Friend, 0, len(resp.Friends))
	for _, f := range resp.Friends {
		friends = append(friends, types.Friend{
			UserId:        f.UserId,
			Nickname:      f.Nickname,
			Avatar:        f.Avatar,
			Remark:        f.Remark,
			FriendGroupId: f.FriendGroupId,
			CreatedAt:     f.CreatedAt,
		})
	}
	return &types.GetFriendsResp{Total: resp.Total, Friends: friends}, nil
}
