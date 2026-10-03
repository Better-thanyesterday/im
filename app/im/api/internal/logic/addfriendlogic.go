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

type AddFriendLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewAddFriendLogic(ctx context.Context, svcCtx *svc.ServiceContext) *AddFriendLogic {
	return &AddFriendLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

// AddFriend 发好友申请:申请人身份以 token 为准
func (l *AddFriendLogic) AddFriend(req *types.AddFriendReq) (*types.AddFriendResp, error) {
	uid, ok := middleware.GetUserID(l.ctx)
	if !ok || uid <= 0 {
		return nil, fmt.Errorf("unauthorized")
	}
	resp, err := l.svcCtx.User.AddFriend(l.ctx, &user.AddFriendReq{
		UserId:       uid,
		TargetUserId: req.TargetUserId,
		ApplyReason:  req.ApplyReason,
	})
	if err != nil {
		return nil, err
	}
	return &types.AddFriendResp{ApplyId: resp.ApplyId}, nil
}
