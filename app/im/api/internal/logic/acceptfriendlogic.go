// Code scaffolded by goctl. Safe to edit.
// goctl 1.10.1

package logic

import (
	"context"

	"im-platform/app/im/api/internal/svc"
	"im-platform/app/im/api/internal/types"
	"im-platform/app/user/rpc/user"

	"github.com/zeromicro/go-zero/core/logx"
)

type AcceptFriendLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewAcceptFriendLogic(ctx context.Context, svcCtx *svc.ServiceContext) *AcceptFriendLogic {
	return &AcceptFriendLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

// AcceptFriend 接受好友申请:申请人/处理者归属校验由 user rpc 完成
func (l *AcceptFriendLogic) AcceptFriend(req *types.AcceptFriendReq) (*types.SuccessResp, error) {
	resp, err := l.svcCtx.User.AcceptFriend(l.ctx, &user.AcceptFriendReq{
		ApplyId: req.ApplyId,
		Remark:  req.Remark,
	})
	if err != nil {
		return nil, err
	}
	return &types.SuccessResp{Success: resp.Success}, nil
}
