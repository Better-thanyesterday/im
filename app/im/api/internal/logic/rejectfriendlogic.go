// Code scaffolded by goctl. Safe to edit.
// goctl 1.10.1

package logic

import (
	"context"
	"fmt"

	"im-platform/app/im/api/internal/svc"
	"im-platform/app/im/api/internal/types"
	userclient "im-platform/app/user/rpc/userclient"
	"im-platform/common/middleware"

	"github.com/zeromicro/go-zero/core/logx"
)

type RejectFriendLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewRejectFriendLogic(ctx context.Context, svcCtx *svc.ServiceContext) *RejectFriendLogic {
	return &RejectFriendLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

// RejectFriend 拒绝好友申请:处理人身份以 token 为准(归属校验在 user rpc)
func (l *RejectFriendLogic) RejectFriend(req *types.RejectFriendReq) (*types.SuccessResp, error) {
	uid, ok := middleware.GetUserID(l.ctx)
	if !ok || uid <= 0 {
		return nil, fmt.Errorf("unauthorized")
	}
	resp, err := l.svcCtx.User.RejectFriend(l.ctx, &userclient.RejectFriendReq{
		ApplyId:    req.ApplyId,
		OperatorId: uid,
	})
	if err != nil {
		return nil, err
	}
	return &types.SuccessResp{Success: resp.Success}, nil
}
