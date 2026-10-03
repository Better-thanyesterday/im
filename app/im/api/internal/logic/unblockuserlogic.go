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

type UnblockUserLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewUnblockUserLogic(ctx context.Context, svcCtx *svc.ServiceContext) *UnblockUserLogic {
	return &UnblockUserLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

// UnblockUser 取消拉黑:操作者身份以 token 为准
func (l *UnblockUserLogic) UnblockUser(req *types.BlockUserReq) (*types.SuccessResp, error) {
	uid, ok := middleware.GetUserID(l.ctx)
	if !ok || uid <= 0 {
		return nil, fmt.Errorf("unauthorized")
	}
	resp, err := l.svcCtx.User.UnblockUser(l.ctx, &user.UnblockUserReq{
		TargetUserId: req.TargetUserId,
		UserId:       uid,
	})
	if err != nil {
		return nil, err
	}
	return &types.SuccessResp{Success: resp.Success}, nil
}
