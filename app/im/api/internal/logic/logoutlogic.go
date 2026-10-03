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

type LogoutLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewLogoutLogic(ctx context.Context, svcCtx *svc.ServiceContext) *LogoutLogic {
	return &LogoutLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

// Logout 注销当前 token:token 由鉴权中间件注入 ctx,不接受请求体
func (l *LogoutLogic) Logout() (*types.SuccessResp, error) {
	token, ok := middleware.GetToken(l.ctx)
	if !ok || token == "" {
		return nil, fmt.Errorf("unauthorized")
	}
	resp, err := l.svcCtx.User.Logout(l.ctx, &user.LogoutReq{Token: token})
	if err != nil {
		return nil, err
	}
	return &types.SuccessResp{Success: resp.Success}, nil
}
