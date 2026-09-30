package logic

import (
	"context"

	"im-platform/app/user/rpc/internal/svc"
	"im-platform/app/user/rpc/user"

	"github.com/zeromicro/go-zero/core/logx"
)

type LogoutLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewLogoutLogic(ctx context.Context, svcCtx *svc.ServiceContext) *LogoutLogic {
	return &LogoutLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

// Logout 注销当前 token:删除 token 登录态并从用户 token 表移除。
// 不再写 users.status(登出不是账号状态变更,且原实现 where id=0 静默 no-op 假成功)
func (l *LogoutLogic) Logout(in *user.LogoutReq) (*user.LogoutResp, error) {
	// todo: add your logic here and delete this line
	if in.Token == "" {
		return &user.LogoutResp{
			Success: false,
		}, nil
	}
	if err := l.svcCtx.TokenManager.Revoke(l.ctx, in.Token); err != nil {
		l.Logger.Errorf("logout revoke token failed: %v", err)
		return &user.LogoutResp{
			Success: false,
		}, nil
	}
	return &user.LogoutResp{
		Success: true,
	}, nil
}
