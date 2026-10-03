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

type ResetPasswordLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewResetPasswordLogic(ctx context.Context, svcCtx *svc.ServiceContext) *ResetPasswordLogic {
	return &ResetPasswordLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

// ResetPassword 重置密码:身份校验(旧密码或验证码)由 user rpc 完成
func (l *ResetPasswordLogic) ResetPassword(req *types.ResetPasswordReq) (*types.SuccessResp, error) {
	resp, err := l.svcCtx.User.ResetPassword(l.ctx, &user.ResetPasswordReq{
		Phone:       req.Phone,
		Email:       req.Email,
		OldPassword: req.OldPassword,
		NewPassword: req.NewPassword,
		VerifyCode:  req.VerifyCode,
	})
	if err != nil {
		return nil, err
	}
	return &types.SuccessResp{Success: resp.Success}, nil
}
