package logic

import (
	"context"

	"im-platform/app/user/rpc/internal/svc"
	"im-platform/app/user/rpc/user"
	"im-platform/common/utils"

	"github.com/zeromicro/go-zero/core/logx"
)

type ResetPasswordLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewResetPasswordLogic(ctx context.Context, svcCtx *svc.ServiceContext) *ResetPasswordLogic {
	return &ResetPasswordLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *ResetPasswordLogic) ResetPassword(in *user.ResetPasswordReq) (*user.ResetPasswordResp, error) {
	// todo: add your logic here and delete this line
	u, err := l.svcCtx.UsersModel.FindOneByPhone(l.ctx, in.Phone)
	if err != nil {
		return &user.ResetPasswordResp{
			Success: false,
		}, err
	}
	if !utils.VerifyPassword(in.OldPassword, u.PasswordHash) {
		logx.Errorf("oldpassword is error")
		return &user.ResetPasswordResp{
			Success: false,
		}, err
	}
	PasswordHash, err := utils.HashPassword(in.NewPassword)
	if err != nil {
		logx.Errorf("hash password error: %v", err)
		return &user.ResetPasswordResp{
			Success: false,
		}, err
	}
	// 定向更新密码列,避免全列覆盖把 email/phone/nickname 等清零
	err = l.svcCtx.UsersModel.UpdatePassword(l.ctx, u.Id, PasswordHash)
	if err != nil {
		logx.Errorf("update password is error: %v", err)
		return &user.ResetPasswordResp{
			Success: false,
		}, err
	}
	return &user.ResetPasswordResp{
		Success: true,
	}, nil
}
