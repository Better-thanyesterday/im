package logic

import (
	"context"

	"im-platform/app/user/rpc/internal/svc"
	"im-platform/app/user/rpc/models"
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
	PasswordHash, _ := utils.HashPassword(in.NewPassword)
	err = l.svcCtx.UsersModel.Update(l.ctx, &models.Users{
		PasswordHash: PasswordHash,
		Id:           u.Id,
	})
	if err != nil {
		logx.Errorf("update password is error")
		return &user.ResetPasswordResp{
			Success: false,
		}, err
	}
	return &user.ResetPasswordResp{
		Success: true,
	}, nil
}
