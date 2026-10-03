package logic

import (
	"context"
	"github.com/zeromicro/go-zero/core/logx"
	"im-platform/app/user/rpc/internal/svc"
	"im-platform/app/user/rpc/models"
	"im-platform/app/user/rpc/user"
	"im-platform/common/utils"
)

type RegisterLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewRegisterLogic(ctx context.Context, svcCtx *svc.ServiceContext) *RegisterLogic {
	return &RegisterLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *RegisterLogic) Register(in *user.RegisterRequest) (*user.RegisterResponse, error) {
	// todo: add your logic here and delete this line
	Id := l.svcCtx.Snowflake.NextID()
	PasswordHash, _ := utils.HashPassword(in.Password)
	acc, err := l.svcCtx.UsersModel.InsertWithoutAccount(l.ctx, &models.Users{
		Phone:        in.Phone,
		PasswordHash: PasswordHash,
		Email:        in.Email,
		Id:           Id,
	})
	if err != nil {
		return nil, err
	}
	return &user.RegisterResponse{
		Account: acc,
	}, nil

}
