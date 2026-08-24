package logic

import (
	"context"
	"fmt"
	"im-platform/app/user/rpc/internal/svc"
	"im-platform/app/user/rpc/models"
	"im-platform/app/user/rpc/user"
	"im-platform/common/utils"

	"github.com/zeromicro/go-zero/core/logx"
)

type LoginLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewLoginLogic(ctx context.Context, svcCtx *svc.ServiceContext) *LoginLogic {
	return &LoginLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *LoginLogic) Login(in *user.LoginRequest) (*user.LoginResponse, error) {
	// todo: add your logic here and delete this line

	u,err:=l.svcCtx.UsersModel.FindOneByPhone(l.ctx,in.Phone)
	if err!=nil {
		return &user.LoginResponse{} ,err
	}
	if !utils.VerifyPassword(in.Password,u.PasswordHash) {
		fmt.Println("password is error")
		return &user.LoginResponse{} ,err
	}
	_,err =l.svcCtx.DevicesModel.Insert(l.ctx,&models.Devices{
		Userid: u.Id,
		Devicetype: in.Devicetype,
		Deviceid: in.Deviceid,
		Status: 1,
	})
	if err != nil {
		return nil, err
	}
	
	return &user.LoginResponse{
		Userid: u.Id,
	}, nil
}
