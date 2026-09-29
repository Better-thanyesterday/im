package logic

import (
	"context"
	"errors"
	"im-platform/app/user/rpc/internal/svc"
	"im-platform/app/user/rpc/models"
	"im-platform/app/user/rpc/user"
	"im-platform/common/utils"

	"github.com/zeromicro/go-zero/core/logx"
)

var ErrInvalidCredentials = errors.New("账号或密码错误")

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
	u,err:=l.svcCtx.UsersModel.FindOneByPhone(l.ctx,in.Phone)
	if err!=nil {
		if errors.Is(err, models.ErrNotFound) {
			// 用户不存在与密码错误统一报错,避免用户枚举
			l.Logger.Errorf("login failed: user not found, phone=%s", in.Phone)
			return nil, ErrInvalidCredentials
		}
		return nil, err
	}
	if !utils.VerifyPassword(in.Password,u.PasswordHash) {
		l.Logger.Errorf("login failed: wrong password, phone=%s, userid=%d", in.Phone, u.Id)
		return nil, ErrInvalidCredentials
	}
	_,err =l.svcCtx.DevicesModel.Insert(l.ctx,&models.Devices{
		Userid: u.Id,
		Devicetype: int64(in.Devicetype),
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
