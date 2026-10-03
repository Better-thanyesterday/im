package logic

import (
	"context"
	"errors"
	"fmt"
	"im-platform/app/user/rpc/internal/svc"
	"im-platform/app/user/rpc/models"
	"im-platform/app/user/rpc/user"
	"im-platform/common/utils"
	"strconv"

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
	// 防爆破:同一手机号 15 分钟窗口内失败超过阈值直接拒绝
	failKey := fmt.Sprintf("im:login:fail:%s", in.Phone)
	if cnt, _ := l.svcCtx.Redis.GetCtx(l.ctx, failKey); cnt != "" {
		if n, _ := strconv.ParseInt(cnt, 10, 64); n >= maxLoginFails {
			return nil, errors.New("失败次数过多,请15分钟后再试")
		}
	}

	u, err := l.svcCtx.UsersModel.FindOneByPhone(l.ctx, in.Phone)
	if err != nil {
		if errors.Is(err, models.ErrNotFound) {
			// 用户不存在与密码错误统一报错,避免用户枚举
			l.Logger.Errorf("login failed: user not found, phone=%s", in.Phone)
			l.recordLoginFail(failKey)
			return nil, ErrInvalidCredentials
		}
		return nil, err
	}
	if !utils.VerifyPassword(in.Password, u.PasswordHash) {
		l.Logger.Errorf("login failed: wrong password, phone=%s, userid=%d", in.Phone, u.Id)
		l.recordLoginFail(failKey)
		return nil, ErrInvalidCredentials
	}
	_, err = l.svcCtx.DevicesModel.Insert(l.ctx, &models.Devices{
		Userid:     u.Id,
		Devicetype: int64(in.Devicetype),
		Deviceid:   in.Deviceid,
		Status:     1,
	})
	if err != nil {
		return nil, err
	}

	return &user.LoginResponse{
		Userid: u.Id,
	}, nil
}

const maxLoginFails = 5 // 15 分钟窗口内最大失败次数

// recordLoginFail 累计登录失败次数,首次失败时设置 15 分钟过期窗口
func (l *LoginLogic) recordLoginFail(key string) {
	n, err := l.svcCtx.Redis.IncrCtx(l.ctx, key)
	if err != nil {
		l.Logger.Errorf("incr login fail count err: %v", err)
		return
	}
	if n == 1 {
		_ = l.svcCtx.Redis.ExpireCtx(l.ctx, key, 900)
	}
}
