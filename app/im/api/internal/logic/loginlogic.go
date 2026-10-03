// Code scaffolded by goctl. Safe to edit.
// goctl 1.10.1

package logic

import (
	"context"
	"errors"
	"time"

	"im-platform/app/im/api/internal/svc"
	"im-platform/app/im/api/internal/types"
	"im-platform/app/user/rpc/user"
	"im-platform/common/utils"

	"github.com/zeromicro/go-zero/core/logx"
)

type LoginLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewLoginLogic(ctx context.Context, svcCtx *svc.ServiceContext) *LoginLogic {
	return &LoginLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *LoginLogic) Login(req *types.LoginReq) (*types.LoginResp, error) {
	loginResp, err := l.svcCtx.User.Login(l.ctx, &user.LoginRequest{
		Phone:      req.Phone,
		Email:      req.Email,
		Password:   req.Password,
		Devicetype: req.DeviceType,
		Deviceid:   req.DeviceId,
	})
	if err != nil {
		return nil, err
	}
	// user rpc 对"用户不存在/密码错误"统一返回业务错误,这里只防退化:
	// userid<=0 的响应绝不能签发 token(否则等于无密码登录)
	if loginResp.Userid <= 0 {
		l.Logger.Errorf("login | msg=rpc returned invalid userid | user_id=%d phone=%s", loginResp.Userid, req.Phone)
		return nil, errors.New("账号或密码错误")
	}
	token, err := l.svcCtx.TokenManager.Issue(l.ctx, utils.TokenInfo{
		UserID:     loginResp.Userid,
		DeviceID:   req.DeviceId,
		DeviceType: req.DeviceType,
	}, time.Duration(l.svcCtx.Config.Token.DefaultTTLHours)*time.Hour)
	if err != nil {
		l.Logger.Errorf("issue token failed | user_id=%d err=%v", loginResp.Userid, err)
		return nil, err
	}
	// 不在这里写在线表:HTTP 登录还没建 WS,写的是"幽灵在线";
	// 在线状态由 gateway 的 WS 连接注册统一写入并由心跳续期
	return &types.LoginResp{
		Token: token,
	}, nil
}
