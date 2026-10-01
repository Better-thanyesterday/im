// Code scaffolded by goctl. Safe to edit.
// goctl 1.10.1

package logic

import (
	"context"
	"errors"
	"im-platform/app/api/api/internal/svc"
	"im-platform/app/api/api/internal/types"
	"im-platform/app/user/rpc/user"
	"im-platform/common/utils"
	"time"

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

func (l *LoginLogic) Login(req *types.LoginReq) (resp *types.LoginResp, err error) {
	// todo: add your logic here and delete this line
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
	if loginResp.Userid <= 0 {
		l.Logger.Errorf("login: rpc returned invalid userid=%d, phone=%s", loginResp.Userid, req.Phone)
		return nil, errors.New("账号或密码错误")
	}
	token, err := l.svcCtx.TokenManager.Issue(l.ctx, utils.TokenInfo{
		UserID:     loginResp.Userid,
		DeviceID:   req.DeviceId,
		DeviceType: req.DeviceType,
	}, time.Hour*24)
	// 不在这里写 im:online:HTTP 登录还没建 WS,写入的是"幽灵在线";
	// 在线状态由 WS 连接注册(wsconnectlogic.Register)统一写入并由心跳续期
	return &types.LoginResp{
		Token: token,
	}, nil
}
