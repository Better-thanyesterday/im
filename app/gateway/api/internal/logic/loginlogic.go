// Code scaffolded by goctl. Safe to edit.
// goctl 1.10.1

package logic

import (
	"context"
	"fmt"
	"im-platform/app/gateway/api/internal/svc"
	"im-platform/app/gateway/api/internal/types"
	"im-platform/app/user/rpc/user"
	"im-platform/common/utils"
	"strconv"
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
	token, err := l.svcCtx.TokenManager.Issue(l.ctx, utils.TokenInfo{
		UserID:     loginResp.Userid,
		DeviceID:   req.DeviceId,
		DeviceType: req.DeviceType,
	}, time.Hour*24)
	onlineKey := fmt.Sprintf("im:online:%d", loginResp.Userid)
	err =l.svcCtx.Redis.HsetCtx(l.ctx, onlineKey, strconv.FormatInt(int64(req.DeviceType), 10), l.svcCtx.Config.Gateway.GrpcAddr)
	if err != nil {
		logx.Errorf("set im:online:onlineKey failed")
	}
	l.svcCtx.Redis.ExpireCtx(l.ctx,onlineKey,900)	
	return &types.LoginResp{
		Token: token,
	}, nil
}
