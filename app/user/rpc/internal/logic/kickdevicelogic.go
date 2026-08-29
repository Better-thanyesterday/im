package logic

import (
	"context"

	"im-platform/app/user/rpc/internal/svc"
	"im-platform/app/user/rpc/user"

	"github.com/zeromicro/go-zero/core/logx"
)

type KickDeviceLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewKickDeviceLogic(ctx context.Context, svcCtx *svc.ServiceContext) *KickDeviceLogic {
	return &KickDeviceLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *KickDeviceLogic) KickDevice(in *user.KickDeviceReq) (*user.KickDeviceResp, error) {
	// todo: add your logic here and delete this line
	err:=l.svcCtx.DevicesModel.Delete(l.ctx,in.DeviceId)
	if err != nil {
		return &user.KickDeviceResp{
			Success: false,
		}, nil
	}
	return &user.KickDeviceResp{
		Success: true,
	}, nil
}
