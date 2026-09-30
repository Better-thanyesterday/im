package logic

import (
	"context"
	"fmt"

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
	if in.UserId <= 0 || in.DeviceId <= 0 {
		return nil, fmt.Errorf("invalid kick device req: user=%d device=%d", in.UserId, in.DeviceId)
	}
	// 校验设备归属:只能踢自己的设备
	dev, err := l.svcCtx.DevicesModel.FindOne(l.ctx, in.DeviceId)
	if err != nil {
		l.Logger.Errorf("kick device: find device %d err: %v", in.DeviceId, err)
		return &user.KickDeviceResp{
			Success: false,
		}, nil
	}
	if dev.Userid != in.UserId {
		l.Logger.Errorf("kick device forbidden: device=%d owner=%d caller=%d", in.DeviceId, dev.Userid, in.UserId)
		return &user.KickDeviceResp{
			Success: false,
		}, nil
	}
	// 踢出该设备的在线 token(拉黑 + 清登录态),失败不阻断删除
	if dev.Deviceid != "" {
		if err := l.svcCtx.TokenManager.RevokeDevice(l.ctx, dev.Userid, dev.Deviceid); err != nil {
			l.Logger.Errorf("revoke device token failed: user=%d device=%s err=%v", dev.Userid, dev.Deviceid, err)
		}
	}
	if err := l.svcCtx.DevicesModel.Delete(l.ctx, in.DeviceId); err != nil {
		l.Logger.Errorf("delete device %d err: %v", in.DeviceId, err)
		return &user.KickDeviceResp{
			Success: false,
		}, nil
	}
	return &user.KickDeviceResp{
		Success: true,
	}, nil
}
