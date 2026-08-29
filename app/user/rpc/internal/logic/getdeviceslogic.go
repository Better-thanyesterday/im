package logic

import (
	"context"

	"im-platform/app/user/rpc/internal/svc"
	"im-platform/app/user/rpc/user"

	"github.com/zeromicro/go-zero/core/logx"
)

type GetDevicesLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewGetDevicesLogic(ctx context.Context, svcCtx *svc.ServiceContext) *GetDevicesLogic {
	return &GetDevicesLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

// 设备管理
func (l *GetDevicesLogic) GetDevices(in *user.GetDevicesReq) (*user.GetDevicesResp, error) {
	// todo: add your logic here and delete this line
	res, err := l.svcCtx.DevicesModel.FindByUserId(l.ctx, in.UserId)
	if err != nil {
		return nil, err
	}
	var devices []*user.DeviceInfo
	for _, m := range res {
		devices = append(devices, &user.DeviceInfo{
			Id:         m.Id,
			DeviceType: int32(m.Devicetype),
			DeviceName: m.Devicename.String,
			DeviceId:   m.Deviceid,
			Location:   m.Location.String,
			LoginAt:    m.LoginAt.Time.Unix(),
			LogoutAt:   m.LogoutAt.Time.Unix(),
			LastActiveAt:m.LastActiveAt.Time.Unix(),
			Status: int32(m.Status),
		})
	}

	return &user.GetDevicesResp{
		Devices: devices,
	}, nil
}
