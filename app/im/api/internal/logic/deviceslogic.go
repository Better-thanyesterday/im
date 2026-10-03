// Code scaffolded by goctl. Safe to edit.
// goctl 1.10.1

package logic

import (
	"context"
	"fmt"

	"im-platform/app/im/api/internal/svc"
	"im-platform/app/im/api/internal/types"
	"im-platform/app/user/rpc/user"
	"im-platform/common/middleware"

	"github.com/zeromicro/go-zero/core/logx"
)

type DevicesLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewDevicesLogic(ctx context.Context, svcCtx *svc.ServiceContext) *DevicesLogic {
	return &DevicesLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

// GetDevices 查本人登录设备列表
func (l *DevicesLogic) GetDevices() (*types.DevicesResp, error) {
	uid, ok := middleware.GetUserID(l.ctx)
	if !ok || uid <= 0 {
		return nil, fmt.Errorf("unauthorized")
	}
	resp, err := l.svcCtx.User.GetDevices(l.ctx, &user.GetDevicesReq{UserId: uid})
	if err != nil {
		return nil, err
	}
	devices := make([]types.Device, 0, len(resp.Devices))
	for _, d := range resp.Devices {
		devices = append(devices, types.Device{
			Id:           d.Id,
			DeviceType:   d.DeviceType,
			DeviceName:   d.DeviceName,
			DeviceId:     d.DeviceId,
			Location:     d.Location,
			LoginAt:      d.LoginAt,
			LastActiveAt: d.LastActiveAt,
			Status:       d.Status,
		})
	}
	return &types.DevicesResp{Devices: devices}, nil
}
