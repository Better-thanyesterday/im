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

type KickDeviceLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewKickDeviceLogic(ctx context.Context, svcCtx *svc.ServiceContext) *KickDeviceLogic {
	return &KickDeviceLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

// KickDevice 踢下线本人设备:归属校验(user_id 必须与设备归属一致)由 user rpc 做
func (l *KickDeviceLogic) KickDevice(req *types.KickDeviceReq) (*types.SuccessResp, error) {
	uid, ok := middleware.GetUserID(l.ctx)
	if !ok || uid <= 0 {
		return nil, fmt.Errorf("unauthorized")
	}
	resp, err := l.svcCtx.User.KickDevice(l.ctx, &user.KickDeviceReq{
		DeviceId: req.DeviceId,
		UserId:   uid,
	})
	if err != nil {
		return nil, err
	}
	return &types.SuccessResp{Success: resp.Success}, nil
}
