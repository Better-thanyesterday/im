package server

import (
	"context"

	"im-platform/app/gateway/api/conn"
	"im-platform/app/gateway/api/internal/svc"
	"im-platform/app/gateway/rpc/gateway"
	"github.com/zeromicro/go-zero/core/logx"
)

type GatewayServer struct {
	gateway.UnimplementedGatewayServer // ← 加这一行
	svcCtx                             *svc.ServiceContext
}

func NewGatewayServer(svcCtx *svc.ServiceContext) *GatewayServer {
	return &GatewayServer{svcCtx: svcCtx}
}

func (s *GatewayServer) PushToConn(ctx context.Context, req *gateway.PushToConnReq) (*gateway.PushToConnResp, error) {
	c, _ := s.svcCtx.ConnManager.Get(req.UserId, req.DeviceType)
	if c == nil {
		logx.Infof("PushToConn conn not found | user=%d device=%d", req.UserId, req.DeviceType)
		return &gateway.PushToConnResp{
			Success:  false,
			ErrorMsg: "conn not found",
		}, nil
	}
	
	switch res := c.Send(req.Payload); res {
	case conn.SendOK:
		return &gateway.PushToConnResp{Success: true}, nil
	case conn.SendClosed:
		logx.Infof("PushToConn conn closed | user=%d device=%d", req.UserId, req.DeviceType)
	case conn.SendQueueFull:
		logx.Infof("PushToConn queue full | user=%d device=%d", req.UserId, req.DeviceType)
		return &gateway.PushToConnResp{
			Success:  false,
			ErrorMsg: "queue full",
		}, nil
		default:
			return &gateway.PushToConnResp{
			Success:  false,
			ErrorMsg: "unknown",
		}, nil
	}
	return &gateway.PushToConnResp{Success: true}, nil
}
