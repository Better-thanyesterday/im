package server

import (
	"context"

	"im-platform/app/gateway/api/conn"
	"im-platform/app/gateway/api/internal/svc"
	"im-platform/app/gateway/api/protocol"
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
	
	// 下行必须封帧(与上行 DecodeFrame 对称),否则客户端无法识别帧类型
	frame := protocol.EncodeFrame(protocol.FramePush, req.Payload)
	switch res := c.Send(frame); res {
	case conn.SendOK:
		return &gateway.PushToConnResp{Success: true}, nil
	case conn.SendClosed:
		// 连接已关闭:必须返回失败,让 push 服务把消息转入离线箱;
		// 原实现漏了 return,落到 Success:true,消息永久丢失
		logx.Infof("PushToConn conn closed | user=%d device=%d", req.UserId, req.DeviceType)
		return &gateway.PushToConnResp{
			Success:  false,
			ErrorMsg: "conn closed",
		}, nil
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
}

// BatchPushToConn 群聊写扩散的批量推送入口:一次 RPC 推多个用户,
// 在本网关进程内逐用户逐设备下发,替代消息服务 O(N) 次跨服务 Deliver
func (s *GatewayServer) BatchPushToConn(ctx context.Context, req *gateway.BatchPushToConnReq) (*gateway.BatchPushToConnResp, error) {
	// 下行统一封帧,与 PushToConn 对称
	frame := protocol.EncodeFrame(protocol.FramePush, req.Payload)
	var (
		successCount int32
		failed       []int64
	)
	for _, uid := range req.UserIds {
		conns:= s.svcCtx.ConnManager.GetByUser(uid)
		if len(conns) == 0 {
			failed = append(failed, uid)
			continue
		}
		delivered := false
		for _, c := range conns {
			if c.Send(frame) == conn.SendOK {
				successCount++
				delivered = true
			}
		}
		if !delivered {
			failed = append(failed, uid)
		}
	}
	return &gateway.BatchPushToConnResp{
		SuccessCount:  successCount,
		FailedUserIds: failed,
	}, nil
}
