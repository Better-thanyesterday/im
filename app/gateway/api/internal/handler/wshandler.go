package handler

import (
	"im-platform/app/gateway/api/conn"
	"im-platform/app/gateway/api/internal/logic"
	"im-platform/app/gateway/api/internal/svc"
	"im-platform/common/middleware"
	"net/http"

	"github.com/gorilla/websocket"
	"github.com/zeromicro/go-zero/core/logx"
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin:     func(r *http.Request) bool { return true },
}

func WsHandler(svcCtx *svc.ServiceContext) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		//auth 中间件，可以从 r.Context() 里取用户信息；
		connect, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			logx.Errorf("websocket upgrade failed: %v", err)
			return
		}
		// 封装连接并注册到连接管理器
		// 升级后不要再使用 r.Context()（handler 返回即被取消），
		userId, ok := r.Context().Value(middleware.CtxUserID).(int64)
		deviceType, ok2 := r.Context().Value(middleware.CtxDeviceType).(int32)
		if !ok || !ok2 {
			logx.Errorf("Context information lose")
			connect.Close()
			return
		}
		c := conn.NewConn(userId, deviceType, connect)
		svcCtx.ConnManager.Add(c)
		go c.WritePump()
		c.ReadPump(func(data []byte) {
			// TODO: 上行消息分发（聊天消息、心跳上报等），先记录日志
			logx.Infof("ws message | user=%d device=%d len=%d", userId, deviceType, len(data))
			logic.HandleFrame(svcCtx,c,data)
		})
		defer svcCtx.ConnManager.RemoveConn(c)
	}
}
