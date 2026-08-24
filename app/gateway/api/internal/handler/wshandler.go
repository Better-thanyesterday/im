package handler

import (
	"context"
	"fmt"
	"im-platform/app/gateway/api/conn"
	"im-platform/app/gateway/api/internal/logic"
	"im-platform/app/gateway/api/internal/svc"
	"im-platform/common/middleware"
	"net/http"
	"strconv"

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
		//回调下线
		c := conn.NewConn(userId, deviceType, connect, func(c *conn.Conn) {
			onConnClosed(svcCtx, c)
		})
		l := logic.NewWsConnectLogic(r.Context(), svcCtx)
		l.Register(c)
		defer func() {
			if _, ok := svcCtx.ConnManager.Get(userId, deviceType); ok {
				svcCtx.ConnManager.RemoveConn(c)
			}
		}()
	}
}

// onConnClosed：连接断开后的清理
func onConnClosed(svcCtx *svc.ServiceContext, c *conn.Conn) {
	// 1. 从本地连接管理器移除
	bucket := svcCtx.ConnManager.BucketOf(c.UserId())
	bucket.Mu.Lock()
	defer bucket.Mu.Unlock()
	k := conn.ConnKey(c.UserId(),c.DeviceType())
	// 注意：只有当前连接还在 map 里才删（防止新连接把旧连接覆盖了，旧连接的 onClose 误删新连接）
	if cur, ok := bucket.Conns[k]; ok && cur == c { // 只有自己还在 map 里才删
		delete(bucket.Conns, k)
	}
	// 2. 清 Redis 在线状态
	key := fmt.Sprintf("im:online:%d", c.UserId())
	if _, err := svcCtx.Redis.Hdel(key, fmt.Sprintf("%d", c.DeviceType())); err != nil {
		logx.Errorf("redis hdel failed | user=%d device=%d err=%v", c.UserId(), c.DeviceType(), err)
	}
	//
	offlineKey := fmt.Sprintf("im:offline:%d", c.UserId())
	err := svcCtx.Redis.HsetCtx(context.Background(), offlineKey, strconv.FormatInt(int64(c.DeviceType()), 10), svcCtx.Config.Gateway.GrpcAddr)
	if err != nil {
		logx.Errorf("set offline failed")
	}
	svcCtx.Redis.ExpireCtx(context.Background(),offlineKey,900)
	// 3. 如果该用户所有设备都下线了，删除整个 key
	// if n, _ := g.redis.Hlen(key); n == 0 {
	// 	g.redis.Del(key)
	// }

	logx.Infof("conn closed & online cleared | user=%d device=%d", c.UserId(), c.DeviceType())
}
