package handler

import (
	"fmt"
	"im-platform/app/gateway/rpc/internal/conn"
	"im-platform/app/gateway/rpc/internal/logic"
	"im-platform/app/gateway/rpc/internal/svc"
	"im-platform/common/middleware"
	"net/http"
	"net/url"

	"github.com/gorilla/websocket"
	"github.com/zeromicro/go-zero/core/logx"
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	// 非浏览器客户端(原生 App)不携带 Origin,放行;
	// 带 Origin 的请求必须是同源,防止浏览器侧跨站 WS 劫持
	CheckOrigin: func(r *http.Request) bool {
		origin := r.Header.Get("Origin")
		if origin == "" {
			return true
		}
		u, err := url.Parse(origin)
		if err != nil {
			return false
		}
		return u.Host == r.Host
	},
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
		//回调下线和刷新在线状态
		c := conn.NewConn(userId, deviceType, connect, func(c *conn.Conn) {
			onConnClosed(svcCtx, c)
		},func() {
			// 心跳只续期本设备的活性 key;不能 Expire 整个在线 Hash,
			// 否则 A 设备崩溃后其 field 残留,B 的心跳一直给死地址续命
			liveKey := fmt.Sprintf("im:online:%d:%d", userId, deviceType)
			svcCtx.Redis.Expire(liveKey,90)
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
	// 1. 从本地连接管理器移除(只做内存操作,Redis 调用放锁外,
	//    避免 Redis 抖动时 bucket 写锁被拖住、同桶全部连接的操作被冻结)
	bucket := svcCtx.ConnManager.BucketOf(c.UserId())
	k := conn.ConnKey(c.UserId(),c.DeviceType())
	bucket.Mu.Lock()
	// 注意：只有当前连接还在 map 里才删（防止新连接把旧连接覆盖了，旧连接的 onClose 误删新连接）
	cur, ok := bucket.Conns[k]
	self := ok && cur == c
	if self {
		delete(bucket.Conns, k)
	}
	bucket.Mu.Unlock()
	// 2. 清 Redis 在线状态(锁外网络调用):
	//    只有被清理的是"自己"才清——被新连接顶替时,在线态归新连接所有,不能误删
	if self {
		key := fmt.Sprintf("im:online:%d", c.UserId())
		if _, err := svcCtx.Redis.Hdel(key, fmt.Sprintf("%d", c.DeviceType())); err != nil {
			logx.Errorf("redis hdel failed | user=%d device=%d err=%v", c.UserId(), c.DeviceType(), err)
		}
		liveKey := fmt.Sprintf("im:online:%d:%d", c.UserId(), c.DeviceType())
		if _, err := svcCtx.Redis.Del(liveKey); err != nil {
			logx.Errorf("redis del live key failed | key=%s err=%v", liveKey, err)
		}
		// 原 im:offline:{uid} 的写入已删除:全仓库无读者,纯死数据
	}
	logx.Infof("conn closed & online cleared | user=%d device=%d self=%v", c.UserId(), c.DeviceType(), self)
}
