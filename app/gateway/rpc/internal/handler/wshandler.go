package handler

import (
	"context"
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
			logx.Errorf("ws 升级失败 | err=%v", err)
			return
		}
		// 封装连接并注册到连接管理器
		// 升级后不要再使用 r.Context()（handler 返回即被取消），
		userId, ok := r.Context().Value(middleware.CtxUserID).(int64)
		deviceType, ok2 := r.Context().Value(middleware.CtxDeviceType).(int32)
		if !ok || !ok2 {
			logx.Errorf("ws 握手鉴权上下文缺失 | ok=%v ok2=%v", ok, ok2)
			connect.Close()
			return
		}
		//回调下线和刷新在线状态
		c := conn.NewConn(userId, deviceType, connect, func(c *conn.Conn) {
			onConnClosed(svcCtx, c)
		}, func() {
			// 心跳只续期本设备的活性 key;不能 Expire 整个在线 Hash,
			// 否则 A 设备崩溃后其 field 残留,B 的心跳一直给死地址续命
			liveKey := fmt.Sprintf("im:online:%d:%d", userId, deviceType)
			svcCtx.Redis.Expire(liveKey, 90)
		})
		l := logic.NewWsConnectLogic(r.Context(), svcCtx)
		l.Register(r.Context(), c)
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
	k := conn.ConnKey(c.UserId(), c.DeviceType())
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
		onlineKey := fmt.Sprintf("im:online:%d:%d", c.UserId(), c.DeviceType())
		gwKey := fmt.Sprintf("im:online:gw:%d", c.UserId())
		script := `
        redis.call('DEL', KEYS[1])
        redis.call('HDEL', KEYS[2], ARGV[1])
        return 1
    	`
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		if _, err := svcCtx.Redis.EvalCtx(ctx, script,
			[]string{onlineKey, gwKey},
			c.DeviceType(), // ARGV[1] = deviceType
		); err != nil {
			logx.Errorf("下线清理 Redis 失败 | user=%d device=%d err=%v",
				c.UserId(), c.DeviceType(), err)
		}
	}
	logx.Infof("连接关闭,在线态已清理 | user=%d device=%d self=%v", c.UserId(), c.DeviceType(), self)
}
