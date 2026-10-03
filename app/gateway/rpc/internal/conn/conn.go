package conn

import (
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/zeromicro/go-zero/core/logx"
)

type SendResult int

const (
	SendOK        SendResult = 1 + iota // 已入队，等 WritePump 下发
	SendClosed                          // 连接已关闭，走离线
	SendQueueFull                       // 队列满，消费跟不上
)

// Conn 封装一条 WebSocket 连接，线程安全
type Conn struct {
	userID      int64
	deviceType  int32
	ws          *websocket.Conn
	send        chan []byte // 下行消息队列，由 WritePump 独占消费 ，原因是WriteMessage不允许g并发
	accept      chan []byte
	remoteAddr  string
	connectedAt time.Time
	closeOnce   sync.Once
	closed      chan struct{}
	onClose     func(c *Conn)
	onRenew     func()
}

func NewConn(userID int64, deviceType int32, ws *websocket.Conn, onclose func(c *Conn), onrenew func()) *Conn {
	return &Conn{
		userID:      userID,
		deviceType:  deviceType,
		ws:          ws,
		accept:      make(chan []byte, 256),
		send:        make(chan []byte, 256),
		remoteAddr:  ws.RemoteAddr().String(),
		connectedAt: time.Now(),
		closed:      make(chan struct{}),
		onClose:     onclose,
		onRenew:     onrenew,
	}
}

func (c *Conn) Key() string            { return ConnKey(c.userID, c.deviceType) }
func (c *Conn) UserId() int64          { return c.userID }
func (c *Conn) DeviceType() int32      { return c.deviceType }
func (c *Conn) RemoteAddr() string     { return c.remoteAddr }
func (c *Conn) ConnectedAt() time.Time { return c.connectedAt }

func (c *Conn) Close() {
	c.closeOnce.Do(func() {
		close(c.closed)
		c.ws.Close()
		if c.onClose != nil {
			c.onClose(c)
		}
	})
}

// Send 非阻塞投递；队列满返回 SendQueueFull,连接已关闭返回 SendClosed。
//
// 语义边界(必须清楚):
// SendOK ≠ 已送达。"先查 closed 再投 send"与 Close 之间存在天然竞态——
// 两步之间连接关闭的话,消息进了没人消费的队列,Send 仍返回 SendOK,消息静默丢失。
// 这在 IM 下行是可接受的(at-least-once,靠离线信箱兜底),
// 但 push 侧绝不能把 SendOK 当作"客户端已收到"的依据;
// PushToConn 返回 Success 只代表"消息进入了存活连接的发送队列"。
func (c *Conn) Send(data []byte) SendResult {
	select {
	case <-c.closed:
		return SendClosed
	default:
	}
	select {
	case c.send <- data:
		return SendOK
	default:
		return SendQueueFull
	}
}

// ReadPump 读泵:onReconnect 是可选的建连后异步钩子(当前无人使用——
// 离线补拉已收敛为客户端主动发 FrameSyncRequest,见 wsconnectlogic.Register 注释)
func (c *Conn) ReadPump(onReconnect func(c *Conn)) {
	defer c.Close()
	c.ws.SetReadLimit(4096) // 单帧上限，防止内存被打爆
	if onReconnect != nil {
		go func() {
			defer func() {
				if r := recover(); r != nil {
					logx.Errorf("重连恢复 panic | user=%d err=%v", c.userID, r)
				}
			}()
			onReconnect(c)
		}()
	}
	c.ws.SetPongHandler(func(string) error {
		// 收到客户端 pong，续期
		c.ws.SetReadDeadline(time.Now().Add(90 * time.Second))
		return nil
	})
	c.ws.SetReadDeadline(time.Now().Add(90 * time.Second))
	for {
		_, data, err := c.ws.ReadMessage()
		if err != nil {
			logx.Errorf("读泵退出 | user=%d err=%v", c.userID, err)
			return
		}
		select {
		case c.accept <- data:
		case <-c.closed:
			return
		}
	}
}

// 写循环：唯一写入口，包含心跳 ping
func (c *Conn) WritePump() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-c.closed:
			return
		case data := <-c.send:
			c.ws.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if err := c.ws.WriteMessage(websocket.BinaryMessage, data); err != nil {
				c.Close()
				return
			}
		case <-ticker.C:
			c.ws.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if err := c.ws.WriteMessage(websocket.PingMessage, nil); err != nil {
				c.Close()
				return
			}
			// 同步调用:每次 ping 只做一次 Redis Expire,Redis 客户端自带超时兜底;
			// 原 go onRenew() 每 30s 无界起 goroutine,Redis 抖动时会堆积
			if c.onRenew != nil {
				c.onRenew()
			}
		}

	}
}

// OnMessage 起固定数量 worker 消费 accept 队列,调用 onMessage 处理每帧。
// 关闭语义:closed 触发后生产者(ReadPump)已停止投递,worker 先非阻塞
// drain 完在途帧再退出——Close 不丢已入队的数据;
// drain 与生产者最后一条入队存在微小竞态窗口,属 at-least-once 语义可接受范围
func (c *Conn) OnMessage(onMessage func(data []byte)) {
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-c.closed:
					// drain:消费完剩余在途帧再退出
					for {
						select {
						case d := <-c.accept:
							onMessage(d)
						default:
							return
						}
					}
				case d, ok := <-c.accept:
					if !ok {
						return
					}
					onMessage(d)
				}
			}
		}()
	}
	wg.Wait()
}
