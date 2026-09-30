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

// Send 非阻塞投递；队列满或连接已关闭返回 false
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

func (c *Conn) ReadPump(onReconnect func(c *Conn)) {
	defer c.Close()
	c.ws.SetReadLimit(4096) // 单帧上限，防止内存被打爆
	// 6. 触发重连恢复：拉离线消息
	if onReconnect != nil {
		go func() {
			defer func() {
				if r := recover(); r != nil {
					logx.Errorf("onReconnect panic | user=%d err=%v", c.userID, r)
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
			logx.Errorf("read pump exit | user=%d err=%v", c.userID, err)
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
				return
			}
		case <-ticker.C:
			c.ws.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if err := c.ws.WriteMessage(websocket.PingMessage, nil); err != nil {
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

// 默认4
func (c *Conn) OnMessage(onMessage func(data []byte)) {
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-c.closed:
					return
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
