package conn

import (

	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/zeromicro/go-zero/core/logx"
)

type SendResult int

const (
	SendOK        SendResult = iota // 已入队，等 WritePump 下发
	SendClosed                      // 连接已关闭，走离线
	SendQueueFull                   // 队列满，消费跟不上
)

// Conn 封装一条 WebSocket 连接，线程安全
type Conn struct {
	userId     int64
	deviceType int32
	ws         *websocket.Conn
	send       chan []byte // 下行消息队列，由 WritePump 独占消费

	remoteAddr  string
	connectedAt time.Time

	closeOnce sync.Once
	closed    chan struct{}
	onClose   func(c *Conn)
	onRenew   func()
}

func NewConn(userId int64, deviceType int32, ws *websocket.Conn, onclose func(c *Conn), onrenew func()) *Conn {
	return &Conn{
		userId:      userId,
		deviceType:  deviceType,
		ws:          ws,
		send:        make(chan []byte, 256),
		remoteAddr:  ws.RemoteAddr().String(),
		connectedAt: time.Now(),
		closed:      make(chan struct{}),
		onClose:     onclose,
		onRenew:     onrenew,
	}
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
func (c *Conn) Key() string            { return ConnKey(c.userId, c.deviceType) }
func (c *Conn) UserId() int64          { return c.userId }
func (c *Conn) DeviceType() int32      { return c.deviceType }
func (c *Conn) RemoteAddr() string     { return c.remoteAddr }
func (c *Conn) ConnectedAt() time.Time { return c.connectedAt }

// Close 幂等关闭，任意 goroutine 可安全调用
func (c *Conn) Close() {
	c.closeOnce.Do(func() {
		close(c.closed)
		c.ws.Close()
		if c.onClose != nil {
			c.onClose(c)
		}
	})
}

func (c *Conn) ReadPump(onMessage func(data []byte), onReconnect func(c *Conn)) {
	defer c.Close()
	c.ws.SetReadLimit(4096) // 单帧上限，防止内存被打爆
	// 6. 触发重连恢复：拉离线消息
	var once sync.Once
	
	once.Do(func() {
		if onReconnect != nil {
			go onReconnect(c) // 异步执行，不阻塞消息读取
		}
	})
	c.ws.SetPongHandler(func(string) error {
		// 收到客户端 pong，续期
		c.ws.SetReadDeadline(time.Now().Add(90 * time.Second))
		return nil
	})
	for {
		c.ws.SetReadDeadline(time.Now().Add(90 * time.Second))
		_, data, err := c.ws.ReadMessage()
		if err != nil {
			logx.Errorf("read pump exit | user=%d err=%v", c.userId, err) // conn 包需 import logx
			return
		}

		onMessage(data)
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
			// 只要绑定了函数，就异步调用
			if c.onRenew != nil {
				go c.onRenew()
			}
		}

	}
}
