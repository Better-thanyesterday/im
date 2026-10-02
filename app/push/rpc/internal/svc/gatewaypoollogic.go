package svc

import (
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const gatewayIdleTimeout = 10 * time.Minute // 空闲连接回收阈值

// evictFailThreshold 连续失败多少次才真正逐出连接(语义见 ReportFail)
const evictFailThreshold = 3

type poolEntry struct {
	conn *grpc.ClientConn
	// 原子量:命中路径更新 lastUsed 不需要写锁,janitor 读取也无需加锁
	lastUsed atomic.Int64 // unix 秒
	// 连续失败计数,任意一次成功清零;达 evictFailThreshold 逐出
	failStreak atomic.Int32
}

// GatewayPool 按 gateway 地址维护 gRPC 连接:
// - 空闲超过阈值的连接由后台 janitor 回收(原实现永不回收,gateway 下线后残留死连接)
// - 推送失败时调用方应 Evict,下次 Get 重新建连
// - 明文 insecure 仅限内网部署,跨网段需换成 TLS 凭据
type GatewayPool struct {
	// RWMutex + 双重检查:每条消息推每个设备都要过 Get,是推送热路径,
	// 原全局 Mutex 连缓存命中都串行化,高并发下所有推送 goroutine 排队过一把锁
	mu    sync.RWMutex
	conns map[string]*poolEntry
}

func NewGatewayPool() *GatewayPool {
	p := &GatewayPool{conns: make(map[string]*poolEntry)}
	go p.janitor()
	return p
}

func (p *GatewayPool) Get(addr string) (*grpc.ClientConn, error) {
	// 快路径:RLock 查命中,命中直接返回(几乎全部调用都走这里)
	p.mu.RLock()
	if e, ok := p.conns[addr]; ok {
		p.mu.RUnlock()
		e.lastUsed.Store(time.Now().Unix())
		return e.conn, nil
	}
	p.mu.RUnlock()

	// 慢路径:升级写锁建连;升级后必须复查,防止等锁期间别的 goroutine 已建好同地址连接
	p.mu.Lock()
	if e, ok := p.conns[addr]; ok {
		p.mu.Unlock()
		e.lastUsed.Store(time.Now().Unix())
		return e.conn, nil
	}
	// grpc.Dial 已废弃:NewClient 非阻塞创建,连通性靠调用期错误暴露
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		p.mu.Unlock()
		return nil, err
	}
	e := &poolEntry{conn: conn}
	e.lastUsed.Store(time.Now().Unix())
	p.conns[addr] = e
	p.mu.Unlock()
	return conn, nil
}

// ReportFail 推送失败上报:连续失败达 evictFailThreshold 才真正逐出重建。
// 不能单次失败就逐出——多个设备常共享同一 gatewayAddr,一条推送失败就关掉整条连接,
// 会连坐其他设备正在该连接上的在途请求,它们再失败又各自上报,抖动被放大;
// 任意一次成功即清零,真死的网关在阈值次内必然打满计数,自愈速度不受影响
func (p *GatewayPool) ReportFail(addr string) {
	p.mu.RLock()
	e, ok := p.conns[addr]
	p.mu.RUnlock()
	if !ok {
		return
	}
	if e.failStreak.Add(1) < evictFailThreshold {
		return
	}
	p.mu.Lock()
	// 可能已被并发上报或 janitor 先行逐出:只逐出仍指向同一 entry 的连接
	if cur, ok := p.conns[addr]; ok && cur == e {
		delete(p.conns, addr)
	} else {
		ok = false
	}
	p.mu.Unlock()
	if ok {
		_ = e.conn.Close()
	}
}

// MarkOK 推送成功,清零该连接的连续失败计数
func (p *GatewayPool) MarkOK(addr string) {
	p.mu.RLock()
	e, ok := p.conns[addr]
	p.mu.RUnlock()
	if ok {
		e.failStreak.Store(0)
	}
}

// janitor 周期回收空闲连接
func (p *GatewayPool) janitor() {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()
	for range ticker.C {
		now := time.Now().Unix()
		p.mu.Lock()
		var evicted []*grpc.ClientConn
		for addr, e := range p.conns {
			if now-e.lastUsed.Load() > int64(gatewayIdleTimeout.Seconds()) {
				evicted = append(evicted, e.conn)
				delete(p.conns, addr)
			}
		}
		p.mu.Unlock()
		for _, c := range evicted {
			_ = c.Close()
		}
	}
}

func (p *GatewayPool) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, e := range p.conns {
		_ = e.conn.Close()
	}
	p.conns = make(map[string]*poolEntry)
}
