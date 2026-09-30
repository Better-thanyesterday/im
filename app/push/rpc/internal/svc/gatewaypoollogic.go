package svc

import (
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const gatewayIdleTimeout = 10 * time.Minute // 空闲连接回收阈值

type poolEntry struct {
	conn     *grpc.ClientConn
	lastUsed int64 // unix 秒
}

// GatewayPool 按 gateway 地址维护 gRPC 连接:
// - 空闲超过阈值的连接由后台 janitor 回收(原实现永不回收,gateway 下线后残留死连接)
// - 推送失败时调用方应 Evict,下次 Get 重新建连
// - 明文 insecure 仅限内网部署,跨网段需换成 TLS 凭据
type GatewayPool struct {
	mu    sync.Mutex
	conns map[string]*poolEntry
}

func NewGatewayPool() *GatewayPool {
	p := &GatewayPool{conns: make(map[string]*poolEntry)}
	go p.janitor()
	return p
}

func (p *GatewayPool) Get(addr string) (*grpc.ClientConn, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if e, ok := p.conns[addr]; ok {
		e.lastUsed = time.Now().Unix()
		return e.conn, nil
	}
	// grpc.Dial 已废弃:NewClient 非阻塞创建,连通性靠调用期错误暴露
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	p.conns[addr] = &poolEntry{conn: conn, lastUsed: time.Now().Unix()}
	return conn, nil
}

// Evict 连接失效时逐出并关闭,下次 Get 重新建连
func (p *GatewayPool) Evict(addr string) {
	p.mu.Lock()
	e, ok := p.conns[addr]
	if ok {
		delete(p.conns, addr)
	}
	p.mu.Unlock()
	if ok {
		_ = e.conn.Close()
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
			if now-e.lastUsed > int64(gatewayIdleTimeout.Seconds()) {
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
