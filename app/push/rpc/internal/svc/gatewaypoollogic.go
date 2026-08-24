package svc

import (
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"sync"
)

type GatewayPool struct {
	mu    sync.RWMutex
	conns map[string]*grpc.ClientConn
}

func NewGatewayPool() *GatewayPool {
	return &GatewayPool{conns: make(map[string]*grpc.ClientConn)}
}

func (p *GatewayPool) Get(addr string) (*grpc.ClientConn, error) {
	p.mu.RLock()
	conn, ok := p.conns[addr]
	p.mu.RUnlock()
	if ok {
		return conn, nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if conn, ok := p.conns[addr]; ok {
		return conn, nil
	}
	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	p.conns[addr] = conn
	return conn, nil
}

func (p *GatewayPool) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, c := range p.conns {
		c.Close()
	}
}
