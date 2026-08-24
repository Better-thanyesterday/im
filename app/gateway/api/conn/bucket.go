package conn

import "sync"

type bucket struct {
	mu    sync.RWMutex
	conns map[string]*Conn // key: "userId:deviceType"
}

func newBucket() *bucket {
	return &bucket{conns: make(map[string]*Conn)}
}