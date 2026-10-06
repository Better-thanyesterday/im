package conn

import "sync"

type bucket struct {
	Mu    sync.RWMutex
	Conns map[string]*Conn // key: "userID:deviceType"
}

func newBucket() *bucket {
	return &bucket{Conns: make(map[string]*Conn)}
}
