package conn

import (
	"strconv"
)

type ConnManager struct {
	buckets []*bucket
}

func NewManager(bucketNum int) *ConnManager {
	if bucketNum <= 0 {
		bucketNum = 16
	}
	m := &ConnManager{buckets: make([]*bucket, bucketNum)}
	for i := range m.buckets {
		m.buckets[i] = newBucket()
	}
	return m
}

func ConnKey(userId int64, deviceType int32) string {
	return strconv.FormatInt(userId, 10) + ":" + strconv.FormatInt(int64(deviceType), 10)
}

func (m *ConnManager) bucketOf(userId int64) *bucket {
	idx := uint64(userId) % uint64((len(m.buckets)))
	return m.buckets[idx]
}

func (m *ConnManager) Add(c *Conn) {
	b := m.bucketOf(c.userId)
	defer b.mu.Unlock()
	b.mu.Lock()
	key := c.Key()
	if old := b.conns[key]; old != nil && old != c {
		old.Close()
	}
	b.conns[key] = c
}

func (m *ConnManager) Get(userId int64, deviceType int32) (*Conn, bool) {
	b := m.bucketOf(userId)
	b.mu.RLock()
	defer b.mu.RUnlock()
	c, ok := b.conns[ConnKey(userId, deviceType)]
	return c, ok
}

func (m *ConnManager) GetByUser(userId int64) []*Conn {
	b := m.bucketOf(userId)
	b.mu.RLock()
	defer b.mu.RUnlock()

	var res []*Conn
	for _, c := range b.conns {
		if c.userId == userId {
			res = append(res, c)
		}
	}
	return res
}

func (m *ConnManager) Count() int {
	total := 0
	for _, b := range m.buckets {
		b.mu.RLock()
		total += len(b.conns)
		b.mu.RUnlock()
	}
	return total
}

func (m *ConnManager) Remove(userId int64, deviceType int32) {
	b := m.bucketOf(userId)
	b.mu.Lock()
	key:=ConnKey(userId,deviceType)
	c,ok:=b.conns[key]
	if ok{
		delete(b.conns,key)
	}
	b.mu.Unlock()
	if ok {
		c.Close()
	}
}


func (m *ConnManager) RemoveConn(c*Conn) {
	b := m.bucketOf(c.userId)
	b.mu.Lock()
	key:=c.Key()
	cur,ok :=b.conns[key]
	if ok&&cur==c{
		delete(b.conns,key)
	}
	b.mu.Unlock()
	if ok&&cur==c{
		c.Close()
	}
}
// Range 遍历所有连接；fn 返回 false 提前终止。
// 注意：不要在 fn 里调用 manager 的加锁方法，否则死锁
func (m *ConnManager) Range(fn func(*Conn) bool) {
	for _, b := range m.buckets {
		b.mu.RLock()
		for _, c := range b.conns {
			if !fn(c) {
				b.mu.RUnlock()
				return
			}
		}
		b.mu.RUnlock()
	}
}

// CloseAll 关闭全部连接（服务优雅退出时调用）
func (m *ConnManager) CloseAll() {
	for _, b := range m.buckets {
		b.mu.Lock()
		conns := make([]*Conn, 0, len(b.conns))
		for _, c := range b.conns {
			conns = append(conns, c)
		}
		b.conns = make(map[string]*Conn)
		b.mu.Unlock()

		for _, c := range conns {
			c.Close() // 锁外关闭，避免持有锁时做 IO
		}
	}
}