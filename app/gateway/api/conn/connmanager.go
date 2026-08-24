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

func (m *ConnManager) BucketOf(userId int64) *bucket {
	idx := uint64(userId) % uint64((len(m.buckets)))
	return m.buckets[idx]
}

func (m *ConnManager) Add(c *Conn) {
	b := m.BucketOf(c.userId)
	defer b.Mu.Unlock()
	b.Mu.Lock()
	key := c.Key()
	if old := b.Conns[key]; old != nil && old != c {
		old.Close()
	}
	b.Conns[key] = c
}

func (m *ConnManager) Get(userId int64, deviceType int32) (*Conn, bool) {
	b := m.BucketOf(userId)
	b.Mu.RLock()
	defer b.Mu.RUnlock()
	c, ok := b.Conns[ConnKey(userId, deviceType)]
	return c, ok
}

func (m *ConnManager) GetByUser(userId int64) []*Conn {
	b := m.BucketOf(userId)
	b.Mu.RLock()
	defer b.Mu.RUnlock()

	var res []*Conn
	for _, c := range b.Conns {
		if c.userId == userId {
			res = append(res, c)
		}
	}
	return res
}

func (m *ConnManager) Count() int {
	total := 0
	for _, b := range m.buckets {
		b.Mu.RLock()
		total += len(b.Conns)
		b.Mu.RUnlock()
	}
	return total
}

func (m *ConnManager) Remove(userId int64, deviceType int32) {
	b := m.BucketOf(userId)
	b.Mu.Lock()
	key:=ConnKey(userId,deviceType)
	c,ok:=b.Conns[key]
	if ok{
		delete(b.Conns,key)
	}
	b.Mu.Unlock()
	if ok {
		c.Close()
	}
}


func (m *ConnManager) RemoveConn(c*Conn) {
	b := m.BucketOf(c.userId)
	b.Mu.Lock()
	key:=c.Key()
	cur,ok :=b.Conns[key]
	if ok&&cur==c{
		delete(b.Conns,key)
	}
	b.Mu.Unlock()
	if ok&&cur==c{
		c.Close()
	}
}
// Range 遍历所有连接；fn 返回 false 提前终止。
// 注意：不要在 fn 里调用 manager 的加锁方法，否则死锁
func (m *ConnManager) Range(fn func(*Conn) bool) {
	for _, b := range m.buckets {
		b.Mu.RLock()
		for _, c := range b.Conns {
			if !fn(c) {
				b.Mu.RUnlock()
				return
			}
		}
		b.Mu.RUnlock()
	}
}

// CloseAll 关闭全部连接（服务优雅退出时调用）
func (m *ConnManager) CloseAll() {
	for _, b := range m.buckets {
		b.Mu.Lock()
		Conns := make([]*Conn, 0, len(b.Conns))
		for _, c := range b.Conns {
			Conns = append(Conns, c)
		}
		b.Conns = make(map[string]*Conn)
		b.Mu.Unlock()

		for _, c := range Conns {
			c.Close() // 锁外关闭，避免持有锁时做 IO
		}
	}
}

