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

func ConnKey(userID int64, deviceType int32) string {
	return strconv.FormatInt(userID, 10) + ":" + strconv.FormatInt(int64(deviceType), 10)
}
//有倾斜的可能
func (m *ConnManager) BucketOf(userID int64) *bucket {
	idx := uint64(userID) % uint64((len(m.buckets)))
	return m.buckets[idx]
}

// Add 注册连接到分桶;返回同 key 被顶替的旧连接(没有则 nil)。
// 桶操作只收敛于此一处;旧连接的"踢下线通知 + Close"由调用方在锁外完成——
// 踢下线帧(FrameKick)必须先于 Close 发出,所以这里不能代关
func (m *ConnManager) Add(c *Conn) *Conn {
	b := m.BucketOf(c.userID)
	b.Mu.Lock()
	key := c.Key()
	var old *Conn
	if cur := b.Conns[key]; cur != nil && cur != c {
		old = cur
	}
	b.Conns[key] = c
	b.Mu.Unlock()
	return old
}

func (m *ConnManager) Get(userID int64, deviceType int32) (*Conn, bool) {
	b := m.BucketOf(userID)
	b.Mu.RLock()
	defer b.Mu.RUnlock()
	c, ok := b.Conns[ConnKey(userID, deviceType)]
	return c, ok
}

func (m *ConnManager) GetByUser(userID int64) []*Conn {
	b := m.BucketOf(userID)
	b.Mu.RLock()
	defer b.Mu.RUnlock()

	var res []*Conn
	for _, c := range b.Conns {
		if c.userID == userID {
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

func (m *ConnManager) Remove(userID int64, deviceType int32) {
	b := m.BucketOf(userID)
	b.Mu.Lock()
	key:=ConnKey(userID,deviceType)
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
	b := m.BucketOf(c.userID)
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
// 注意：不要在 fn 里调用 manager 的加锁方法，否则死锁。
// 锁内只收集,锁外执行回调,避免回调里再做 Redis/网络调用时拖住桶锁
func (m *ConnManager) Range(fn func(*Conn)) {
	for _, b := range m.buckets {
		// 1. 锁内只收集
		b.Mu.RLock()
		conns := make([]*Conn, 0, len(b.Conns))
		for _, c := range b.Conns {
			conns = append(conns, c)
		}
		b.Mu.RUnlock()

		// 2. 锁外执行回调
		for _, c := range conns {
			fn(c)
		}
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

