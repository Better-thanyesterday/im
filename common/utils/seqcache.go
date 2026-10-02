package utils

import (
	"sync"
	"sync/atomic"
)

type SeqIdCache struct {
	mu sync.RWMutex
	// 对象级缓存：批量预取的 SeqID 段，减少 Redis 往返
	// key: conv_id, value: {current, max, mu}
	segments map[string]*seqSegment
}

type seqSegment struct {
	current int64 // 当前已分配的 Seq
	max     int64 // 预取到的上限（含）
}

func NewSeqIdCache() *SeqIdCache {
	return &SeqIdCache{
		segments: make(map[string]*seqSegment),
	}
}

// Get 尝试从本地缓存分配一个 seq
// 返回值: (seq, ok)
func (c *SeqIdCache) Get(convId string) (int64, bool) {
	c.mu.RLock()
	seg, ok := c.segments[convId]
	c.mu.RUnlock()

	if !ok {
		return 0, false
	}
	// 先原子加，再判断越界；如果越界需要回滚（或者先判断再加，但判断和加不是原子的）
	// 更简单的做法：先读 current+1 是否 <= max，再 CAS 或直接用 atomic 并检查返回值
	for {
		cur := atomic.LoadInt64(&seg.current)
		if cur >= seg.max {
			return 0, false // 本段耗尽，触发重新预取
		}
		if atomic.CompareAndSwapInt64(&seg.current, cur, cur+1) {
			return cur + 1, true
		}
	}
}

// Put 放入新预取的段。
// 正常路径:start=段底-1、max=段顶,Get 从 start+1 连续发号到 max;
// max=0(或 max<=start)是"一次性段":仅记录 current、无可发区间,Get 恒 miss——
// 供 PG 兜底路径使用,单号分配没有可缓存的区间,防止把未经 PG 批准的号发出去
func (c *SeqIdCache) Put(convId string, start, max int64) {
	c.mu.Lock()
	if max != 0 && max > start {
		c.segments[convId] = &seqSegment{
			current: start,
			max:     max,
		}
	} else {
		c.segments[convId] = &seqSegment{
			current: start,
		}
	}
	c.mu.Unlock()
}
