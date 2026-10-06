package hotconfig

import "sync/atomic"

// atomicValue 原子持有不可变配置快照(指针整体替换,读侧无锁)
type atomicValue[T any] struct {
	v atomic.Value
}

func (a *atomicValue[T]) Store(t *T) { a.v.Store(t) }

func (a *atomicValue[T]) Load() *T {
	if v, ok := a.v.Load().(*T); ok && v != nil {
		return v
	}
	// 理论不可达:New 一定先 Store;兜底返回零值,保证 Get 永不为 nil
	return new(T)
}
