// Package hotconfig 基于 etcd watch 的热配置:watch 一个 key,内容变更即原子替换,
// 服务内通过 Get() 读取,不重启生效。
//
// 用法:
//
//	cfg := hotconfig.New[MessageCfg](cli, "im/hotcfg/message", &MessageCfg{SendRateLimit: 30})
//	go cfg.Watch(context.Background())          // main 里起 goroutine
//	limit := cfg.Get().SendRateLimit            // 业务代码随时读,总是最新值
package hotconfig

import (
	"context"
	"encoding/json"
	"time"

	"github.com/zeromicro/go-zero/core/logx"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type HotConfig[T any] struct {
	client *clientv3.Client
	key    string
	cur    atomicValue[T]
}

// New 用 initial 值初始化并立即尝试从 etcd 加载一次(加载失败不影响启动)
func New[T any](client *clientv3.Client, key string, initial *T) *HotConfig[T] {
	h := &HotConfig[T]{client: client, key: key}
	h.cur.Store(initial)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if resp, err := client.Get(ctx, key); err == nil && len(resp.Kvs) > 0 {
		h.apply(resp.Kvs[0].Value)
	}
	return h
}

// Get 返回当前配置快照,永不为 nil
func (h *HotConfig[T]) Get() *T {
	return h.cur.Load()
}

// Watch 阻塞监听 key 变更,每个事件原子替换配置快照;进程生命周期内常驻
func (h *HotConfig[T]) Watch(ctx context.Context) {
	for {
		ch := h.client.Watch(ctx, h.key)
		for wresp := range ch {
			for _, ev := range wresp.Events {
				if ev.Type == clientv3.EventTypePut {
					h.apply(ev.Kv.Value)
				}
			}
		}
		// watch 断流:退避重连
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Second):
		}
	}
}

func (h *HotConfig[T]) apply(data []byte) {
	v := new(T)
	if err := json.Unmarshal(data, v); err != nil {
		logx.Errorf("热配置解析失败,保留旧值 | key=%s err=%v", h.key, err)
		return
	}
	h.cur.Store(v)
	logx.Infof("热配置已生效 | key=%s value=%s", h.key, string(data))
}

// _ = fmt 占位避免未使用告警(示例代码片段引用了 fmt.Errorf 风格的错误返回)
