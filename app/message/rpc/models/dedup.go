package models

import (
	"context"
	"fmt"
	"time"

	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/core/stores/redis"
)

type DedupModel struct {
	redis *redis.Redis
}

func NewDedupModel(r *redis.Redis) *DedupModel {
	return &DedupModel{redis: r}
}

func (m *DedupModel) dedupKey(convId, clientMsgId string) string {
	return fmt.Sprintf("im:dedup:%s:%s", convId, clientMsgId)
}

var dedupTtlSeconds = int(24 * time.Hour.Seconds())

// TryAcquire 分发前占坑(SET NX EX,每条消息独立 key):
// 返回 true 表示首次接收,可以进行 seq 分配与持久化;
// 返回 false 表示已接收过或在途,调用方应反查 PG 返回原消息。
// 统一占坑时序后,客户端快速重试不会拿到两个 seq
func (m *DedupModel) TryAcquire(ctx context.Context, convId, clientMsgId string) (bool, error) {
	ok, err := m.redis.SetnxExCtx(ctx, m.dedupKey(convId, clientMsgId), "1", dedupTtlSeconds)
	if err != nil {
		// Redis 故障:放行,靠 PG 唯一索引兜底(不阻断发送)
		logx.WithContext(ctx).Errorf("dedup try acquire failed, pass through: conv=%s cmid=%s err=%v", convId, clientMsgId, err)
		return true, nil
	}
	return ok, nil
}

// Release 持久化失败时释放占坑,让客户端重试不被幂等键挡住
func (m *DedupModel) Release(ctx context.Context, convId, clientMsgId string) {
	if _, err := m.redis.DelCtx(ctx, m.dedupKey(convId, clientMsgId)); err != nil {
		logx.WithContext(ctx).Errorf("dedup release failed: conv=%s cmid=%s err=%v", convId, clientMsgId, err)
	}
}
