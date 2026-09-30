package logic 

import (
	"context"
	"fmt"
	"im-platform/app/message/rpc/internal/svc"
	"strconv"
	_ "sync/atomic"

	"github.com/zeromicro/go-zero/core/logx"
)

type SeqIdLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}



func NewSeqIdLogic(ctx context.Context, svcCtx *svc.ServiceContext) *SeqIdLogic {
	return &SeqIdLogic{
		ctx:      ctx,
		svcCtx:   svcCtx,
		Logger:   logx.WithContext(ctx),
	}
}

// AllocateSeq 为指定会话分配下一个 SeqID
// 策略：Redis INCR（主）→ 批量预取（优化）→ PG 兜底（降级）
func (l *SeqIdLogic) AllocateSeq(convId string) (seq int64, needAsync bool, err error) {
	//local Cache hit
	seg, ok := l.svcCtx.SeqIdCache.Get(convId);
	if  ok{
		l.Infof("seq hit cache, conv=%s, seq=%d", convId, seg)
		//写缓存成功写kafka
		return seg, true, nil
	}
	// 2. Redis 批量预取：一次 INCRBY 拿 N 个 Seq，减少 90% 的 Redis 往返
	
	const batchSize int64 = 100
	seqKey := fmt.Sprintf("im:seq:%s", convId)
	// Redis INCRBY 原子返回当前最大值
	maxSeq, err := l.svcCtx.Redis.Incrby(seqKey, batchSize)
	if err != nil {
		logx.Errorf("redis incrby failed, fallback to pg: %v", err)
		// 降级到 PG 兜底
		seq,err := l.allocateFromPG(convId);
		if  err != nil {
			logx.Errorf("allocateFromPG failed, fallback to pg: %v", err)
			return 0, false, err
			// 兜底失败写kafka
		}
		l.svcCtx.SeqIdCache.Put(convId,seq,0)
		// PG 分配的号必须回填 Redis(只前进不回退),否则 Redis 恢复后计数器
		// 停在旧值,会重复分配 PG 已发出的号
		l.backfillRedisSeq(seqKey, seq)
		return seq, false, nil
	}
	// seq 计数器是会话的永久单调状态,绝不能设 TTL:
	// 过期后从 0 重新计数,所有端按旧 seq 同步会整体错乱
	start := maxSeq - batchSize
	l.svcCtx.SeqIdCache.Put(convId, start, maxSeq)
	l.Infof("seq preallocated, conv=%s, range=[%d,%d]", convId, start+1, maxSeq)
	//redis 成功写kafka
	return start + 1, true, nil
}

// backfillRedisSeq 把 PG 分配出的水位回填到 Redis(仅前进),尽力而为
func (l *SeqIdLogic) backfillRedisSeq(seqKey string, pgSeq int64) {
	lua := `
		local cur = tonumber(redis.call('GET', KEYS[1]) or '0')
		local target = tonumber(ARGV[1])
		if cur < target then
			redis.call('SET', KEYS[1], target)
		end
		return 1
	`
	if _, err := l.svcCtx.Redis.EvalCtx(l.ctx, lua, []string{seqKey}, strconv.FormatInt(pgSeq, 10)); err != nil {
		logx.Errorf("backfill redis seq failed: key=%s seq=%d err=%v", seqKey, pgSeq, err)
	}
}

// allocateFromPG PG 兜底：直接行锁更新 seq_counters 表
func (l *SeqIdLogic) allocateFromPG(convId string) (int64, error) {
	var seq int64
	seq, err := l.svcCtx.SeqModel.CustomQueryRowCtx(l.ctx, convId)
	if err != nil {
		return 0, fmt.Errorf("pg allocate seq failed: %w", err)
	}
	return seq, nil
}

// BatchAllocateSeq 批量分配（用于群聊写扩散，一次给 N 个成员各分配 inbox seq）
func (l *SeqIdLogic) BatchAllocateSeq(convId string, count int) ([]int64, error) {
	if count <= 0 {
		return nil, nil
	}
	seqKey := fmt.Sprintf("im:seq:%s", convId)
	maxSeq, err := l.svcCtx.Redis.Incrby(seqKey, int64(count))
	if err != nil {
		logx.Errorf("redis batch incrby failed: %v", err)
		// 降级：逐个 PG 分配（慢但可靠）
		seqs := make([]int64, count)
		for i := 0; i < count; i++ {
			seq, err := l.allocateFromPG(convId)
			if err != nil {
				return nil, err
			}
			seqs[i] = seq
		}
		return seqs, nil
	}
	// 生成连续序列号
	seqs := make([]int64, count)
	start := maxSeq - int64(count) + 1
	for i := 0; i < count; i++ {
		seqs[i] = start + int64(i)
	}
	return seqs, nil
}
