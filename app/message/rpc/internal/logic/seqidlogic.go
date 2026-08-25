package logic

import (
	"context"
	"fmt"
	"im-platform/app/message/rpc/internal/svc"
	"sync/atomic"
	_ "sync/atomic"

	"github.com/zeromicro/go-zero/core/logx"
)

type SeqIdLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
	// 对象级缓存：批量预取的 SeqID 段，减少 Redis 往返
	// key: conv_id, value: {current, max, mu}
	seqCache map[string]*seqSegment
}

type seqSegment struct {
	current int64 // 当前已分配的 Seq
	max     int64 // 预取到的上限（含）
}

func NewSeqIdLogic(ctx context.Context, svcCtx *svc.ServiceContext) *SeqIdLogic {
	return &SeqIdLogic{
		ctx:      ctx,
		svcCtx:   svcCtx,
		Logger:   logx.WithContext(ctx),
		seqCache: make(map[string]*seqSegment),
	}
}

// AllocateSeq 为指定会话分配下一个 SeqID
// 策略：Redis INCR（主）→ 批量预取（优化）→ PG 兜底（降级）
func (l *SeqIdLogic) AllocateSeq(convId string) (int64, error, bool) {
	//local Cache hit
	seg, ok := l.seqCache[convId];
	if  ok && seg.current < seg.max {
		atomic.AddInt64(&seg.current,1)
		l.Infof("seq hit cache, conv=%s, seq=%d", convId, seg.current)
		//写缓存成功写kafka
		return seg.current, nil, true
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
			return 0,err,false
			// 兜底失败写kafka
		}
		seg.current=seq
		return seq ,nil, false
	}
	_ = l.svcCtx.Redis.Expire(seqKey, 86400*30)
	start := maxSeq - batchSize
	l.seqCache[convId] = &seqSegment{
		current: start,
		max:     maxSeq,
	}
	l.Infof("seq preallocated, conv=%s, range=[%d,%d]", convId, start, maxSeq)
	//redis 成功写kafka
	return start, nil, true
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
	_ = l.svcCtx.Redis.Expire(seqKey, 86400*30)

	// 生成连续序列号
	seqs := make([]int64, count)
	start := maxSeq - int64(count) + 1
	for i := 0; i < count; i++ {
		seqs[i] = start + int64(i)
	}
	return seqs, nil
}
