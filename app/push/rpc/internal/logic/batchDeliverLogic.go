package logic

import (
	"context"
	"sync"

	"im-platform/app/push/rpc/internal/svc"
	"im-platform/app/push/rpc/push"

	"github.com/zeromicro/go-zero/core/logx"
)

// batchDeliverWorkers 批量投递的并发上限:
// 不限制会把 500 人群变成 499 个并发 goroutine 同打 gateway
const batchDeliverWorkers = 8

type BatchDeliverLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewBatchDeliverLogic(ctx context.Context, svcCtx *svc.ServiceContext) *BatchDeliverLogic {
	return &BatchDeliverLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

// 批量投递:内部按受限并发逐个走 Deliver 语义(在线推送/离线兜底/未读计数)
func (l *BatchDeliverLogic) BatchDeliver(in *push.BatchDeliverReq) (*push.BatchDeliverResp, error) {
	if len(in.UserIds) == 0 || in.Message == nil {
		return &push.BatchDeliverResp{}, nil
	}
	var (
		wg      sync.WaitGroup
		mu      sync.Mutex
		success int32
		sem     = make(chan struct{}, batchDeliverWorkers)
	)
	for _, uid := range in.UserIds {
		wg.Add(1)
		sem <- struct{}{}
		go func(uid int64) {
			defer wg.Done()
			defer func() { <-sem }()
			dl := NewDeliverLogic(l.ctx, l.svcCtx)
			resp, err := dl.Deliver(l.ctx, &push.DeliverReq{
				UserId:   uid,
				PushType: in.PushType,
				Message:  in.Message,
			})
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				l.Errorf("batch deliver to %d failed: %v", uid, err)
				return
			}
			if resp != nil && resp.Success {
				success++
			}
		}(uid)
	}
	wg.Wait()
	return &push.BatchDeliverResp{
		SuccessCount: success,
	}, nil
}
