package logic

import (
	"context"
	"fmt"
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

// 批量投递:内部按受限并发逐个走 Deliver 语义(在线推送/离线兜底/未读计数)。
// 到达顺序:同用户跨消息不保证有序(并发投递+实时/离线双路径),客户端契约是按
// seq_id 会话内重排去重(见 push.proto PushMessage.seq_id),不做服务端按 uid 串行化
func (l *BatchDeliverLogic) BatchDeliver(in *push.BatchDeliverReq) (*push.BatchDeliverResp, error) {
	if len(in.UserIds) == 0 || in.Message == nil {
		return &push.BatchDeliverResp{}, nil
	}
	// 同一 Message 推 N 个用户:payload 按 PushType 序列化一次全批复用(Full=完整消息,Notify=轻量帧),
	// 与单发 Deliver 的下行协议一致
	body, err := marshalPayload(in.PushType, in.Message)
	if err != nil {
		return nil, fmt.Errorf("marshal push message failed: %w", err)
	}
	var (
		wg      sync.WaitGroup
		mu      sync.Mutex
		success int32
		failed  []int64
		sem     = make(chan struct{}, batchDeliverWorkers)
	)
	for _, uid := range in.UserIds {
		wg.Add(1)
		sem <- struct{}{}
		go func(uid int64) {
			defer wg.Done()
			defer func() { <-sem }()
			dl := NewDeliverLogic(l.ctx, l.svcCtx)
			resp, err := dl.deliver(l.ctx, &push.DeliverReq{
				UserId:   uid,
				PushType: in.PushType,
				Message:  in.Message,
			}, body)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				l.Errorf("batch deliver to %d failed: %v", uid, err)
				failed = append(failed, uid)
				return
			}
			if resp != nil && resp.Success {
				success++
			} else {
				// 实时未达(已由 Deliver 转离线信箱兜底):记入 failed 供排障对账
				failed = append(failed, uid)
			}
		}(uid)
	}
	wg.Wait()
	return &push.BatchDeliverResp{
		SuccessCount:  success,
		FailedUserIds: failed,
	}, nil
}
