package logic

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"im-platform/app/message/rpc/internal/svc"

	"github.com/zeromicro/go-zero/core/logx"
)

// BundleCompensator 持久化失败批次的后台补偿。
// writeDiffusion 的 inbox 批是发射后不管:Kafka 与 PG 兜底同时失败时,
// 该批未读数/水位就永久丢失(messages 主表在,但重连水位取自 inbox 的 DeliveredMaxSeq)。
// 失败整批交给这里,单 worker 带退避重试,复用 persistBundle 的完整管道(Kafka → PG 兜底),
// 进程存活期内最终一致;进程崩溃仍会丢,彻底持久化需 PG outbox 表,当前规模不引入。
type BundleCompensator struct {
	svcCtx   *svc.ServiceContext
	ch       chan *WriteDiffBundle
	quit     chan struct{}
	stopOnce sync.Once
	dropped  atomic.Int64
}

const (
	compensateQueueSize = 4096
	// 退避 1s→16s,8 次约 1 分钟仍失败则放弃(大概率是数据本身非法,重试无意义)
	compensateMaxRetry = 8
)

func NewBundleCompensator(svcCtx *svc.ServiceContext) *BundleCompensator {
	return &BundleCompensator{
		svcCtx: svcCtx,
		ch:     make(chan *WriteDiffBundle, compensateQueueSize),
		quit:   make(chan struct{}),
	}
}

var bundleCompensator atomic.Pointer[BundleCompensator]

// InitBundleCompensator 在 main 的 servicegroup 启动前调用,返回值 sg.Add 进优雅退出
func InitBundleCompensator(svcCtx *svc.ServiceContext) *BundleCompensator {
	c := NewBundleCompensator(svcCtx)
	bundleCompensator.Store(c)
	return c
}

// submitBundleCompensation 发送路径的失败入口;未初始化时(如单测)只告警不阻塞
func submitBundleCompensation(b *WriteDiffBundle) {
	if c := bundleCompensator.Load(); c != nil {
		c.Submit(b)
	} else {
		logx.Errorf("bundle compensator not initialized, bundle dropped: conv=%s", compensateKey(b))
	}
}

// Submit 非阻塞投递。队列满 = Kafka/PG 长时间不可用,只能丢弃并计数;
// droppedTotal 持续增长需人工介入(修 Kafka 后按 PG 重建 inbox 或重放)
func (c *BundleCompensator) Submit(b *WriteDiffBundle) {
	select {
	case c.ch <- b:
	default:
		logx.Errorf("compensate queue full, bundle dropped: conv=%s inbox=%d droppedTotal=%d",
			compensateKey(b), len(b.Inboxes), c.dropped.Add(1))
	}
}

// Start 供 servicegroup 调用;单 worker 串行重试,失败批次天然保序且无需加锁
func (c *BundleCompensator) Start() {
	go c.loop()
}

// Stop 供 servicegroup 优雅退出;不 drain 队列——进程都在退出了,残余批次随进程终止
func (c *BundleCompensator) Stop() {
	c.stopOnce.Do(func() { close(c.quit) })
}

func (c *BundleCompensator) loop() {
	for {
		select {
		case b := <-c.ch:
			c.retry(b)
		case <-c.quit:
			return
		}
	}
}

func (c *BundleCompensator) retry(b *WriteDiffBundle) {
	delay := time.Second
	for attempt := 1; attempt <= compensateMaxRetry; attempt++ {
		// 补偿与原始请求生命周期无关,用脱离请求的 ctx
		ctx := context.Background()
		if err := NewAsyncPersistMsg(ctx, c.svcCtx).persistBundle(ctx, b); err == nil {
			return
		} else {
			logx.Errorf("compensate bundle failed: attempt=%d/%d conv=%s err=%v",
				attempt, compensateMaxRetry, compensateKey(b), err)
		}
		select {
		case <-time.After(delay):
			if delay < 16*time.Second {
				delay *= 2
			}
		case <-c.quit:
			return
		}
	}
	logx.Errorf("compensate bundle give up: conv=%s inbox=%d droppedTotal=%d",
		compensateKey(b), len(b.Inboxes), c.dropped.Add(1))
}

func compensateKey(b *WriteDiffBundle) string {
	if b.Msg != nil {
		return b.Msg.Convid
	}
	if len(b.Inboxes) > 0 {
		return b.Inboxes[0].Convid
	}
	return ""
}
