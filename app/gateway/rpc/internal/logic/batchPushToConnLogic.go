package logic

import (
	"context"

	"im-platform/app/gateway/rpc/gateway"
	"im-platform/app/gateway/rpc/internal/svc"

	"github.com/zeromicro/go-zero/core/logx"
)

type BatchPushToConnLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewBatchPushToConnLogic(ctx context.Context, svcCtx *svc.ServiceContext) *BatchPushToConnLogic {
	return &BatchPushToConnLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

// 批量推送:一次 RPC 推多个用户(群聊写扩散用,避免 O(N) 次 Deliver)
func (l *BatchPushToConnLogic) BatchPushToConn(in *gateway.BatchPushToConnReq) (*gateway.BatchPushToConnResp, error) {
	// todo: add your logic here and delete this line

	return &gateway.BatchPushToConnResp{}, nil
}
