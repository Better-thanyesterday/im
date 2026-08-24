package logic

import (
	"context"

	"im-platform/app/push/rpc/internal/svc"
	"im-platform/app/push/rpc/push"

	"github.com/zeromicro/go-zero/core/logx"
)

type ClearUnreadLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewClearUnreadLogic(ctx context.Context, svcCtx *svc.ServiceContext) *ClearUnreadLogic {
	return &ClearUnreadLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

// 清零 im:unread:{user_id} 中对应会话的计数
func (l *ClearUnreadLogic) ClearUnread(in *push.ClearUnreadReq) (*push.ClearUnreadResp, error) {
	// todo: add your logic here and delete this line

	return &push.ClearUnreadResp{}, nil
}
