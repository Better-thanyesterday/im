package logic

import (
	"context"
	"fmt"

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

// ClearUnread 清零 im:unread:{user_id} 的未读计数:
// conv_id 为空清全会话,否则只清指定会话(与 DeliverLogic.incrUnread 的写入对应)
func (l *ClearUnreadLogic) ClearUnread(in *push.ClearUnreadReq) (*push.ClearUnreadResp, error) {
	// todo: add your logic here and delete this line
	if in.UserId <= 0 {
		return nil, fmt.Errorf("invalid clear unread req: user=%d", in.UserId)
	}
	key := fmt.Sprintf("im:unread:%d", in.UserId)
	if in.ConvId == "" {
		if _, err := l.svcCtx.Redis.Del(key); err != nil {
			logx.Errorf("clear unread del failed: key=%s err=%v", key, err)
			return nil, err
		}
		return &push.ClearUnreadResp{}, nil
	}
	if _, err := l.svcCtx.Redis.HdelCtx(l.ctx, key, in.ConvId); err != nil {
		logx.Errorf("clear unread hdel failed: key=%s conv=%s err=%v", key, in.ConvId, err)
		return nil, err
	}
	return &push.ClearUnreadResp{}, nil
}
