package logic

import (
	"context"
	"fmt"

	"im-platform/app/user/rpc/internal/svc"
	"im-platform/app/user/rpc/user"

	"github.com/zeromicro/go-zero/core/logx"
)

type IsBlockedLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewIsBlockedLogic(ctx context.Context, svcCtx *svc.ServiceContext) *IsBlockedLogic {
	return &IsBlockedLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *IsBlockedLogic) IsBlocked(in *user.IsBlockedReq) (*user.IsBlockedResp, error) {
	if in.UserId <= 0 || in.TargetId <= 0 {
		return nil, fmt.Errorf("invalid is blocked req: user=%d target=%d", in.UserId, in.TargetId)
	}
	blocked, err := l.svcCtx.FriendsModel.IsBlocked(l.ctx, in.UserId, in.TargetId)
	if err != nil {
		return nil, err
	}
	return &user.IsBlockedResp{
		IsBlocked: blocked,
	}, nil
}
