package logic

import (
	"context"

	"im-platform/app/user/rpc/internal/svc"
	"im-platform/app/user/rpc/user"

	"github.com/zeromicro/go-zero/core/logx"
)

type UnblockUserLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewUnblockUserLogic(ctx context.Context, svcCtx *svc.ServiceContext) *UnblockUserLogic {
	return &UnblockUserLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *UnblockUserLogic) UnblockUser(in *user.UnblockUserReq) (*user.UnblockUserResp, error) {
	// todo: add your logic here and delete this line
	// 定向更新 status,避免生成版 Update(单列 where + 全列覆盖)误伤其他关系行
	err := l.svcCtx.FriendsModel.UpdateRelationStatus(l.ctx, in.UserId, in.TargetUserId, 1)
	if err != nil {
		return nil, err
	}
	return &user.UnblockUserResp{
		Success: true,
	}, nil
}
