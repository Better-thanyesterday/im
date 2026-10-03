package logic

import (
	"context"

	"im-platform/app/user/rpc/internal/svc"
	"im-platform/app/user/rpc/user"

	"github.com/zeromicro/go-zero/core/logx"
)

type BlockUserLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewBlockUserLogic(ctx context.Context, svcCtx *svc.ServiceContext) *BlockUserLogic {
	return &BlockUserLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *BlockUserLogic) BlockUser(in *user.BlockUserReq) (*user.BlockUserResp, error) {
	// todo: add your logic here and delete this line
	// 定向更新 status,避免生成版 Update(单列 where + 全列覆盖)误伤其他关系行
	err := l.svcCtx.FriendsModel.UpdateRelationStatus(l.ctx, in.UserId, in.TargetUserId, 2)
	if err != nil {
		return nil, err
	}
	return &user.BlockUserResp{
		Success: true,
	}, nil
}
