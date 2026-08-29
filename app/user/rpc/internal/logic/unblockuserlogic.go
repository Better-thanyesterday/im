package logic

import (
	"context"

	"im-platform/app/user/rpc/internal/svc"
	"im-platform/app/user/rpc/models"
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
	err := l.svcCtx.FriendsModel.Update(l.ctx, &models.Friends{
		Status:   1,
		FriendId: in.TargetUserId,
		UserId:   in.UserId,
	})
	if err != nil {
		return nil, err
	}
	return &user.UnblockUserResp{
		Success: true,
	}, nil
}
