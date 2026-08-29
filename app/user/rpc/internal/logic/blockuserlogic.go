package logic

import (
	"context"

	"im-platform/app/user/rpc/internal/svc"
	"im-platform/app/user/rpc/models"
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
	err:=l.svcCtx.FriendsModel.Update(l.ctx,&models.Friends{
		Status: 2,
		FriendId: in.TargetUserId,
		UserId: in.UserId,
	})
	if err != nil {
		return nil, err
	}
	return &user.BlockUserResp{
		Success: true,
	}, nil
}
