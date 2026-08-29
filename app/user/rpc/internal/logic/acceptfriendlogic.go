package logic

import (
	"context"
	"database/sql"

	"im-platform/app/user/rpc/internal/svc"
	"im-platform/app/user/rpc/models"
	"im-platform/app/user/rpc/user"

	"github.com/zeromicro/go-zero/core/logx"
)

type AcceptFriendLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewAcceptFriendLogic(ctx context.Context, svcCtx *svc.ServiceContext) *AcceptFriendLogic {
	return &AcceptFriendLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *AcceptFriendLogic) AcceptFriend(in *user.AcceptFriendReq) (*user.AcceptFriendResp, error) {
	// todo: add your logic here and delete this line
	err:=l.svcCtx.FriendAppliesModel.Update(l.ctx,&models.Friendapplies{
		Status: 2,
		ApplicantId: in.ApplyId,
		HandlerRemark: sql.NullString{
			String: in.Remark,
			Valid: true,
		},
	})
	if err != nil {
		return nil, err
	}
	return &user.AcceptFriendResp{
		Success: true,
	}, nil
}
