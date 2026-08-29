package logic

import (
	"context"
	"database/sql"

	"im-platform/app/user/rpc/internal/svc"
	"im-platform/app/user/rpc/models"
	"im-platform/app/user/rpc/user"
	"github.com/zeromicro/go-zero/core/logx"
)

type AddFriendLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewAddFriendLogic(ctx context.Context, svcCtx *svc.ServiceContext) *AddFriendLogic {
	return &AddFriendLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

// 好友管理
func (l *AddFriendLogic) AddFriend(in *user.AddFriendReq) (*user.AddFriendResp, error) {
	// todo: add your logic here and delete this line
	id:=l.svcCtx.Snokflake.NextID()
	_,err := l.svcCtx.FriendAppliesModel.Insert(l.ctx, &models.Friendapplies{
		TargetId: in.TargetUserId,
		ApplyReason: sql.NullString{
			String: in.ApplyReason,
			Valid: true,
		},
		ApplicantId: id,
		Status: 1,
	})
	if err != nil {
		return nil, err
	}
	return &user.AddFriendResp{
		ApplyId: id,
	}, nil
}
