package logic

import (
	"context"
	"database/sql"
	"fmt"

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
	if in.UserId <= 0 || in.TargetUserId <= 0 || in.UserId == in.TargetUserId {
		return nil, fmt.Errorf("invalid add friend req: user=%d target=%d", in.UserId, in.TargetUserId)
	}
	// ApplicantId 必须是申请人 uid(原实现存的是新雪花 ID,接受时无法还原申请人)
	applyId, err := l.svcCtx.FriendAppliesModel.InsertApply(l.ctx, &models.Friendapplies{
		ApplicantId: in.UserId,
		TargetId: in.TargetUserId,
		ApplyReason: sql.NullString{
			String: in.ApplyReason,
			Valid: true,
		},
		Status: 1,
	})
	if err != nil {
		return nil, err
	}
	return &user.AddFriendResp{
		ApplyId: applyId,
	}, nil
}
