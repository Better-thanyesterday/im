package logic

import (
	"context"
	"errors"
	"fmt"

	"im-platform/app/user/rpc/internal/svc"
	"im-platform/app/user/rpc/user"

	"github.com/zeromicro/go-zero/core/logx"
)

type RejectFriendLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewRejectFriendLogic(ctx context.Context, svcCtx *svc.ServiceContext) *RejectFriendLogic {
	return &RejectFriendLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

// RejectFriend 拒绝好友申请:只有被申请人能处理,且申请必须处于待处理状态
func (l *RejectFriendLogic) RejectFriend(in *user.RejectFriendReq) (*user.RejectFriendResp, error) {
	if in.ApplyId <= 0 || in.OperatorId <= 0 {
		return nil, fmt.Errorf("invalid reject friend req: apply=%d operator=%d", in.ApplyId, in.OperatorId)
	}
	apply, err := l.svcCtx.FriendAppliesModel.FindOne(l.ctx, in.ApplyId)
	if err != nil {
		return nil, err
	}
	// 归属校验:被申请人才能处理,防止篡改他人的申请
	if apply.TargetId != in.OperatorId {
		return nil, errors.New("只有被申请人可以处理该好友申请")
	}
	if apply.Status != 1 {
		return nil, errors.New("该申请已处理过")
	}
	if err := l.svcCtx.FriendAppliesModel.RejectFriend(l.ctx, in.ApplyId, in.OperatorId); err != nil {
		return nil, err
	}
	return &user.RejectFriendResp{Success: true}, nil
}
