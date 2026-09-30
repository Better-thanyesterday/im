package logic

import (
	"context"
	"fmt"

	"im-platform/app/user/rpc/internal/svc"
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
	if in.ApplyId <= 0 {
		return nil, fmt.Errorf("invalid apply id: %d", in.ApplyId)
	}
	apply, err := l.svcCtx.FriendAppliesModel.FindOne(l.ctx, in.ApplyId)
	if err != nil {
		return nil, err
	}
	// 事务内:置申请已处理 + 写入双向好友关系(原来只改申请状态且 where id=0 no-op,
	// 好友关系从未落库)
	err = l.svcCtx.FriendAppliesModel.AcceptFriendTx(l.ctx, in.ApplyId, apply.ApplicantId, apply.TargetId, in.Remark)
	if err != nil {
		return nil, err
	}
	return &user.AcceptFriendResp{
		Success: true,
	}, nil
}
