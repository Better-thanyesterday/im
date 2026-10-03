// Code scaffolded by goctl. Safe to edit.
// goctl 1.10.1

package logic

import (
	"context"
	"fmt"

	"im-platform/app/im/api/internal/svc"
	"im-platform/app/im/api/internal/types"
	"im-platform/app/user/rpc/user"
	"im-platform/common/middleware"

	"github.com/zeromicro/go-zero/core/logx"
)

type FriendAppliesLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewFriendAppliesLogic(ctx context.Context, svcCtx *svc.ServiceContext) *FriendAppliesLogic {
	return &FriendAppliesLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

// GetFriendApplies 查本人收到/发起的好友申请列表
func (l *FriendAppliesLogic) GetFriendApplies(req *types.GetFriendAppliesReq) (*types.GetFriendAppliesResp, error) {
	uid, ok := middleware.GetUserID(l.ctx)
	if !ok || uid <= 0 {
		return nil, fmt.Errorf("unauthorized")
	}
	resp, err := l.svcCtx.User.GetFriendApplies(l.ctx, &user.GetFriendAppliesReq{
		UserId:   uid,
		Status:   req.Status,
		Page:     req.Page,
		PageSize: req.PageSize,
	})
	if err != nil {
		return nil, err
	}
	applies := make([]types.FriendApply, 0, len(resp.Applies))
	for _, a := range resp.Applies {
		applies = append(applies, types.FriendApply{
			Id:                a.Id,
			ApplicantId:       a.ApplicantId,
			ApplicantNickname: a.ApplicantNickname,
			ApplicantAvatar:   a.ApplicantAvatar,
			TargetId:          a.TargetId,
			ApplyReason:       a.ApplyReason,
			Status:            a.Status,
			CreatedAt:         a.CreatedAt,
			HandledAt:         a.HandledAt,
		})
	}
	return &types.GetFriendAppliesResp{Total: resp.Total, Applies: applies}, nil
}
