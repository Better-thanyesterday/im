package logic

import (
	"context"
	"errors"
	"time"

	"im-platform/app/group/rpc/group"
	"im-platform/app/group/rpc/internal/svc"
	"im-platform/common/constants"

	"github.com/zeromicro/go-zero/core/logx"
)

type MuteMemberLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewMuteMemberLogic(ctx context.Context, svcCtx *svc.ServiceContext) *MuteMemberLogic {
	return &MuteMemberLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

// MuteMember 禁言/解除禁言:mute_until=0 表示解除;群主可禁言任何人,管理员只能禁言普通成员
func (l *MuteMemberLogic) MuteMember(in *group.MuteMemberReq) (*group.MuteMemberResp, error) {
	if in.GroupId <= 0 || in.OperatorId <= 0 || in.TargetId <= 0 {
		return nil, constants.NewMsgError(constants.ErrCodeMsgInValidParam)
	}
	if _, err := fetchGroup(l.ctx, l.svcCtx, in.GroupId); err != nil {
		return nil, err
	}
	operator, err := fetchMember(l.ctx, l.svcCtx, in.GroupId, in.OperatorId)
	if err != nil {
		return nil, err
	}
	if err := requirePrivilege(operator, "禁言成员"); err != nil {
		return nil, err
	}
	if in.OperatorId == in.TargetId {
		return nil, errors.New("不能禁言自己")
	}
	target, err := fetchMember(l.ctx, l.svcCtx, in.GroupId, in.TargetId)
	if err != nil {
		return nil, err
	}
	if target.Role == int64(group.GroupRole_OWNER) {
		return nil, errors.New("不能禁言群主")
	}
	if target.Role == int64(group.GroupRole_ADMIN) && operator.Role != int64(group.GroupRole_OWNER) {
		return nil, errors.New("管理员只能由群主禁言")
	}
	// mute_until=0 → 零值 time → NULL(解除禁言)
	var muteUntil time.Time
	if in.MuteUntil > 0 {
		muteUntil = time.UnixMilli(in.MuteUntil)
	}
	if err := l.svcCtx.GroupMembersModel.UpdateMuteUntil(l.ctx, in.GroupId, in.TargetId, muteUntil); err != nil {
		return nil, err
	}
	return &group.MuteMemberResp{}, nil
}
