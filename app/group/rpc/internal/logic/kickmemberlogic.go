package logic

import (
	"context"
	"errors"

	"im-platform/app/group/rpc/group"
	"im-platform/app/group/rpc/internal/svc"
	"im-platform/common/constants"

	"github.com/zeromicro/go-zero/core/logx"
)

type KickMemberLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewKickMemberLogic(ctx context.Context, svcCtx *svc.ServiceContext) *KickMemberLogic {
	return &KickMemberLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

// KickMember 踢人:群主/管理员可踢;不能踢群主、不能踢自己(自己走退群)
func (l *KickMemberLogic) KickMember(in *group.KickMemberReq) (*group.KickMemberResp, error) {
	if in.GroupId <= 0 || in.OperatorId <= 0 || in.TargetId <= 0 {
		return nil, constants.NewMsgError(constants.ErrCodeMsgInValidParam)
	}
	g, err := fetchGroup(l.ctx, l.svcCtx, in.GroupId)
	if err != nil {
		return nil, err
	}
	operator, err := fetchMember(l.ctx, l.svcCtx, in.GroupId, in.OperatorId)
	if err != nil {
		return nil, err
	}
	if err := requirePrivilege(operator, "踢出成员"); err != nil {
		return nil, err
	}
	if in.OperatorId == in.TargetId {
		return nil, errors.New("不能踢出自己,请使用退群")
	}
	target, err := fetchMember(l.ctx, l.svcCtx, in.GroupId, in.TargetId)
	if err != nil {
		return nil, err
	}
	if target.Role == int64(group.GroupRole_OWNER) {
		return nil, errors.New("不能踢出群主")
	}
	// 管理员只能踢普通成员,管理员之间互踢由群主处理
	if target.Role == int64(group.GroupRole_ADMIN) && operator.Role != int64(group.GroupRole_OWNER) {
		return nil, errors.New("管理员只能由群主踢出")
	}
	if err := l.svcCtx.GroupsModel.RemoveMemberTx(l.ctx, in.GroupId, in.TargetId); err != nil {
		return nil, err
	}
	return &group.KickMemberResp{MemberVersion: g.MemberVersion + 1}, nil
}
