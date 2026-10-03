package logic

import (
	"context"
	"errors"

	"im-platform/app/group/rpc/group"
	"im-platform/app/group/rpc/internal/svc"
	"im-platform/common/constants"

	"github.com/zeromicro/go-zero/core/logx"
)

type SetAdminLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewSetAdminLogic(ctx context.Context, svcCtx *svc.ServiceContext) *SetAdminLogic {
	return &SetAdminLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

// SetAdmin 设/撤管理员:仅群主可操作,且不能作用于群主本人
func (l *SetAdminLogic) SetAdmin(in *group.SetAdminReq) (*group.SetAdminResp, error) {
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
	if err := requireOwner(operator); err != nil {
		return nil, err
	}
	if in.OperatorId == in.TargetId {
		return nil, errors.New("不能对自己设置管理员")
	}
	target, err := fetchMember(l.ctx, l.svcCtx, in.GroupId, in.TargetId)
	if err != nil {
		return nil, err
	}
	if target.Role == int64(group.GroupRole_OWNER) {
		return nil, errors.New("群主无需设置管理员")
	}
	role := int64(group.GroupRole_MEMBER)
	if in.IsAdmin {
		role = int64(group.GroupRole_ADMIN)
	}
	if err := l.svcCtx.GroupsModel.SetMemberRoleTx(l.ctx, in.GroupId, in.TargetId, role); err != nil {
		return nil, err
	}
	return &group.SetAdminResp{MemberVersion: g.MemberVersion + 1}, nil
}
