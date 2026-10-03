package logic

import (
	"context"
	"fmt"

	"im-platform/app/group/rpc/group"
	"im-platform/app/group/rpc/internal/svc"
	"im-platform/common/constants"

	"github.com/zeromicro/go-zero/core/logx"
)

type UpdateGroupInfoLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewUpdateGroupInfoLogic(ctx context.Context, svcCtx *svc.ServiceContext) *UpdateGroupInfoLogic {
	return &UpdateGroupInfoLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

// UpdateGroupInfo 修改群资料:群主/管理员可操作;proto 约定空串/0 表示不修改
func (l *UpdateGroupInfoLogic) UpdateGroupInfo(in *group.UpdateGroupInfoReq) (*group.UpdateGroupInfoResp, error) {
	if in.GroupId <= 0 || in.OperatorId <= 0 {
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
	if err := requirePrivilege(operator, "修改群信息"); err != nil {
		return nil, err
	}
	var name, avatar, notice *string
	if in.Name != "" {
		name = &in.Name
	}
	if in.Avatar != "" {
		avatar = &in.Avatar
	}
	if in.Notice != "" {
		notice = &in.Notice
	}
	var invitePerm, joinApproval, maxMember *int64
	if in.InvitePermission > 0 {
		v := int64(in.InvitePermission)
		invitePerm = &v
	}
	if in.JoinApproval > 0 {
		v := int64(in.JoinApproval)
		joinApproval = &v
	}
	if in.MaxMember > 0 {
		v := int64(in.MaxMember)
		if v < g.MemberCount {
			return nil, fmt.Errorf("最大成员数不能小于当前成员数 %d", g.MemberCount)
		}
		maxMember = &v
	}
	if err := l.svcCtx.GroupsModel.UpdateGroupInfoFields(l.ctx, in.GroupId, name, avatar, notice, invitePerm, joinApproval, maxMember); err != nil {
		return nil, err
	}
	return &group.UpdateGroupInfoResp{}, nil
}
