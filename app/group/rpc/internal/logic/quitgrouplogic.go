package logic

import (
	"context"
	"errors"

	"im-platform/app/group/rpc/group"
	"im-platform/app/group/rpc/internal/svc"
	"im-platform/common/constants"

	"github.com/zeromicro/go-zero/core/logx"
)

type QuitGroupLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewQuitGroupLogic(ctx context.Context, svcCtx *svc.ServiceContext) *QuitGroupLogic {
	return &QuitGroupLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

// QuitGroup 退群:成员删除自己的成员行(群主不允许直接退,须先转让或解散)
func (l *QuitGroupLogic) QuitGroup(in *group.QuitGroupReq) (*group.QuitGroupResp, error) {
	if in.GroupId <= 0 || in.UserId <= 0 {
		return nil, constants.NewMsgError(constants.ErrCodeMsgInValidParam)
	}
	g, err := fetchGroup(l.ctx, l.svcCtx, in.GroupId)
	if err != nil {
		return nil, err
	}
	member, err := fetchMember(l.ctx, l.svcCtx, in.GroupId, in.UserId)
	if err != nil {
		return nil, err
	}
	if member.Role == int64(group.GroupRole_OWNER) {
		return nil, errors.New("群主不能直接退群,请先转让群主或解散群")
	}
	if err := l.svcCtx.GroupsModel.RemoveMemberTx(l.ctx, in.GroupId, in.UserId); err != nil {
		return nil, err
	}
	return &group.QuitGroupResp{MemberVersion: g.MemberVersion + 1}, nil
}
