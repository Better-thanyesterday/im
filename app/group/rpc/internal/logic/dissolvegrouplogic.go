package logic

import (
	"context"
	"errors"

	"im-platform/app/group/rpc/group"
	"im-platform/app/group/rpc/internal/svc"
	"im-platform/common/constants"

	"github.com/zeromicro/go-zero/core/logx"
)

type DissolveGroupLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewDissolveGroupLogic(ctx context.Context, svcCtx *svc.ServiceContext) *DissolveGroupLogic {
	return &DissolveGroupLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

// DissolveGroup 解散群:仅群主可操作;软删(status=2 + dissolved_at),成员行保留作历史
func (l *DissolveGroupLogic) DissolveGroup(in *group.DissolveGroupReq) (*group.DissolveGroupResp, error) {
	if in.GroupId <= 0 || in.OperatorId <= 0 {
		return nil, constants.NewMsgError(constants.ErrCodeMsgInValidParam)
	}
	g, err := fetchGroup(l.ctx, l.svcCtx, in.GroupId)
	if err != nil {
		return nil, err
	}
	if g.OwnerId != in.OperatorId {
		return nil, errors.New("只有群主可以解散群")
	}
	if err := l.svcCtx.GroupsModel.MarkDissolved(l.ctx, in.GroupId); err != nil {
		return nil, err
	}
	return &group.DissolveGroupResp{}, nil
}
