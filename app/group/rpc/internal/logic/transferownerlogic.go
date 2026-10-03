package logic

import (
	"context"
	"errors"

	"im-platform/app/group/rpc/group"
	"im-platform/app/group/rpc/internal/svc"
	"im-platform/common/constants"

	"github.com/zeromicro/go-zero/core/logx"
)

type TransferOwnerLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewTransferOwnerLogic(ctx context.Context, svcCtx *svc.ServiceContext) *TransferOwnerLogic {
	return &TransferOwnerLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

// TransferOwner 转让群主:仅当前群主可发起,新群主必须是群成员;原群主降为普通成员
func (l *TransferOwnerLogic) TransferOwner(in *group.TransferOwnerReq) (*group.TransferOwnerResp, error) {
	if in.GroupId <= 0 || in.OldOwnerId <= 0 || in.NewOwnerId <= 0 {
		return nil, constants.NewMsgError(constants.ErrCodeMsgInValidParam)
	}
	g, err := fetchGroup(l.ctx, l.svcCtx, in.GroupId)
	if err != nil {
		return nil, err
	}
	if g.OwnerId != in.OldOwnerId {
		return nil, errors.New("只有群主可以转让群主")
	}
	if in.OldOwnerId == in.NewOwnerId {
		return nil, errors.New("新群主不能是自己")
	}
	// 确认新群主是成员(TRANSFER 要求 target 必须在群内)
	if _, err := fetchMember(l.ctx, l.svcCtx, in.GroupId, in.NewOwnerId); err != nil {
		return nil, err
	}
	if err := l.svcCtx.GroupsModel.TransferOwnerTx(l.ctx, in.GroupId, in.OldOwnerId, in.NewOwnerId); err != nil {
		return nil, err
	}
	return &group.TransferOwnerResp{MemberVersion: g.MemberVersion + 1}, nil
}
