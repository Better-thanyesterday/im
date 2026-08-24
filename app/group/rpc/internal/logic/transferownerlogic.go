package logic

import (
	"context"

	"im-platform/app/group/rpc/internal/svc"
	"im-platform/app/group/rpc/group"
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

func (l *TransferOwnerLogic) TransferOwner(in *group.TransferOwnerReq) (*group.TransferOwnerResp, error) {
	// todo: add your logic here and delete this line

	return &group.TransferOwnerResp{}, nil
}
