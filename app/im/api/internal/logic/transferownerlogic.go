// Code scaffolded by goctl. Safe to edit.
// goctl 1.10.1

package logic

import (
	"context"
	"fmt"

	"im-platform/app/group/rpc/group"
	"im-platform/app/im/api/internal/svc"
	"im-platform/app/im/api/internal/types"
	"im-platform/common/middleware"

	"github.com/zeromicro/go-zero/core/logx"
)

type TransferOwnerLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewTransferOwnerLogic(ctx context.Context, svcCtx *svc.ServiceContext) *TransferOwnerLogic {
	return &TransferOwnerLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

// TransferOwner 转让群主:old_owner 取自 token(只有本人能转出自己的群)
func (l *TransferOwnerLogic) TransferOwner(req *types.TransferOwnerReq) (*types.GroupOpResp, error) {
	uid, ok := middleware.GetUserID(l.ctx)
	if !ok || uid <= 0 {
		return nil, fmt.Errorf("unauthorized")
	}
	resp, err := l.svcCtx.Group.TransferOwner(l.ctx, &group.TransferOwnerReq{
		GroupId:    req.GroupId,
		OldOwnerId: uid,
		NewOwnerId: req.NewOwnerId,
	})
	if err != nil {
		return nil, err
	}
	return &types.GroupOpResp{MemberVersion: resp.MemberVersion}, nil
}
