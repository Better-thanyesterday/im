// Code scaffolded by goctl. Safe to edit.
// goctl 1.10.1

package logic

import (
	"context"

	"im-platform/app/group/rpc/group"
	"im-platform/app/im/api/internal/svc"
	"im-platform/app/im/api/internal/types"

	"github.com/zeromicro/go-zero/core/logx"
)

type GetMemberVersionLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewGetMemberVersionLogic(ctx context.Context, svcCtx *svc.ServiceContext) *GetMemberVersionLogic {
	return &GetMemberVersionLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

// GetMemberVersion 查成员版本号(客户端成员缓存失效校验用)
func (l *GetMemberVersionLogic) GetMemberVersion(req *types.GetMemberVersionReq) (*types.GetMemberVersionResp, error) {
	resp, err := l.svcCtx.Group.GetMemberVersion(l.ctx, &group.GetMemberVersionReq{GroupId: req.GroupId})
	if err != nil {
		return nil, err
	}
	return &types.GetMemberVersionResp{
		MemberVersion: resp.MemberVersion,
		MemberCount:   resp.MemberCount,
	}, nil
}
