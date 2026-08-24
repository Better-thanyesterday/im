package logic

import (
	"context"

	"github.com/zeromicro/go-zero/core/logx"
	"im-platform/app/group/rpc/group"
	"im-platform/app/group/rpc/internal/svc"
)

type GetMembersLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewGetMembersLogic(ctx context.Context, svcCtx *svc.ServiceContext) *GetMembersLogic {
	return &GetMembersLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

// ==================== 查询接口（供 Message 服务调用） ====================
func (l *GetMembersLogic) GetMembers(in *group.GetMembersReq) (*group.GetMembersResp, error) {
	// todo: add your logic here and delete this line

	return &group.GetMembersResp{}, nil
}
