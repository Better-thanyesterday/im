package logic

import (
	"context"

	"im-platform/app/group/rpc/group"
	"im-platform/app/group/rpc/internal/svc"

	"github.com/zeromicro/go-zero/core/logx"
)

type CheckMemberLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewCheckMemberLogic(ctx context.Context, svcCtx *svc.ServiceContext) *CheckMemberLogic {
	return &CheckMemberLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}
//极轻量的权限校验，验证“某个用户”在群里的有效性和状态
func (l *CheckMemberLogic) CheckMember(in *group.CheckMemberReq) (*group.CheckMemberResp, error) {
	// todo: add your logic here and delete this line

	return &group.CheckMemberResp{}, nil
}
