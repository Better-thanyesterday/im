package logic

import (
	"context"

	"im-platform/app/group/rpc/group"
	"im-platform/app/group/rpc/internal/svc"

	"github.com/zeromicro/go-zero/core/logx"
)

type GetMemberVersionLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewGetMemberVersionLogic(ctx context.Context, svcCtx *svc.ServiceContext) *GetMemberVersionLogic {
	return &GetMemberVersionLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

// GetMemberVersion 成员列表版本号:客户端/其他服务用于成员缓存失效校验
func (l *GetMemberVersionLogic) GetMemberVersion(in *group.GetMemberVersionReq) (*group.GetMemberVersionResp, error) {
	g, err := l.svcCtx.GroupsModel.FindOne(l.ctx, in.GroupId)
	if err != nil {
		return nil, err
	}
	return &group.GetMemberVersionResp{
		MemberVersion: g.MemberVersion,
		MemberCount:   int32(g.MemberCount),
	}, nil
}
