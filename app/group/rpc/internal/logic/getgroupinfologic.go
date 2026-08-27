package logic

import (
	"context"

	"im-platform/app/group/rpc/internal/svc"
	"im-platform/app/group/rpc/group"
	"github.com/zeromicro/go-zero/core/logx"
)

type GetGroupInfoLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewGetGroupInfoLogic(ctx context.Context, svcCtx *svc.ServiceContext) *GetGroupInfoLogic {
	return &GetGroupInfoLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}
//数据：群名称、群头像、群主 ID (owner_id)、当前人数 (member_count)、成员版本号 (member_version)、群公告、群设置（是否免审批）等。
func (l *GetGroupInfoLogic) GetGroupInfo(in *group.GetGroupInfoReq) (*group.GetGroupInfoResp, error) {
	// todo: add your logic here and delete this line

	return &group.GetGroupInfoResp{}, nil
}
