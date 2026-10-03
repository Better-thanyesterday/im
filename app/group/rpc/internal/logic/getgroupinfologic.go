package logic

import (
	"context"

	"github.com/zeromicro/go-zero/core/logx"
	"im-platform/app/group/rpc/group"
	"im-platform/app/group/rpc/internal/svc"
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

// 数据：群名称、群头像、群主 ID (owner_id)、当前人数 (member_count)、成员版本号 (member_version)、群公告、群设置（是否免审批）等。
func (l *GetGroupInfoLogic) GetGroupInfo(in *group.GetGroupInfoReq) (*group.GetGroupInfoResp, error) {
	// todo: add your logic here and delete this line
	g, err := l.svcCtx.GroupsModel.FindOne(l.ctx, in.GroupId)
	if err != nil {
		return nil, err
	}
	return &group.GetGroupInfoResp{
		Info: &group.GroupInfo{
			Id:               g.Id,
			Status:           int32(g.Status),
			Name:             g.Name,
			Avatar:           g.Avatar.String,
			Notice:           g.Notice.String,
			MemberCount:      int32(g.MemberCount),
			MaxMember:        int32(g.MaxMember),
			JoinApproval:     int32(g.JoinApproval),
			InvitePermission: int32(g.InvitePermission),
			GroupType:        int32(g.GroupType),
			MemberVersion:    g.MemberVersion,
		},
	}, nil
}
