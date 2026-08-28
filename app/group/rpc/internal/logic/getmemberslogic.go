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
//成员 ID、角色、群昵称、入群时间等
// ==================== 查询接口（供 Message 服务调用） ====================
func (l *GetMembersLogic) GetMembers(in *group.GetMembersReq) (*group.GetMembersResp, error) {
	// todo: add your logic here and delete this line
	memberInfos,cur,err :=l.svcCtx.GroupMembersModel.GetBatchMemberInfo(l.ctx,in.GroupId,in.LastId,int64(in.PageSize))
	hasmore:=true
	if err != nil {
		return nil,err
	}
	if len(memberInfos)<int(in.PageSize) {
		hasmore=false
	}
	return &group.GetMembersResp{
		Members: memberInfos,
		HasMore: hasmore,
		LastId: cur,
	}, nil
}
