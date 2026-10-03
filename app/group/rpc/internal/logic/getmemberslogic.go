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

// 成员 ID、角色、群昵称、入群时间等
// ==================== 查询接口（供 Message 服务调用） ====================
func (l *GetMembersLogic) GetMembers(in *group.GetMembersReq) (*group.GetMembersResp, error) {
	// todo: add your logic here and delete this line
	// pageSize clamp:防止调用方传超大值一次拉全表
	pageSize := int64(in.PageSize)
	if pageSize <= 0 {
		pageSize = 200
	}
	if pageSize > 500 {
		pageSize = 500
	}
	memberInfos, cur, err := l.svcCtx.GroupMembersModel.GetBatchMemberInfo(l.ctx, in.GroupId, in.LastId, pageSize)
	hasmore := true
	if err != nil {
		return nil, err
	}
	if len(memberInfos) < int(pageSize) {
		hasmore = false
	}
	return &group.GetMembersResp{
		Members: memberInfos,
		HasMore: hasmore,
		LastId:  cur,
	}, nil
}
