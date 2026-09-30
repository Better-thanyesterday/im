package logic

import (
	"context"
	"database/sql"
	"time"

	"im-platform/app/group/rpc/group"
	"im-platform/app/group/rpc/internal/svc"
	"im-platform/app/group/rpc/models"
	"im-platform/common/constants"

	"github.com/zeromicro/go-zero/core/logx"
)

type InviteMemberLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewInviteMemberLogic(ctx context.Context, svcCtx *svc.ServiceContext) *InviteMemberLogic {
	return &InviteMemberLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

// ==================== 成员管理 ====================
func (l *InviteMemberLogic) InviteMember(in *group.InviteMemberReq) (*group.InviteMemberResp, error) {
	// todo: add your logic here and delete this line
	if in.GroupId <= 0 || len(in.UserIds) == 0 {
		return nil, constants.NewMsgError(constants.ErrCodeMsgInValidParam)
	}
	// 群不存在直接报错(原实现忽略 err,后面 g.Name 空指针 panic)
	g, err := l.svcCtx.GroupsModel.FindOne(l.ctx, in.GroupId)
	if err != nil {
		return nil, err
	}
	var fail []int64
	for _, uid := range in.UserIds {
		_, err := l.svcCtx.GroupMembersModel.Insert(l.ctx, &models.Groupmembers{
			Id:         l.svcCtx.Snokflake.NextID(),
			GroupId:    in.GroupId,
			Role:       3,
			UserId:     uid,
			LastAckSeq: 0,
			JoinTime:   time.Now(),
			GroupNickname: sql.NullString{
				String: g.Name,
				Valid:  g.Name != "",
			},
		})
		if err != nil {
			fail = append(fail, uid)
		}
	}
	// 成功人数用单条 SQL 原子累加,同时递增 member_version 使成员缓存失效;
	// 原实现读-改-写 member_count 且走全列覆盖 Update(把群名/群主/公告全部清零)
	if success := int64(len(in.UserIds) - len(fail)); success > 0 {
		if err := l.svcCtx.GroupsModel.IncrMembers(l.ctx, in.GroupId, success); err != nil {
			logx.Errorf("incr member count failed: group=%d delta=%d err=%v", in.GroupId, success, err)
		}
	}
	return &group.InviteMemberResp{
		FailedUserIds: fail,
	}, nil
}
