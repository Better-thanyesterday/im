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
	// 批量插入(去重),替代循环单条 INSERT;按实际插入数原子累加成员数
	now := time.Now()
	seen := make(map[int64]bool, len(in.UserIds))
	rows := make([]*models.Groupmembers, 0, len(in.UserIds))
	for _, uid := range in.UserIds {
		if uid <= 0 || seen[uid] {
			continue
		}
		seen[uid] = true
		rows = append(rows, &models.Groupmembers{
			Id:         l.svcCtx.Snowflake.NextID(),
			GroupId:    in.GroupId,
			Role:       3,
			UserId:     uid,
			LastAckSeq: 0,
			JoinTime:   now,
			GroupNickname: sql.NullString{
				String: g.Name,
				Valid:  g.Name != "",
			},
		})
	}
	inserted, err := l.svcCtx.GroupMembersModel.BatchInsertIgnore(l.ctx, rows)
	if err != nil {
		return nil, err
	}
	insertedSet := make(map[int64]bool, len(inserted))
	for _, uid := range inserted {
		insertedSet[uid] = true
	}
	var fail []int64
	for _, uid := range in.UserIds {
		if !insertedSet[uid] {
			fail = append(fail, uid)
		}
	}
	// 成功人数用单条 SQL 原子累加,同时递增 member_version 使成员缓存失效;
	// 原实现读-改-写 member_count 且走全列覆盖 Update(把群名/群主/公告全部清零)
	if success := int64(len(inserted)); success > 0 {
		if err := l.svcCtx.GroupsModel.IncrMembers(l.ctx, in.GroupId, success); err != nil {
			logx.Errorf("incr member count failed: group=%d delta=%d err=%v", in.GroupId, success, err)
		}
	}
	return &group.InviteMemberResp{
		FailedUserIds: fail,
	}, nil
}
