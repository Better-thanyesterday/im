package logic

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"im-platform/app/group/rpc/group"
	"im-platform/app/group/rpc/internal/svc"
	"im-platform/app/group/rpc/models"

	"github.com/zeromicro/go-zero/core/logx"
)

type CreateGroupLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewCreateGroupLogic(ctx context.Context, svcCtx *svc.ServiceContext) *CreateGroupLogic {
	return &CreateGroupLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

// ==================== 群生命周期 ====================
func (l *CreateGroupLogic) CreateGroup(in *group.CreateGroupReq) (*group.CreateGroupResp, error) {
	// todo: add your logic here and delete this line
	if in.CreatorId <= 0 || in.Name == "" {
		return nil, fmt.Errorf("invalid create group req: creator=%d name=%q", in.CreatorId, in.Name)
	}
	maxMember := int64(in.MaxMember)
	if maxMember <= 0 {
		maxMember = 500
	}
	// 初始成员去重并排除创建者(创建者单独作为群主插入)
	seen := map[int64]bool{in.CreatorId: true}
	members := make([]*models.Groupmembers, 0, len(in.InitialMembers))
	for _, uid := range in.InitialMembers {
		if uid <= 0 || seen[uid] {
			continue
		}
		seen[uid] = true
		members = append(members, &models.Groupmembers{
			Id:       l.svcCtx.Snowflake.NextID(),
			Role:     3,
			UserId:   uid,
			JoinTime: time.Now(),
			GroupNickname: sql.NullString{
				String: in.Name,
				Valid:  in.Name != "",
			},
		})
	}
	groupInf := &models.Groups{
		Id:   l.svcCtx.Snowflake.NextID(),
		Name: in.Name,
		Avatar: sql.NullString{
			String: in.Avatar,
			Valid:  in.Avatar != "",
		},
		MemberCount:      int64(1 + len(members)),
		MemberVersion:    1,
		MaxMember:        maxMember,
		GroupType:        int64(in.GroupType),
		Status:           1,
		InvitePermission: int64(in.InvitePermission),
		OwnerId:          in.CreatorId,
		JoinApproval:     int64(in.JoinApproval),
	}
	for _, mem := range members {
		mem.GroupId = groupInf.Id
	}
	owner := &models.Groupmembers{
		Id:      l.svcCtx.Snowflake.NextID(),
		GroupId: groupInf.Id,
		UserId:  in.CreatorId,
		Role:    1,
		GroupNickname: sql.NullString{
			String: in.Name,
			Valid:  in.Name != "",
		},
		JoinTime: time.Now(),
	}
	// 事务写库:群 + 群主 + 初始成员,任一步失败整体回滚,不再留孤儿群
	if err := l.svcCtx.GroupsModel.CreateGroupTx(l.ctx, groupInf, append([]*models.Groupmembers{owner}, members...)); err != nil {
		logx.Errorf("create group failed: %v", err)
		return nil, err
	}
	return &group.CreateGroupResp{
		MemberVersion: groupInf.MemberVersion,
		GroupId:       groupInf.Id,
	}, nil
}
