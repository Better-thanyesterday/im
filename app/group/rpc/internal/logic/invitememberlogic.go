package logic

import (
	"context"
	"database/sql"
	"time"

	"im-platform/app/group/rpc/group"
	"im-platform/app/group/rpc/internal/svc"
	"im-platform/app/group/rpc/models"

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
	g, _ := l.svcCtx.GroupsModel.FindOne(l.ctx,in.GroupId)
	var fail []int64
	count:=len(in.UserIds) 
	for _, m := range in.UserIds {
		_, err := l.svcCtx.GroupMembersModel.Insert(l.ctx, &models.Groupmembers{
			Id:         l.svcCtx.Snokflake.NextID(),
			GroupId:    in.GroupId,
			Role:       3,
			UserId:     m,
			LastAckSeq: 0,
			JoinTime:   time.Now(),
			GroupNickname: sql.NullString{
				String: g.Name,
				Valid:  g.Name != "",
			},
		})
		if err != nil {
			fail=append(fail, m)
			count--
		}
		
	}
	count=count+int(g.MemberCount)
	err:=l.svcCtx.GroupsModel.Update(l.ctx,&models.Groups{
		Id: in.GroupId,
		MemberCount: int64(count),
	})
	if err != nil {
		logx.Error("membercount update fail")
	}
	return &group.InviteMemberResp{
		FailedUserIds: fail,
	}, nil
}
