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
	groupInf := &models.Groups{
		Id:   l.svcCtx.Snokflake.NextID(),
		Name: in.Name,
		Avatar: sql.NullString{
			String: in.Avatar,
			Valid:  in.Avatar != "",
		},
		MemberCount:      1,
		MemberVersion:    1,
		GroupType:        1,
		Status:           1,
		InvitePermission: int64(in.InvitePermission),
		OwnerId:          in.CreatorId,
		JoinApproval:     int64(in.JoinApproval),
	}
	_, err := l.svcCtx.GroupsModel.Insert(l.ctx, groupInf)
	if err != nil {
		logx.Errorf("create group fail:%v",err)
		return nil, err
	}
	gmemberInf:=&models.Groupmembers{
		Id: l.svcCtx.Snokflake.NextID(),
		GroupId: groupInf.Id,
		Role: 1,
		GroupNickname: sql.NullString{
			String: in.Name,
			Valid:  in.Name != "",
		},
		JoinTime: time.Now(),
	}
	_,err=l.svcCtx.GroupMembersModel.Insert(l.ctx,gmemberInf)
	if err != nil {
		logx.Errorf("create groupmember fail:%v",err)
		return nil, err
	}
	return &group.CreateGroupResp{
		MemberVersion: 1,
		GroupId:       groupInf.Id,
	}, nil
}
