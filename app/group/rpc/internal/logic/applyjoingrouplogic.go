package logic

import (
	"context"
	"errors"
	"time"

	"im-platform/app/group/rpc/group"
	"im-platform/app/group/rpc/internal/svc"
	"im-platform/app/group/rpc/models"
	"im-platform/common/constants"

	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/core/stores/sqlx"
)

type ApplyJoinGroupLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewApplyJoinGroupLogic(ctx context.Context, svcCtx *svc.ServiceContext) *ApplyJoinGroupLogic {
	return &ApplyJoinGroupLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

// ApplyJoinGroup 入群申请:join_approval=1(免审批)直接入群;
// 需审批时落 groupapplies(重复待处理申请幂等返回原申请)
func (l *ApplyJoinGroupLogic) ApplyJoinGroup(in *group.ApplyJoinGroupReq) (*group.ApplyJoinGroupResp, error) {
	if in.GroupId <= 0 || in.ApplicantId <= 0 {
		return nil, constants.NewMsgError(constants.ErrCodeMsgInValidParam)
	}
	g, err := fetchGroup(l.ctx, l.svcCtx, in.GroupId)
	if err != nil {
		return nil, err
	}
	exists, err := l.svcCtx.GroupMembersModel.CheckExist(l.ctx, in.GroupId, in.ApplicantId)
	if err != nil {
		return nil, err
	}
	if exists {
		return nil, errors.New("已经是群成员")
	}
	if g.MaxMember > 0 && g.MemberCount >= g.MaxMember {
		return nil, errors.New("群成员数已达上限")
	}

	// 免审批:直接入群
	if g.JoinApproval == 1 {
		member := &models.Groupmembers{
			Id:            l.svcCtx.Snowflake.NextID(),
			GroupId:       in.GroupId,
			UserId:        in.ApplicantId,
			Role:          int64(group.GroupRole_MEMBER),
			JoinTime:      time.Now(),
			GroupNickname: groupNicknameOf(g.Name),
		}
		inserted, err := l.svcCtx.GroupsModel.JoinGroupTx(l.ctx, in.GroupId, member)
		if err != nil {
			return nil, err
		}
		if !inserted {
			return nil, errors.New("已经是群成员")
		}
		return &group.ApplyJoinGroupResp{ApplyId: 0, Status: 2}, nil
	}

	// 需审批:重复待处理申请幂等返回原申请,避免刷出多条
	existing, err := l.svcCtx.GroupAppliesModel.FindOneByGroupIdApplicantId(l.ctx, in.GroupId, in.ApplicantId)
	if err != nil && !errors.Is(err, sqlx.ErrNotFound) {
		return nil, err
	}
	if existing != nil && existing.Status == int64(group.ApplyStatus_PENDING) {
		return &group.ApplyJoinGroupResp{ApplyId: existing.Id, Status: 1}, nil
	}

	applyId := l.svcCtx.Snowflake.NextID()
	_, err = l.svcCtx.GroupAppliesModel.Insert(l.ctx, &models.Groupapplies{
		Id:          applyId,
		GroupId:     in.GroupId,
		ApplicantId: in.ApplicantId,
		ApplyType:   1, // 1-主动申请
		ApplyReason: sqlNullString(in.ApplyReason),
		Status:      int64(group.ApplyStatus_PENDING),
	})
	if err != nil {
		return nil, err
	}
	return &group.ApplyJoinGroupResp{ApplyId: applyId, Status: 1}, nil
}
