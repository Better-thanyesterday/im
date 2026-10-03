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
)

type HandleApplyLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewHandleApplyLogic(ctx context.Context, svcCtx *svc.ServiceContext) *HandleApplyLogic {
	return &HandleApplyLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

// HandleApply 审批入群申请:群主/管理员可处理;群归属从申请行反查(调用方可只传 apply_id,
// group_id 仅作防呆比对);接受时入群(JoinGroupTx 内含计数+版本),拒绝时仅改申请状态
func (l *HandleApplyLogic) HandleApply(in *group.HandleApplyReq) (*group.HandleApplyResp, error) {
	if in.ApplyId <= 0 || in.OperatorId <= 0 {
		return nil, constants.NewMsgError(constants.ErrCodeMsgInValidParam)
	}
	apply, err := l.svcCtx.GroupAppliesModel.FindOne(l.ctx, in.ApplyId)
	if err != nil {
		return nil, err
	}
	// 防呆:调用方若带 group_id 必须与申请归属一致
	if in.GroupId != 0 && in.GroupId != apply.GroupId {
		return nil, errors.New("申请与该群不匹配")
	}
	g, err := fetchGroup(l.ctx, l.svcCtx, apply.GroupId)
	if err != nil {
		return nil, err
	}
	operator, err := fetchMember(l.ctx, l.svcCtx, apply.GroupId, in.OperatorId)
	if err != nil {
		return nil, err
	}
	if err := requirePrivilege(operator, "审批入群申请"); err != nil {
		return nil, err
	}
	if apply.Status != int64(group.ApplyStatus_PENDING) {
		return nil, errors.New("该申请已处理过")
	}

	targetStatus := int64(group.ApplyStatus_REJECTED)
	if in.Approve {
		targetStatus = int64(group.ApplyStatus_ACCEPTED)
		// 先入群再标记:入群幂等(已是成员则跳过),标记失败时可重复审批不产生副作用
		member := &models.Groupmembers{
			Id:            l.svcCtx.Snowflake.NextID(),
			GroupId:       in.GroupId,
			UserId:        apply.ApplicantId,
			Role:          int64(group.GroupRole_MEMBER),
			JoinTime:      time.Now(),
			GroupNickname: groupNicknameOf(g.Name),
		}
		inserted, err := l.svcCtx.GroupsModel.JoinGroupTx(l.ctx, in.GroupId, member)
		if err != nil {
			return nil, err
		}
		if !inserted {
			// 审批期间已通过邀请等途径入群:只标记申请,版本号不再变
			if err := l.svcCtx.GroupAppliesModel.HandleApply(l.ctx, in.ApplyId, in.OperatorId, targetStatus); err != nil {
				return nil, err
			}
			return &group.HandleApplyResp{MemberVersion: g.MemberVersion}, nil
		}
	}
	if err := l.svcCtx.GroupAppliesModel.HandleApply(l.ctx, in.ApplyId, in.OperatorId, targetStatus); err != nil {
		return nil, err
	}
	version := g.MemberVersion
	if in.Approve {
		version = g.MemberVersion + 1
	}
	return &group.HandleApplyResp{MemberVersion: version}, nil
}
