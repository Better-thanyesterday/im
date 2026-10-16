package logic

import (
	"context"

	"im-platform/app/user/rpc/internal/svc"
	"im-platform/app/user/rpc/user"

	"github.com/zeromicro/go-zero/core/logx"
)

type GetFriendAppliesLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewGetFriendAppliesLogic(ctx context.Context, svcCtx *svc.ServiceContext) *GetFriendAppliesLogic {
	return &GetFriendAppliesLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *GetFriendAppliesLogic) GetFriendApplies(in *user.GetFriendAppliesReq) (*user.GetFriendAppliesResp, error) {
	// status=0(未指定)按"待处理"处理:SQL 是 status = $2 精确匹配,
	// 传 0 会恒返回空,而前端默认就不带这个参数
	status := in.Status
	if status == 0 {
		status = 1 // 1=pending
	}
	rows, err := l.svcCtx.FriendAppliesModel.FindByTargetWithUserInfo(
		l.ctx,
		in.UserId,
		status,
		in.Page,
		in.PageSize,
	)
	if err != nil {
		return nil, err
	}
	applies := make([]*user.FriendApplyInfo, 0, len(rows))
	for _, row := range rows {
		applies = append(applies, &user.FriendApplyInfo{
			Id:                row.Id,
			ApplicantId:       row.ApplicantId,
			ApplicantNickname: row.ApplicantNickname,
			ApplicantAvatar:   row.ApplicantAvatar,
			TargetId:          row.TargetId,
			ApplyReason:       row.ApplyReason,
			Status:            row.Status,
			CreatedAt:         row.CreatedAt,
			HandledAt:         row.HandledAt,
		})
	}
	return &user.GetFriendAppliesResp{
		Applies: applies,
		Total:   int32(len(rows)),
	}, nil
}
