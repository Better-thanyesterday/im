// Code scaffolded by goctl. Safe to edit.
// goctl 1.10.1

package logic

import (
	"context"
	"fmt"

	"im-platform/app/group/rpc/group"
	"im-platform/app/im/api/internal/svc"
	"im-platform/app/im/api/internal/types"
	"im-platform/common/middleware"

	"github.com/zeromicro/go-zero/core/logx"
)

type HandleApplyLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewHandleApplyLogic(ctx context.Context, svcCtx *svc.ServiceContext) *HandleApplyLogic {
	return &HandleApplyLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

// HandleApply 审批入群申请:审批人身份以 token 为准
func (l *HandleApplyLogic) HandleApply(req *types.HandleApplyReq) (*types.GroupOpResp, error) {
	uid, ok := middleware.GetUserID(l.ctx)
	if !ok || uid <= 0 {
		return nil, fmt.Errorf("unauthorized")
	}
	resp, err := l.svcCtx.Group.HandleApply(l.ctx, &group.HandleApplyReq{
		// group_id 由 rpc 侧按 apply 归属校验,这里只需传审批人与申请
		GroupId:    0,
		OperatorId: uid,
		ApplyId:    req.ApplyId,
		Approve:    req.Approve,
	})
	if err != nil {
		return nil, err
	}
	return &types.GroupOpResp{MemberVersion: resp.MemberVersion}, nil
}
