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

type QuitGroupLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewQuitGroupLogic(ctx context.Context, svcCtx *svc.ServiceContext) *QuitGroupLogic {
	return &QuitGroupLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

// QuitGroup 退群:退出者身份以 token 为准
func (l *QuitGroupLogic) QuitGroup(req *types.QuitGroupReq) (*types.GroupOpResp, error) {
	uid, ok := middleware.GetUserID(l.ctx)
	if !ok || uid <= 0 {
		return nil, fmt.Errorf("unauthorized")
	}
	resp, err := l.svcCtx.Group.QuitGroup(l.ctx, &group.QuitGroupReq{
		GroupId: req.GroupId,
		UserId:  uid,
	})
	if err != nil {
		return nil, err
	}
	return &types.GroupOpResp{MemberVersion: resp.MemberVersion}, nil
}
