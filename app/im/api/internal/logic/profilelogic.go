// Code scaffolded by goctl. Safe to edit.
// goctl 1.10.1

package logic

import (
	"context"
	"fmt"

	"im-platform/app/im/api/internal/svc"
	"im-platform/app/im/api/internal/types"
	"im-platform/app/user/rpc/user"
	"im-platform/common/middleware"

	"github.com/zeromicro/go-zero/core/logx"
)

type ProfileLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewProfileLogic(ctx context.Context, svcCtx *svc.ServiceContext) *ProfileLogic {
	return &ProfileLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

// GetProfile 查资料:viewer_id 恒为本人,只有本人能拿到 phone/email(隐私由 user rpc 控制)
func (l *ProfileLogic) GetProfile(req *types.ProfileReq) (*types.ProfileResp, error) {
	uid, ok := middleware.GetUserID(l.ctx)
	if !ok || uid <= 0 {
		return nil, fmt.Errorf("unauthorized")
	}
	p, err := l.svcCtx.User.GetProfile(l.ctx, &user.GetProfileReq{
		UserId:   req.UserId, // 0=查本人
		ViewerId: uid,
	})
	if err != nil {
		return nil, err
	}
	return &types.ProfileResp{
		UserId:    p.UserId,
		Phone:     p.Phone,
		Email:     p.Email,
		Nickname:  p.Nickname,
		Avatar:    p.Avatar,
		Signature: p.Signature,
		Gender:    p.Gender,
		Region:    p.Region,
		Birthday:  p.Birthday,
		Status:    p.Status,
		CreatedAt: p.CreatedAt,
		UpdatedAt: p.UpdatedAt,
	}, nil
}
