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

type UpdateProfileLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewUpdateProfileLogic(ctx context.Context, svcCtx *svc.ServiceContext) *UpdateProfileLogic {
	return &UpdateProfileLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

// UpdateProfile 只能改本人资料:user_id 由 ctx 注入,不接受请求体指定
func (l *UpdateProfileLogic) UpdateProfile(req *types.UpdateProfileReq) (*types.SuccessResp, error) {
	uid, ok := middleware.GetUserID(l.ctx)
	if !ok || uid <= 0 {
		return nil, fmt.Errorf("unauthorized")
	}
	resp, err := l.svcCtx.User.UpdateProfile(l.ctx, &user.UpdateProfileReq{
		UserId:    uid,
		Nickname:  req.Nickname,
		Avatar:    req.Avatar,
		Signature: req.Signature,
		Gender:    req.Gender,
		Region:    req.Region,
		Birthday:  req.Birthday,
	})
	if err != nil {
		return nil, err
	}
	return &types.SuccessResp{Success: resp.Success}, nil
}
