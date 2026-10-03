package logic

import (
	"context"
	"strconv"
	"time"

	"im-platform/app/user/rpc/internal/svc"
	"im-platform/app/user/rpc/user"

	"github.com/zeromicro/go-zero/core/logx"
)

type UpdateProfileLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewUpdateProfileLogic(ctx context.Context, svcCtx *svc.ServiceContext) *UpdateProfileLogic {
	return &UpdateProfileLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *UpdateProfileLogic) UpdateProfile(in *user.UpdateProfileReq) (*user.UpdateProfileResp, error) {
	// todo: add your logic here and delete this line
	gender := strconv.FormatInt(int64(in.Gender), 10)
	// 定向更新资料列 + 显式 user_id,避免 where id=0 静默 no-op 假成功
	err := l.svcCtx.UsersModel.UpdateProfileFields(l.ctx, in.UserId,
		in.Nickname, in.Avatar, in.Signature, gender, in.Region,
		time.Unix(in.Birthday, 0),
	)
	if err != nil {
		logx.Errorf("update Profile fail: user=%d err=%v", in.UserId, err)
		return &user.UpdateProfileResp{
			Success: false,
		}, nil
	}
	return &user.UpdateProfileResp{
		Success: true,
	}, nil
}
