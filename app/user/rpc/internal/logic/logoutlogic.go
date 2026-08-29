package logic

import (
	"context"
	"database/sql"

	"im-platform/app/user/rpc/internal/svc"
	"im-platform/app/user/rpc/models"
	"im-platform/app/user/rpc/user"

	"github.com/zeromicro/go-zero/core/logx"
)

type LogoutLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewLogoutLogic(ctx context.Context, svcCtx *svc.ServiceContext) *LogoutLogic {
	return &LogoutLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *LogoutLogic) Logout(in *user.LogoutReq) (*user.LogoutResp, error) {
	// todo: add your logic here and delete this line
	err:=l.svcCtx.UsersModel.Update(l.ctx,&models.Users{
		Status: sql.NullInt64{
			Int64: 0,
			Valid: true,
		},
	})
	if err!=nil {
		return &user.LogoutResp{
			Success: false,
	}, err
	}
	return &user.LogoutResp{
		Success: true,
	}, nil
}
