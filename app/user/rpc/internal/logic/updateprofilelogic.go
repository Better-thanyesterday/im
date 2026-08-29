package logic

import (
	"context"
	"strconv"

	"im-platform/app/user/rpc/internal/svc"
	"im-platform/app/user/rpc/models"
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
	gender:=strconv.FormatInt(int64(in.Gender),10)
	err:=l.svcCtx.UsersModel.Update(l.ctx,&models.Users{
		Avatar: in.Avatar,
		Gender: gender,
		Region: in.Region,
		Signature: in.Signature,
		// Birthday: time.Unix(in.Birthday),
	})
	if err!=nil {
		logx.Errorf("update Profile fail")
		return &user.UpdateProfileResp{
		Success: false,
	}, nil
	}
	return &user.UpdateProfileResp{
		Success: true,
	}, nil
}
