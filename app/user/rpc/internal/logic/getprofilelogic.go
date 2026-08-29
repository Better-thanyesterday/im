package logic

import (
	"context"
	"strconv"

	"im-platform/app/user/rpc/internal/svc"
	"im-platform/app/user/rpc/user"

	"github.com/zeromicro/go-zero/core/logx"
)

type GetProfileLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewGetProfileLogic(ctx context.Context, svcCtx *svc.ServiceContext) *GetProfileLogic {
	return &GetProfileLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

// 用户资料
func (l *GetProfileLogic) GetProfile(in *user.GetProfileReq) (*user.Profile, error) {
	// todo: add your logic here and delete this line
	u,err:=l.svcCtx.UsersModel.FindOne(l.ctx,in.UserId)
	if err!=nil {
		logx.Errorf("Get Profile fail")
		return nil,err
	}
	gender,_:=strconv.ParseInt(u.Gender,10,64)
	return &user.Profile{
		UserId: u.Id,
		Avatar: u.Avatar,
		Email: u.Email,
		Gender: int32(gender),
		Region: u.Region,
		Signature: u.Signature,
		Birthday: u.Birthday.Unix(),
		Phone: u.Phone,
		Nickname: u.Nickname,
		Status: int32(u.Status.Int64),
	}, nil
}
