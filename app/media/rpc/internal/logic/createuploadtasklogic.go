package logic

import (
	"context"

	"im-platform/app/media/rpc/internal/svc"
	"im-platform/app/media/rpc/media"

	"github.com/zeromicro/go-zero/core/logx"
)

type CreateUploadTaskLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewCreateUploadTaskLogic(ctx context.Context, svcCtx *svc.ServiceContext) *CreateUploadTaskLogic {
	return &CreateUploadTaskLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

// 初始化分片上传任务，返回 upload_id 与各分片预签名 URL
func (l *CreateUploadTaskLogic) CreateUploadTask(in *media.CreateUploadTaskReq) (*media.CreateUploadTaskResp, error) {
	// todo: add your logic here and delete this line

	return &media.CreateUploadTaskResp{}, nil
}
