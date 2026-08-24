package logic

import (
	"context"

	"im-platform/app/media/rpc/internal/svc"
	"im-platform/app/media/rpc/media"

	"github.com/zeromicro/go-zero/core/logx"
)

type CompleteUploadLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewCompleteUploadLogic(ctx context.Context, svcCtx *svc.ServiceContext) *CompleteUploadLogic {
	return &CompleteUploadLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

// 客户端所有分片上传完成后调用，触发合并、格式校验、安全审核
func (l *CompleteUploadLogic) CompleteUpload(in *media.CompleteUploadReq) (*media.CompleteUploadResp, error) {
	// todo: add your logic here and delete this line

	return &media.CompleteUploadResp{}, nil
}
