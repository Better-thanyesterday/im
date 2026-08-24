package logic

import (
	"context"

	"im-platform/app/media/rpc/internal/svc"
	"im-platform/app/media/rpc/media"

	"github.com/zeromicro/go-zero/core/logx"
)

type GeneratePresignedUrlLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewGeneratePresignedUrlLogic(ctx context.Context, svcCtx *svc.ServiceContext) *GeneratePresignedUrlLogic {
	return &GeneratePresignedUrlLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

// 重新签发下载预签名 URL（客户端 URL 过期时刷新）
func (l *GeneratePresignedUrlLogic) GeneratePresignedUrl(in *media.GeneratePresignedUrlReq) (*media.GeneratePresignedUrlResp, error) {
	// todo: add your logic here and delete this line

	return &media.GeneratePresignedUrlResp{}, nil
}
