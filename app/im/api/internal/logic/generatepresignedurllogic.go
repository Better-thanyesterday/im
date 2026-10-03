// Code scaffolded by goctl. Safe to edit.
// goctl 1.10.1

package logic

import (
	"context"

	"im-platform/app/file/rpc/file"
	"im-platform/app/im/api/internal/svc"
	"im-platform/app/im/api/internal/types"

	"github.com/zeromicro/go-zero/core/logx"
)

const defaultPresignExpiry = 900 // 15 分钟

type GeneratePresignedUrlLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewGeneratePresignedUrlLogic(ctx context.Context, svcCtx *svc.ServiceContext) *GeneratePresignedUrlLogic {
	return &GeneratePresignedUrlLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

// GeneratePresignedUrl 重签下载 URL:URL 过期时客户端刷新用
func (l *GeneratePresignedUrlLogic) GeneratePresignedUrl(req *types.GeneratePresignedUrlReq) (*types.GeneratePresignedUrlResp, error) {
	expiry := req.Expiry
	if expiry <= 0 {
		expiry = defaultPresignExpiry
	}
	resp, err := l.svcCtx.File.GeneratePresignedUrl(l.ctx, &file.GeneratePresignedUrlReq{
		FileId: req.FileId,
		Expiry: expiry,
	})
	if err != nil {
		return nil, err
	}
	return &types.GeneratePresignedUrlResp{
		FileId:       resp.FileId,
		PresignedUrl: resp.PresignedUrl,
		ExpiresAt:    resp.ExpiresAt,
	}, nil
}
