package logic

import (
	"context"
	"fmt"
	"time"

	"im-platform/app/file/rpc/file"
	"im-platform/app/file/rpc/internal/svc"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
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

// 默认预签名有效期：15 分钟
const defaultUrlExpirySecs = 900

// GeneratePresignedUrl 重新签发下载预签名 URL（客户端 URL 过期时刷新）
func (l *GeneratePresignedUrlLogic) GeneratePresignedUrl(in *file.GeneratePresignedUrlReq) (*file.GeneratePresignedUrlResp, error) {
	if in.FileId == "" {
		return nil, fmt.Errorf("file_id required")
	}
	expiry := int64(in.Expiry)
	if expiry <= 0 {
		expiry = defaultUrlExpirySecs
	}
	if expiry > 7*24*3600 {
		return nil, fmt.Errorf("expiry too large (max 7d)") // S3 预签名上限 7 天
	}

	meta, err := l.svcCtx.FilesModel.FindOne(l.ctx, in.FileId)
	if err != nil {
		return nil, fmt.Errorf("file not found: %w", err)
	}
	// 已删除/被拒绝的文件不签发下载 URL
	if meta.Status == 4 || meta.Status == 3 {
		return nil, fmt.Errorf("file not accessible, status=%d", meta.Status)
	}

	po, err := l.svcCtx.Presign.PresignGetObject(l.ctx, &s3.GetObjectInput{
		Bucket: aws.String(meta.Bucket),
		Key:    aws.String(meta.ObjectKey),
	}, s3.WithPresignExpires(time.Duration(expiry)*time.Second))
	if err != nil {
		return nil, fmt.Errorf("presign get failed: %w", err)
	}

	return &file.GeneratePresignedUrlResp{
		FileId:       in.FileId,
		PresignedUrl: po.URL,
		ExpiresAt:    time.Now().Add(time.Duration(expiry) * time.Second).UnixMilli(),
	}, nil
}
