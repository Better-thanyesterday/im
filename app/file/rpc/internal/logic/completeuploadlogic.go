package logic

import (
	"context"
	"fmt"

	"im-platform/app/file/rpc/file"
	"im-platform/app/file/rpc/internal/svc"
	"im-platform/app/file/rpc/models"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
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

// CompleteUpload 合并分片（S3 CompleteMultipartUpload）、状态流转。
// in.Etags：客户端上传每片后从响应 ETag 头取到的值，按 ChunkIndex 顺序排列，
// ETag 原样回传（含引号），与 partNumber=i+1 一一对应。
func (l *CompleteUploadLogic) CompleteUpload(in *file.CompleteUploadReq) (*file.CompleteUploadResp, error) {
	if in.FileId == "" || in.UploadId == "" {
		return nil, fmt.Errorf("invalid complete params")
	}
	if len(in.Etags) == 0 {
		return nil, fmt.Errorf("etags required")
	}

	// 1. 查元数据，文件必须存在且未完成/未删除
	meta, err := l.svcCtx.FilesModel.FindOne(l.ctx, in.FileId)
	if err != nil {
		return nil, fmt.Errorf("file not found: %w", err)
	}
	if meta.Status == models.FileStatusNormal {
		return &file.CompleteUploadResp{FileId: in.FileId, Status: 1}, nil // 幂等：已完成直接返回
	}
	if meta.Status == models.FileStatusDeleted {
		return nil, fmt.Errorf("file already deleted")
	}

	// 2. 按 partNumber=i+1 与 ETag 组装，请求 CompleteMultipartUpload
	parts := make([]types.CompletedPart, 0, len(in.Etags))
	for i, etag := range in.Etags {
		parts = append(parts, types.CompletedPart{
			PartNumber: aws.Int32(int32(i + 1)),
			ETag:       aws.String(etag),
		})
	}
	if _, err := l.svcCtx.S3.CompleteMultipartUpload(l.ctx, &s3.CompleteMultipartUploadInput{
		Bucket:          aws.String(meta.Bucket),
		Key:             aws.String(meta.ObjectKey),
		UploadId:        aws.String(in.UploadId),
		MultipartUpload: &types.CompletedMultipartUpload{Parts: parts},
	}); err != nil {
		return nil, fmt.Errorf("complete multipart upload failed: %w", err)
	}

	// 3. 状态流转：审核中 → 正常（内容审核后续可接 im.file.audit topic 异步处理）
	if err := l.svcCtx.FilesModel.UpdateStatus(l.ctx, in.FileId, models.FileStatusNormal); err != nil {
		return nil, fmt.Errorf("update status failed: %w", err)
	}

	return &file.CompleteUploadResp{
		FileId: in.FileId,
		Url:    fmt.Sprintf("/%s/%s", meta.Bucket, meta.ObjectKey),
		Status: 1, // 1-上传成功待审核
	}, nil
}
