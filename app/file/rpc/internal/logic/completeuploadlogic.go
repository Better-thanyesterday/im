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

// CompleteUpload 完成上传:multipart 路径做 S3 CompleteMultipartUpload 合并,
// 单片快传路径(UploadId 为空,客户端 PUT 已直传)直接状态流转。
// in.Etags:分片路径下客户端按 ChunkIndex 顺序回传的 ETag(含引号),与 partNumber=i+1 对应。
func (l *CompleteUploadLogic) CompleteUpload(in *file.CompleteUploadReq) (*file.CompleteUploadResp, error) {
	if in.FileId == "" || in.UploaderId <= 0 {
		return nil, fmt.Errorf("invalid complete params")
	}
	// 1. 查元数据,校验归属——防止合并/完成他人的上传任务
	meta, err := l.svcCtx.FilesModel.FindOne(l.ctx, in.FileId)
	if err != nil {
		return nil, fmt.Errorf("file not found: %w", err)
	}
	if meta.UploaderId != in.UploaderId {
		return nil, fmt.Errorf("permission denied: file %s belongs to %d", in.FileId, meta.UploaderId)
	}
	if meta.Status == models.FileStatusNormal {
		return &file.CompleteUploadResp{FileId: in.FileId, Status: 1}, nil // 幂等：已完成直接返回
	}
	if meta.Status == models.FileStatusDeleted {
		return nil, fmt.Errorf("file already deleted")
	}

	// 2a. 单片快传:UploadId 为空,对象已由客户端 PUT 直传,跳过 S3 合并
	if in.UploadId == "" {
		if err := l.svcCtx.FilesModel.UpdateStatus(l.ctx, in.FileId, models.FileStatusNormal); err != nil {
			return nil, fmt.Errorf("update status failed: %w", err)
		}
		return &file.CompleteUploadResp{
			FileId: in.FileId,
			Url:    fmt.Sprintf("/%s/%s", meta.Bucket, meta.ObjectKey),
			Status: 1,
		}, nil
	}

	// 2b. 分片路径:必须有 ETags 且与请求一起做 S3 合并
	if len(in.Etags) == 0 {
		return nil, fmt.Errorf("etags required")
	}
	if len(in.Etags) > maxTotalChunks {
		return nil, fmt.Errorf("too many etags")
	}
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
