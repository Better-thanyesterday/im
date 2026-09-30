package logic

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"im-platform/app/file/rpc/file"
	"im-platform/app/file/rpc/internal/svc"
	"im-platform/app/file/rpc/models"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
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

// 预签名 URL 有效期（UploadPart / PutObject）
const presignPutExpiry = 30 * time.Minute

// CreateUploadTask 初始化分片上传任务，返回 file_id、S3 upload_id 与各分片预签名 UploadPart URL。
// 走 S3 原生 Multipart：预签名 URL 自带 partNumber/uploadId 并参与 SigV4 签名，
// 客户端按 ChunkIndex+1 对应 partNumber 上传，CompleteUpload 时携带各片 ETag 合并。
// 注意：除最后一片外每片 >= 5MiB（S3 Multipart 协议限制）。
func (l *CreateUploadTaskLogic) CreateUploadTask(in *file.CreateUploadTaskReq) (*file.CreateUploadTaskResp, error) {
	// 1. 参数校验
	if in.FileName == "" || in.FileSize <= 0 || in.TotalChunks <= 0 || in.UploaderId <= 0 {
		return nil, fmt.Errorf("invalid upload params")
	}

	// 2. 生成 file_id（雪花，字符串对外）
	fileId := strconv.FormatInt(l.svcCtx.Snowflake.NextID(), 10)

	// 3. 对象键：files/{file_id}/{原始文件名}
	objectKey := fmt.Sprintf("files/%s/%s", fileId, in.FileName)

	// 4. 落库元数据（status=审核中：合并完成前文件不可见）
	err := l.svcCtx.FilesModel.Insert(l.ctx, &models.Files{
		Id:         fileId,
		FileName:   in.FileName,
		FileSize:   in.FileSize,
		MimeType:   in.MimeType,
		FileType:   int64(in.FileType),
		UploaderId: in.UploaderId,
		UploadTime: time.Now(),
		Status:     models.FileStatusAudit,
		ObjectKey:  objectKey,
		Bucket:     l.svcCtx.Bucket,
	})
	if err != nil {
		return nil, fmt.Errorf("insert file meta failed: %w", err)
	}

	// 5. 创建 S3 Multipart Upload
	out, err := l.svcCtx.S3.CreateMultipartUpload(l.ctx, &s3.CreateMultipartUploadInput{
		Bucket:      aws.String(l.svcCtx.Bucket),
		Key:         aws.String(objectKey),
		ContentType: aws.String(in.MimeType),
	})
	if err != nil {
		return nil, fmt.Errorf("create multipart upload failed: %w", err)
	}

	// 6. 为每个分片生成预签名 UploadPart URL（proto 的 ChunkIndex 从 0 起，partNumber 从 1 起）
	resp := &file.CreateUploadTaskResp{
		FileId:   fileId,
		UploadId: aws.ToString(out.UploadId),
		Chunks:   make([]*file.ChunkUploadInfo, 0, in.TotalChunks),
	}
	for i := int32(0); i < in.TotalChunks; i++ {
		pm, err := l.svcCtx.Presign.PresignUploadPart(l.ctx, &s3.UploadPartInput{
			Bucket:     aws.String(l.svcCtx.Bucket),
			Key:        aws.String(objectKey),
			UploadId:   out.UploadId,
			PartNumber: aws.Int32(i + 1),
		}, s3.WithPresignExpires(presignPutExpiry))
		if err != nil {
			return nil, fmt.Errorf("presign part %d failed: %w", i, err)
		}
		resp.Chunks = append(resp.Chunks, &file.ChunkUploadInfo{
			ChunkIndex:   i,
			PresignedUrl: pm.URL,
		})
	}
	return resp, nil
}
