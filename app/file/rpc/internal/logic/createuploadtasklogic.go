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

// S3 Multipart 协议限制:除最后一片外每片 >= 5MiB
const minChunkSize = 5 * 1024 * 1024

// 单分片数上限,防客户端传超大 TotalChunks 拖垮预签名循环
const maxTotalChunks = 10000

// CreateUploadTask 初始化上传任务,返回 file_id 与各分片预签名 URL。
// 两条路径:
//   - 单片快传(TotalChunks==1):预签名 PutObject,不走 multipart,
//     CompleteUpload 时 UploadId 传空即完成——小图片等高频小文件省一次合并往返;
//   - 分片上传(TotalChunks>1):S3 原生 Multipart,客户端按 ChunkIndex+1 对应
//     partNumber 上传,CompleteUpload 携带各片 ETag 合并。
func (l *CreateUploadTaskLogic) CreateUploadTask(in *file.CreateUploadTaskReq) (*file.CreateUploadTaskResp, error) {
	// 1. 参数校验
	if in.FileName == "" || in.FileSize <= 0 || in.TotalChunks <= 0 || in.UploaderId <= 0 {
		return nil, fmt.Errorf("invalid upload params")
	}
	if in.TotalChunks > maxTotalChunks {
		return nil, fmt.Errorf("too many chunks, max %d", maxTotalChunks)
	}
	// 分片大小校验(客户端声明了 chunk_size 才做):除最后一片外必须 >= 5MiB,
	// 且声明出的分片数应与 total_chunks 一致——把 S3 限制前置到任务创建,
	// 否则要等客户端传完所有分片、CompleteMultipartUpload 时才失败
	if in.TotalChunks > 1 && in.ChunkSize > 0 {
		if in.ChunkSize < minChunkSize {
			return nil, fmt.Errorf("chunk size must be >= 5MiB except the last one")
		}
		expected := (in.FileSize + in.ChunkSize - 1) / in.ChunkSize
		if expected != int64(in.TotalChunks) {
			return nil, fmt.Errorf("total_chunks mismatch: file_size=%d chunk_size=%d expect=%d got=%d",
				in.FileSize, in.ChunkSize, expected, in.TotalChunks)
		}
	}

	// 2. 生成 file_id（雪花，字符串对外）
	fileID := strconv.FormatInt(l.svcCtx.Snowflake.NextID(), 10)

	// 3. 对象键：files/{file_id}/{原始文件名}
	objectKey := fmt.Sprintf("files/%s/%s", fileID, in.FileName)

	// 4. 落库元数据。
	// 审核暂未接入:原流程 status=FileStatusAudit(审核中,合并完成前不可见),
	// 现直接置 Normal 让文件合并后立即可用;
	// 接入内容审核时改回 FileStatusAudit,由审核服务经 UpdateAudit 流转状态
	if _, err := l.svcCtx.FilesModel.Insert(l.ctx, &models.Files{
		Id:         fileID,
		FileName:   in.FileName,
		FileSize:   in.FileSize,
		MimeType:   in.MimeType,
		FileType:   int64(in.FileType),
		UploaderId: in.UploaderId,
		UploadTime: time.Now(),
		// Status: models.FileStatusAudit, // TODO: 内容审核接入后恢复审核中初态
		Status:    models.FileStatusNormal,
		ObjectKey: objectKey,
		Bucket:    l.svcCtx.Bucket,
	}); err != nil {
		return nil, fmt.Errorf("insert file meta failed: %w", err)
	}

	resp := &file.CreateUploadTaskResp{
		FileId: fileID,
		Chunks: make([]*file.ChunkUploadInfo, 0, in.TotalChunks),
	}

	// 5a. 单片快传:预签名 PutObject,不走 multipart
	if in.TotalChunks == 1 {
		pm, err := l.svcCtx.Presign.PresignPutObject(l.ctx, &s3.PutObjectInput{
			Bucket:      aws.String(l.svcCtx.Bucket),
			Key:         aws.String(objectKey),
			ContentType: aws.String(in.MimeType),
		}, s3.WithPresignExpires(presignPutExpiry))
		if err != nil {
			return nil, fmt.Errorf("presign put failed: %w", err)
		}
		resp.Chunks = append(resp.Chunks, &file.ChunkUploadInfo{
			ChunkIndex:   0,
			PresignedUrl: pm.URL,
		})
		return resp, nil
	}

	// 5b. 分片上传:创建 S3 Multipart Upload + 为每片预签名 UploadPart URL
	//（proto 的 ChunkIndex 从 0 起，partNumber 从 1 起）
	out, err := l.svcCtx.S3.CreateMultipartUpload(l.ctx, &s3.CreateMultipartUploadInput{
		Bucket:      aws.String(l.svcCtx.Bucket),
		Key:         aws.String(objectKey),
		ContentType: aws.String(in.MimeType),
	})
	if err != nil {
		return nil, fmt.Errorf("create multipart upload failed: %w", err)
	}
	resp.UploadId = aws.ToString(out.UploadId)
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
