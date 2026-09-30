package logic

import (
	"context"
	"fmt"

	"im-platform/app/file/rpc/file"
	"im-platform/app/file/rpc/internal/svc"
	"im-platform/app/file/rpc/models"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/zeromicro/go-zero/core/logx"
)

type DeleteFileLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewDeleteFileLogic(ctx context.Context, svcCtx *svc.ServiceContext) *DeleteFileLogic {
	return &DeleteFileLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

// DeleteFile 删除文件（物理删除对象 + 元数据置已删除）。
// S3 DeleteObject 本身幂等：对象不存在也返回成功，无需前置探查。
func (l *DeleteFileLogic) DeleteFile(in *file.DeleteFileReq) (*file.DeleteFileResp, error) {
	if in.FileId == "" {
		return nil, fmt.Errorf("file_id required")
	}

	meta, err := l.svcCtx.FilesModel.FindOne(l.ctx, in.FileId)
	if err != nil {
		return nil, fmt.Errorf("file not found: %w", err)
	}
	// 权限：仅上传者本人可删除
	if in.OperatorId != meta.UploaderId {
		return nil, fmt.Errorf("permission denied")
	}
	if meta.Status == models.FileStatusDeleted {
		return &file.DeleteFileResp{}, nil // 幂等
	}

	// 物理删除 MinIO 对象（S3 兼容 API）
	if _, err := l.svcCtx.S3.DeleteObject(l.ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(meta.Bucket),
		Key:    aws.String(meta.ObjectKey),
	}); err != nil {
		return nil, fmt.Errorf("delete object failed: %w", err)
	}

	// 元数据软删（保留记录供审计）
	if err := l.svcCtx.FilesModel.UpdateStatus(l.ctx, in.FileId, models.FileStatusDeleted); err != nil {
		return nil, fmt.Errorf("update status failed: %w", err)
	}

	logx.WithContext(l.ctx).Infof("file deleted | file_id=%s operator=%d", in.FileId, in.OperatorId)
	return &file.DeleteFileResp{}, nil
}
