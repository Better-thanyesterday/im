package logic

import (
	"context"
	"encoding/json"
	"fmt"

	"im-platform/app/file/rpc/file"
	"im-platform/app/file/rpc/internal/svc"

	"github.com/zeromicro/go-zero/core/logx"
)

type GetFileMetaLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewGetFileMetaLogic(ctx context.Context, svcCtx *svc.ServiceContext) *GetFileMetaLogic {
	return &GetFileMetaLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

// GetFileMeta 查询文件元数据（含审核状态、缩略图地址）
func (l *GetFileMetaLogic) GetFileMeta(in *file.GetFileMetaReq) (*file.GetFileMetaResp, error) {
	if in.FileId == "" {
		return nil, fmt.Errorf("file_id required")
	}

	meta, err := l.svcCtx.FilesModel.FindOne(l.ctx, in.FileId)
	if err != nil {
		return nil, fmt.Errorf("file not found: %w", err)
	}

	resp := &file.GetFileMetaResp{
		FileId:     meta.Id,
		FileName:   meta.FileName,
		FileSize:   meta.FileSize,
		MimeType:   meta.MimeType,
		FileType:   int32(meta.FileType),
		UploaderId: meta.UploaderId,
		UploadTime: meta.UploadTime.UnixMilli(),
		Status:     int32(meta.Status),
	}

	// 缩略图（JSON -> map）
	if meta.Thumbnails.Valid && meta.Thumbnails.String != "" {
		thumbs := make(map[string]string)
		if err := json.Unmarshal([]byte(meta.Thumbnails.String), &thumbs); err == nil {
			resp.Thumbnails = thumbs
		}
	}

	// 审核结果
	if meta.AuditType.Valid {
		resp.AuditResult = &file.AuditResult{
			AuditType: int32(meta.AuditType.Int64),
			Status:    int32(meta.AuditStatus.Int64),
			Detail:    meta.AuditDetail.String,
			AuditedAt: meta.AuditedAt.Time.UnixMilli(),
		}
	}
	return resp, nil
}
