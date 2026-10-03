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

type GetFileMetaLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewGetFileMetaLogic(ctx context.Context, svcCtx *svc.ServiceContext) *GetFileMetaLogic {
	return &GetFileMetaLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

// GetFileMeta 查文件元数据(含审核状态/缩略图);访问可见性校验由 file rpc 做
func (l *GetFileMetaLogic) GetFileMeta(req *types.GetFileMetaReq) (*types.FileMeta, error) {
	resp, err := l.svcCtx.File.GetFileMeta(l.ctx, &file.GetFileMetaReq{FileId: req.FileId})
	if err != nil {
		return nil, err
	}
	return &types.FileMeta{
		FileId:     resp.FileId,
		FileName:   resp.FileName,
		FileSize:   resp.FileSize,
		MimeType:   resp.MimeType,
		FileType:   resp.FileType,
		UploaderId: resp.UploaderId,
		UploadTime: resp.UploadTime,
		Status:     resp.Status,
		Thumbnails: resp.Thumbnails,
	}, nil
}
