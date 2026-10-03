// Code scaffolded by goctl. Safe to edit.
// goctl 1.10.1

package logic

import (
	"context"
	"fmt"

	"im-platform/app/file/rpc/file"
	"im-platform/app/im/api/internal/svc"
	"im-platform/app/im/api/internal/types"
	"im-platform/common/middleware"

	"github.com/zeromicro/go-zero/core/logx"
)

type CreateUploadTaskLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewCreateUploadTaskLogic(ctx context.Context, svcCtx *svc.ServiceContext) *CreateUploadTaskLogic {
	return &CreateUploadTaskLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

// CreateUploadTask 初始化分片上传:uploader 身份以 token 为准,
// 大小/MIME/分片数的服务端校验由 file rpc 做
func (l *CreateUploadTaskLogic) CreateUploadTask(req *types.CreateUploadTaskReq) (*types.CreateUploadTaskResp, error) {
	uid, ok := middleware.GetUserID(l.ctx)
	if !ok || uid <= 0 {
		return nil, fmt.Errorf("unauthorized")
	}
	resp, err := l.svcCtx.File.CreateUploadTask(l.ctx, &file.CreateUploadTaskReq{
		FileName:    req.FileName,
		FileSize:    req.FileSize,
		MimeType:    req.MimeType,
		TotalChunks: req.TotalChunks,
		ChunkSize:   req.ChunkSize,
		UploaderId:  uid,
		FileType:    req.FileType,
	})
	if err != nil {
		return nil, err
	}
	chunks := make([]types.ChunkUpload, 0, len(resp.Chunks))
	for _, c := range resp.Chunks {
		chunks = append(chunks, types.ChunkUpload{
			ChunkIndex:   c.ChunkIndex,
			PresignedUrl: c.PresignedUrl,
		})
	}
	return &types.CreateUploadTaskResp{
		FileId:   resp.FileId,
		UploadId: resp.UploadId,
		Chunks:   chunks,
	}, nil
}
