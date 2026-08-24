package logic

import (
	"context"

	"im-platform/app/media/rpc/internal/svc"
	"im-platform/app/media/rpc/media"

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

// 删除文件（物理删除 MinIO + 清理元数据）
func (l *DeleteFileLogic) DeleteFile(in *media.DeleteFileReq) (*media.DeleteFileResp, error) {
	// todo: add your logic here and delete this line

	return &media.DeleteFileResp{}, nil
}
