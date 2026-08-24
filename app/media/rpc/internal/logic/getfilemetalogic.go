package logic

import (
	"context"

	"im-platform/app/media/rpc/internal/svc"
	"im-platform/app/media/rpc/media"

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

// 查询文件元数据（含审核状态、缩略图地址）
func (l *GetFileMetaLogic) GetFileMeta(in *media.GetFileMetaReq) (*media.GetFileMetaResp, error) {
	// todo: add your logic here and delete this line

	return &media.GetFileMetaResp{}, nil
}
