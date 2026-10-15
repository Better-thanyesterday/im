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

type CompleteUploadLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewCompleteUploadLogic(ctx context.Context, svcCtx *svc.ServiceContext) *CompleteUploadLogic {
	return &CompleteUploadLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

// CompleteUpload 全部分片上传完成后合并:上传者身份以 token 为准,
// 归属校验(只能完成自己的任务)由 file rpc 做
func (l *CompleteUploadLogic) CompleteUpload(req *types.CompleteUploadReq) (*types.CompleteUploadResp, error) {
	uid, ok := middleware.GetUserID(l.ctx)
	if !ok || uid <= 0 {
		return nil, fmt.Errorf("unauthorized")
	}
	resp, err := l.svcCtx.File.CompleteUpload(l.ctx, &file.CompleteUploadReq{
		FileId:     req.FileID,
		UploadId:   req.UploadID,
		Etags:      req.Etags,
		UploaderId: uid,
	})
	if err != nil {
		return nil, err
	}
	return &types.CompleteUploadResp{
		FileID: resp.FileId,
		Url:    resp.Url,
		Status: resp.Status,
	}, nil
}
