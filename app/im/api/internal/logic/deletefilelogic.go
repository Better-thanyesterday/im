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

type DeleteFileLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewDeleteFileLogic(ctx context.Context, svcCtx *svc.ServiceContext) *DeleteFileLogic {
	return &DeleteFileLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

// DeleteFile 删除文件:operator 身份以 token 为准,归属校验(只能删自己上传的)由 file rpc 做
func (l *DeleteFileLogic) DeleteFile(req *types.DeleteFileReq) (*types.SuccessResp, error) {
	uid, ok := middleware.GetUserID(l.ctx)
	if !ok || uid <= 0 {
		return nil, fmt.Errorf("unauthorized")
	}
	_, err := l.svcCtx.File.DeleteFile(l.ctx, &file.DeleteFileReq{
		FileId:     req.FileId,
		OperatorId: uid,
	})
	if err != nil {
		return nil, err
	}
	return &types.SuccessResp{Success: true}, nil
}
