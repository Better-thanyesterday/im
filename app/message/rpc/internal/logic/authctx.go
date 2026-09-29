package logic

import (
	"context"
	"strconv"

	"google.golang.org/grpc/metadata"
)

// userIDFromCtx 从 gRPC incoming metadata 提取 gateway 注入的 x-user-id;
// 不存在或非法时返回 0,调用方回退到请求字段并自行做 >0 校验
func userIDFromCtx(ctx context.Context) int64 {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return 0
	}
	vals := md.Get("x-user-id")
	if len(vals) == 0 {
		return 0
	}
	uid, err := strconv.ParseInt(vals[0], 10, 64)
	if err != nil || uid <= 0 {
		return 0
	}
	return uid
}
