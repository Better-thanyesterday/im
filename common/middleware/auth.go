package middleware

import (
	"context"
	"im-platform/common/utils"
	"net/http"
	"strings"
	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/rest/httpx"
)

type CtxKey string

const (
	CtxUserID     CtxKey = "user_id"
	CtxDeviceID   CtxKey = "device_id"
	CtxDeviceType CtxKey = "device_type"
	CtxToken      CtxKey = "token"
)

// AuthError 鉴权错误（带业务错误码，兼容文档规范）
type AuthError struct {
	Code int    `json:"code"`
	Msg  string `json:"msg"`
}

func (e *AuthError) Error() string {
	return e.Msg
}
// ==================== HTTP中间件（Gateway层）====================

// GatewayAuthMiddleware Gateway接入鉴权中间件
// 从Header或Query提取Token，查Redis校验，注入上下文
// 使用方式：在 .api 文件中 @server(middleware: Auth) 声明
func AuthMiddleware(tm *utils.TokenManager) func(next http.HandlerFunc) http.HandlerFunc{
	return func(next http.HandlerFunc) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			ctx:=r.Context()
			// 兼容 "Bearer <token>" 与裸 token 两种携带方式
			tokenStr:=r.Header.Get("Authorization")
			if parts := strings.SplitN(tokenStr, " ", 2); len(parts) == 2 && strings.EqualFold(parts[0], "Bearer") {
				tokenStr = strings.TrimSpace(parts[1])
			}
			// WebSocket 握手(浏览器/WebView)无法携带 Authorization 头,仅 /ws 路径允许 query token 回退:
			// token 是显式凭证、无 cookie 这类环境载体,不会被第三方页面自动附带,CSWSH 风险不成立
			if tokenStr == "" && r.URL.Path == "/ws" {
				tokenStr = r.URL.Query().Get("token")
			}
			if tokenStr =="" {
				httpx.Error(w, &AuthError{Code: 100002, Msg: "Token缺失"})
				return
			}
			info ,err :=tm.Verify(ctx,tokenStr)
			if err != nil {
				//logx.WithContext(ctx).Warnf("auth failed | token=%s... err=%v", tokenStr[:6], err)
				httpx.Error(w, &AuthError{Code: 100002, Msg: "Token无效或过期"})
				return
			}
			ctx = context.WithValue(ctx, CtxUserID, info.UserID)
			ctx = context.WithValue(ctx, CtxDeviceID, info.DeviceID)
			ctx = context.WithValue(ctx, CtxDeviceType, info.DeviceType)
			ctx = context.WithValue(ctx, CtxToken, tokenStr)

			logx.WithContext(ctx).Debugf("auth success | user_id=%d device=%s", info.UserID, info.DeviceID)
			next(w, r.WithContext(ctx))
		}
	}
}

// ==================== 上下文快捷取值方法 ====================

// GetUserID 从Context获取用户ID
func GetUserID(ctx context.Context) (int64, bool) {
	v, ok := ctx.Value(CtxUserID).(int64)
	return v, ok
}

// MustGetUserID 从Context获取用户ID，不存在则panic（用于确定已鉴权的场景）
func MustGetUserID(ctx context.Context) int64 {
	v, ok := ctx.Value(CtxUserID).(int64)
	if !ok {
		panic("user_id not found in context")
	}
	return v
}

// GetDeviceID 从Context获取设备ID
func GetDeviceID(ctx context.Context) (string, bool) {
	v, ok := ctx.Value(CtxDeviceID).(string)
	return v, ok
}

// GetDeviceType 从Context获取设备类型
func GetDeviceType(ctx context.Context) (int32, bool) {
	v, ok := ctx.Value(CtxDeviceType).(int32)
	return v, ok
}

// GetToken 从Context获取Token
func GetToken(ctx context.Context) (string, bool) {
	v, ok := ctx.Value(CtxToken).(string)
	return v, ok
}

// BuildAuthContext 构建带认证信息的Context（用于RPC客户端调用）
func BuildAuthContext(parent context.Context, token string) context.Context {
	ctx := context.WithValue(parent, CtxToken, token)
	return ctx
}