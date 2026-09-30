package utils

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/core/stores/redis"
)

// ==================== 设计说明 ====================
// 自研短Token方案（非JWT），核心特点：
// 1. Token本体仅为22字符的随机串（base64url-safe编码16字节随机数），足够短
// 2. 所有元数据（user_id, device_id, expire_at等）存储在Redis Hash中
// 3. 秒踢机制通过 "Token黑名单 + 设备踢出通知 + Kafka兜底广播" 三层实现
// 4. Gateway可直接查Redis校验Token，无需RPC调用User服务，P99 < 5ms
//
// Redis Key 设计：
//   im:token:{token}          -> Hash  {user_id, device_id, device_type, created_at}
//   im:user_tokens:{user_id}  -> Hash  {device_id: token, ...}
//   im:token_blacklist:{token}-> String "kicked_at"（被踢Token快速拦截）
//   im:kick_notify:{device_id}-> String "1"（设备被踢标记，Gateway心跳轮询）
// ==================================================

const (
	TokenKeyPrefix   = "im:token:"
	UserTokensPrefix = "im:user_tokens:"
	BlacklistPrefix  = "im:token_blacklist:"
	KickNotifyPrefix = "im:kick_notify:"
	TokenLength      = 22 // base64.RawURLEncoding.EncodeToString(16bytes) = 22chars
)

// TokenInfo Token解析后的元数据
type TokenInfo struct {
	UserID     int64     `json:"user_id"`
	DeviceID   string    `json:"device_id"`
	DeviceType int32     `json:"device_type"`
	Token      string    `json:"token"`
	CreatedAt  time.Time `json:"created_at"`
}

type TokenManager struct {
	defaultTTL time.Duration
	rds        *redis.Redis
}

// NewTokenManager 创建Token管理器
// rds: go-zero Redis客户端（支持Cluster/Node/Sentinel）
// defaultTTL: Token默认有效期，如 7*24*time.Hour
func NewTokenManager(rds *redis.Redis, defaultTTL time.Duration) *TokenManager {
	return &TokenManager{
		rds:        rds,
		defaultTTL: defaultTTL,
	}
}

func (tm *TokenManager) Issue(ctx context.Context, ti TokenInfo, ttl ...time.Duration) (string, error) {
	token, err := genRandomToken()
	if err != nil {
		return "", fmt.Errorf("gen token failed: %w", err)
	}
	validTTL := tm.defaultTTL
	if len(ttl) > 0 && ttl[0] > 0 {
		validTTL = ttl[0]
	}
	tokenkey := TokenKeyPrefix + token
	userTokensKey := UserTokensPrefix + strconv.FormatInt(ti.UserID, 10)
	// 多端互踢:同设备重新登录时,读 user_tokens 里该设备旧 token 并拉黑+删除。
	// 原实现 im:user_tokens 只写不读,旧 token 在自然过期前一直有效
	if ti.DeviceID != "" {
		if oldToken, gerr := tm.rds.HgetCtx(ctx, userTokensKey, ti.DeviceID); gerr == nil && oldToken != "" && oldToken != token {
			_ = tm.rds.SetexCtx(ctx, BlacklistPrefix+oldToken, "relogin", int(validTTL.Seconds()))
			_, _ = tm.rds.DelCtx(ctx, TokenKeyPrefix+oldToken)
			logx.WithContext(ctx).Infof("kicked old token on relogin | user_id=%d device_id=%s old=%s...", ti.UserID, ti.DeviceID, oldToken[:6])
		}
	}
	now := time.Now()
	err = tm.rds.Pipelined(func(p redis.Pipeliner) error {
		p.HMSet(ctx, tokenkey, map[string]string{
			"user_id":     strconv.FormatInt(ti.UserID, 10),
			"device_id":   ti.DeviceID,
			"device_type": strconv.FormatInt(int64(ti.DeviceType), 10),
			"created_at":  strconv.FormatInt(now.Unix(), 10),
		})
		p.Expire(ctx, tokenkey, validTTL+time.Hour)
		p.HSet(ctx, userTokensKey, ti.DeviceID, token)
		p.Expire(ctx, userTokensKey, validTTL+time.Hour)
		return nil
	})
	if err != nil {
		return "", fmt.Errorf("redis pipeline failed: %w", err)
	}
	logx.WithContext(ctx).Infof("token issued | user_id=%d device_id=%s token=%s...", ti.UserID, ti.DeviceID, token[:6])
	return token, nil
}

func (tm *TokenManager) Verify(ctx context.Context, token string) (*TokenInfo, error) {
	if len(token) != TokenLength {
		return nil, fmt.Errorf("invalid token format")
	}

	blacklisted, err := tm.rds.ExistsCtx(ctx, BlacklistPrefix+token)
	if err != nil {
		return nil, fmt.Errorf("redis error")
	}
	if blacklisted {
		return nil, fmt.Errorf("token has been revoked")
	}
	tokenkey := TokenKeyPrefix + token
	data, err := tm.rds.HgetallCtx(ctx, tokenkey)
	if err != nil {
		return nil, fmt.Errorf("redis err:%w", err)
	}
	if len(data) == 0 {
		return nil, fmt.Errorf("token not found")
	}
	userID, _ := strconv.ParseInt(data["user_id"], 10, 64)
	deviceType, _ := strconv.ParseInt(data["device_type"], 10, 32)
	createdAtSec, _ := strconv.ParseInt(data["created_at"], 10, 64)

	return &TokenInfo{
		UserID:     userID,
		DeviceID:   data["device_id"],
		DeviceType: int32(deviceType),
		Token:      token,
		CreatedAt:  time.Unix(createdAtSec, 0),
	}, nil

}

// Refresh Token刷新
// 旧Token加入黑名单（短期），签发新Token，保持同一device_id
func (tm *TokenManager) Refresh(ctx context.Context, oldToken string, ttl ...time.Duration) (string, error) {
	info, err := tm.Verify(ctx, oldToken)
	if err != nil {
		return "", fmt.Errorf("verify old token failed: %w", err)
	}

	// 旧Token加入黑名单，TTL=5分钟（给客户端缓冲时间）
	err = tm.rds.SetexCtx(ctx, BlacklistPrefix+oldToken, "refreshed", 300)
	if err != nil {
		return "", fmt.Errorf("set old token to blacklist failed: %w", err)
	}
	// 必须同时删除旧 token 的登录态:黑名单 5 分钟后过期,
	// 若 im:token:{old} 还在,旧 token 会"复活"
	userTokensKey := UserTokensPrefix + strconv.FormatInt(info.UserID, 10)
	_, _ = tm.rds.DelCtx(ctx, TokenKeyPrefix+oldToken)
	// user_tokens 映射不删:签发新 Token 时同一 device_id 会覆盖,
	// Issue 的互踢逻辑靠它发现旧 token,但此时旧 token 就是本次刷新的,
	// 先删掉防止 Issue 把刚拉黑的旧 token 再踢一遍(无害但产生噪音日志)
	_, _ = tm.rds.HdelCtx(ctx, userTokensKey, info.DeviceID)
	// 签发新Token
	return tm.Issue(ctx, *info, ttl...)
}

// Revoke 注销Token（用户主动登出）
func (tm *TokenManager) Revoke(ctx context.Context, token string) error {
	info, err := tm.Verify(ctx, token)
	if err != nil {
		return err
	}

	tokenKey := TokenKeyPrefix + token
	userTokensKey := UserTokensPrefix + strconv.FormatInt(info.UserID, 10)

	err = tm.rds.Pipelined(func(p redis.Pipeliner) error {
		p.Del(ctx, tokenKey)
		p.HDel(ctx, userTokensKey, info.DeviceID)
		return nil
	})
	return err
}

// RevokeDevice 按设备踢出：拉黑该设备当前 token 并清除登录态（踢设备/改密后调用）
// deviceID 为签发 Token 时使用的设备标识（LoginRequest.deviceid）
func (tm *TokenManager) RevokeDevice(ctx context.Context, userID int64, deviceID string) error {
	userTokensKey := UserTokensPrefix + strconv.FormatInt(userID, 10)
	token, err := tm.rds.HgetCtx(ctx, userTokensKey, deviceID)
	if err != nil {
		return fmt.Errorf("get device token failed: %w", err)
	}
	if token == "" {
		return nil // 该设备当前无在线 token
	}
	err = tm.rds.Pipelined(func(p redis.Pipeliner) error {
		p.Del(ctx, TokenKeyPrefix+token)
		p.HDel(ctx, userTokensKey, deviceID)
		return nil
	})
	if err != nil {
		return fmt.Errorf("revoke device token failed: %w", err)
	}
	// 黑名单兜底,防止并发请求在删除后仍携带旧 token 通过校验
	if err := tm.rds.SetexCtx(ctx, BlacklistPrefix+token, "kicked", 86400); err != nil {
		return err
	}
	// 设备踢出标记,Gateway 心跳轮询发现后主动断连
	_ = tm.rds.SetexCtx(ctx, KickNotifyPrefix+deviceID, "1", 86400)
	return nil
}

// CheckBlackList Token加入黑名单
// func (tm *TokenManager) AddBlackList(ctx context.Context, token string) error{

// }
