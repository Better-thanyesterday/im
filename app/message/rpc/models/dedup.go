package models

import (
	"context"
	"fmt"
	"github.com/zeromicro/go-zero/core/stores/redis"
	"time"
)

type DedupModel struct {
	redis    *redis.Redis
	MsgModel MessagesModel // 用于命中后反查 PG
}

func NewDedupModel(r *redis.Redis, m MessagesModel) *DedupModel {
	return &DedupModel{redis: r, MsgModel: m}
}

// CheckAndGet 返回 (是否重复, 原消息, error)
func (m *DedupModel) CheckAndGet(ctx context.Context, convId, clientMsgId string) (bool, *Messages, error) {
	key := fmt.Sprintf("im:dedup:%s", convId)
	isMember, err := m.redis.Sismember(key, clientMsgId)
	if err != nil || !isMember {
		return false, nil, err
	}
	// Redis 命中，反查 PG 返回完整消息体
	msg, err := m.MsgModel.FindOneByClientmsgid(ctx,clientMsgId)
	if err != nil {
		return true, nil, err
	}
	return true, msg, nil
}

func (m *DedupModel) Set(ctx context.Context, convId, clientMsgId string) error {
	key := fmt.Sprintf("im:dedup:%s", convId)
	if _, err := m.redis.Sadd(key, clientMsgId); err != nil {
		return err
	}
	err := m.redis.Expire(key, int(24*time.Hour.Seconds()))
	return err
}
