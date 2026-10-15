package logic

import (
	"context"

	"im-platform/app/group/rpc/internal/svc"
	"im-platform/common/mq"
	"fmt"

	"github.com/zeromicro/go-zero/core/logx"
)

// PublishGroupChanged 广播群变更事件到 im.event.notify(key=conv_id 保同群有序)。
// 在成功完成 DB 变更之后调用;发布失败只记日志,不影响主流程,
// 离线成员由客户端下次拉群信息/Sync 兜底
func PublishGroupChanged(ctx context.Context, svcCtx *svc.ServiceContext, groupId int64, action string, operatorId, targetUserId int64) {
	if svcCtx.KafkaProducer == nil {
		return
	}
	payload, err := mq.MarshalEvent(&mq.Event{
		Event:        mq.EventGroupChanged,
		GroupID:      groupId,
		Action:       action,
		OperatorID:   operatorId,
		TargetUserID: targetUserId,
	})
	if err != nil {
		return
	}
	convId := fmt.Sprintf("group_%d", groupId)
	if err := svcCtx.KafkaProducer.PublishWithKey(ctx, mq.TopicEventNotify, convId, payload); err != nil {
		logx.WithContext(ctx).Errorf("群变更事件发布失败 | group=%d action=%s err=%v", groupId, action, err)
	}
}
