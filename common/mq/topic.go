package mq

// ============================================================================
// Topic 常量（见《开发指导文档》7.2.3 Kafka Topic 设计）
// ============================================================================
const (
	// TopicMsgPersist 消息异步落库（分区 32 | 副本 3）
	// 生产者：消息服务发送管道   消费者：消息服务 msg-persist-group
	TopicMsgPersist = "im.msg.persist"

	TopicSeqPersist = "im.seq.persist"

	// TopicMsgOffline 离线消息写入 Redis（分区 16 | 副本 3）
	// 生产者：消息/推送服务      消费者：推送服务 offline-group
	TopicMsgOffline = "im.msg.offline"

	// TopicFileAudit 文件内容审核（分区 8 | 副本 3）
	// 生产者：媒体服务           消费者：内容安全服务 security-group
	TopicFileAudit = "im.file.audit"

	// TopicEventNotify 用户上下线、群变更事件广播（分区 8 | 副本 3）
	// 生产者：网关/群组服务      消费者：多组订阅（网关、消息、推送）
	TopicEventNotify = "im.event.notify"

	// TopicPushRetry 推送失败重试（分区 8 | 副本 3）
	// 生产者：推送服务           消费者：推送服务 push-retry-group
	TopicPushRetry = "im.push.retry"
)

// ============================================================================
// 消费者组常量
// ============================================================================
const (
	GroupMsgPersist = "msg-persist-group" // 消息落库消费组
	GroupOffline    = "offline-group"     // 离线消息消费组
	GroupSecurity   = "security-group"    // 内容审核消费组
	GroupPushRetry  = "push-retry-group"  // 推送重试消费组
)

// Topic 描述信息（调试/运维日志用）
var TopicDesc = map[string]string{
	TopicMsgPersist: "消息异步落库",
	TopicMsgOffline: "离线消息写入 Redis",
	TopicFileAudit:  "文件内容审核",
	TopicEventNotify: "用户上下线、群变更事件广播",
	TopicPushRetry:  "推送失败重试",
}