package mq

import (
	"encoding/json"
	"time"
)

// ============================================================================
// im.event.notify 事件广播契约
// 生产者: gateway(上下线) / group.rpc(群变更) / message.rpc(撤回、大群轻量通知)
// 消费者: push.rpc(统一分发:查目标用户 → 直连 gateway 批量下发)
// 载荷为 JSON 信封,客户端按 "event" 字段存在性区分事件帧与聊天推送帧
// ============================================================================

// 事件类型
const (
	EventUserOnline      = "user_online"      // 用户上线: user_id, device_type
	EventUserOffline     = "user_offline"     // 用户下线: user_id, device_type
	EventGroupChanged    = "group_changed"    // 群变更: group_id, action, operator_id, target_user_id
	EventMsgRecall       = "msg_recall"       // 消息撤回: conv_id, msg_id, operator_id
	EventGroupNewMsgLite = "group_new_msg_lite" // 大群轻量通知: group_id, conv_id, seq_id, sender_id
)

// 群变更 action 取值
const (
	GroupActionMemberAdd  = "member_add"
	GroupActionMemberDel  = "member_del"  // 踢出/主动退群
	GroupActionDissolve   = "dissolve"
	GroupActionInfoUpdate = "info_update"
	GroupActionRoleChange = "role_change"
	GroupActionMute       = "mute"
)

// Event 事件信封:所有事件共用一个结构,字段按类型取用
type Event struct {
	Event        string `json:"event"`
	UserID       int64  `json:"user_id,omitempty"`        // 上下线主体
	DeviceType   int64  `json:"device_type,omitempty"`    // 上下线设备
	GroupID      int64  `json:"group_id,omitempty"`       // 群事件群 ID
	Action       string `json:"action,omitempty"`         // 群变更动作
	OperatorID   int64  `json:"operator_id,omitempty"`    // 操作发起人
	TargetUserID int64  `json:"target_user_id,omitempty"` // 群变更受事人
	ConvID       string `json:"conv_id,omitempty"`        // 撤回/轻量通知会话
	MsgID        int64  `json:"msg_id,omitempty"`         // 撤回消息 ID
	SeqID        int64  `json:"seq_id,omitempty"`         // 轻量通知新 Seq
	SenderID     int64  `json:"sender_id,omitempty"`      // 轻量通知发送者
	Timestamp    int64  `json:"timestamp"`                // Unix 毫秒
}

// MarshalEvent 序列化事件信封
func MarshalEvent(e *Event) ([]byte, error) {
	e.Timestamp = time.Now().UnixMilli()
	return json.Marshal(e)
}

// UnmarshalEvent 反序列化事件信封
func UnmarshalEvent(data []byte) (*Event, error) {
	var e Event
	if err := json.Unmarshal(data, &e); err != nil {
		return nil, err
	}
	return &e, nil
}
