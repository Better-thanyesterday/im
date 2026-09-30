package constants

import "fmt"

// ============================================================================
// 消息类型（对应 messages.msg_type）
// ============================================================================
const (
	MsgTypeText     int64 = 1 // 文本
	MsgTypeImage    int64 = 2 // 图片
	MsgTypeVoice    int64 = 3 // 语音
	MsgTypeVideo    int64 = 4 // 视频
	MsgTypeFile     int64 = 5 // 文件
	MsgTypeLocation int64 = 6 // 位置
	MsgTypeSystem   int64 = 7 // 系统
)

// MsgTypeName 消息类型名（日志 / 推送摘要 / 未读预览用）
var MsgTypeName = map[int64]string{
	MsgTypeText:     "text",
	MsgTypeImage:    "image",
	MsgTypeVoice:    "voice",
	MsgTypeVideo:    "video",
	MsgTypeFile:     "file",
	MsgTypeLocation: "location",
	MsgTypeSystem:   "system",
}

// IsValidMsgType 校验消息类型是否合法（发送入口参数校验）
func IsValidMsgType(t int64) bool {
	_, ok := MsgTypeName[t]
	return ok
}

// ============================================================================
// 消息状态（对应 messages.status）
// ============================================================================
const (
	MsgStatusNormal   int64 = 1 // 正常
	MsgStatusRecalled int64 = 2 // 撤回
	MsgStatusDeleted  int64 = 3 // 删除
)

// ============================================================================
// ACK 类型（客户端经 Gateway 上报，对应 AckMessage.ack_type）
// 消息生命周期：已发送 → 已送达 → 已读
// 注意:不要再定义 MsgStatusDelivered/MsgStatusRead 之类的常量,
// 旧定义值 2/3 与 MsgStatusRecalled/MsgStatusDeleted 撞值,已删除;
// 已读状态用 AckTypeRead + read_seq 水位表达,不走 messages.status 列
// ============================================================================
const (
	AckTypeDelivered int64 = 1 // 已送达（客户端收到消息）
	AckTypeRead      int64 = 2 // 已读（客户端打开会话）
)

// ============================================================================
// 收件箱状态（对应 inboxes.status）
// ============================================================================
const (
	InboxStatusNormal  int64 = 1 // 正常
	InboxStatusDeleted int64 = 2 // 用户侧删除
)

// ============================================================================
// 会话类型（消息路由器分发依据）
// ============================================================================
const (
	ConvTypeSingle int64 = 1 // 单聊
	ConvTypeGroup  int64 = 2 // 群聊
	ConvTypeSystem int64 = 3 // 系统
)

// ============================================================================
// 会话 ID 生成规则（见《数据库设计文档》4.1）
// 单聊：min_uid:max_uid；群聊：group_{group_id}
// ============================================================================
func BuildSingleConvID(uidA, uidB int64) string {
	if uidA < uidB {
		return fmt.Sprintf("%d:%d", uidA, uidB)
	}
	return fmt.Sprintf("%d:%d", uidB, uidA)
}

func BuildGroupConvID(groupID int64) string {
	return fmt.Sprintf("group_%d", groupID)
}

// ============================================================================
// 错误码（规范：{服务标识}{模块}{序号}，3 = 消息服务）
// 300001~300003 见《开发指导文档》11.4；300004+ 为 MVP 业务补充
// ============================================================================
const (
	ErrCodeMsgSendTooFrequent int64 = 300001 // 消息发送过于频繁
	ErrCodeMsgIdempotentDup   int64 = 300002 // 消息幂等重复
	ErrCodeMsgBlocked         int64 = 300003 // 对方已拉黑
	ErrCodeMsgInValidParam    int64 = 300004 // 消息非法 
	ErrCodeMsgNotFound        int64 = 300005 // 消息不存在
	ErrCodeMsgRecallTimeout   int64 = 300006 // 撤回超时（默认 2 分钟）
	ErrCodeMsgRecallForbidden int64 = 300007 // 无权限撤回（非发送方）
	ErrCodeMsgNotFriend       int64 = 300008 // 非好友,禁止单聊
)

// MsgErrMsg 错误码 → 错误文案
var MsgErrMsg = map[int64]string{
	ErrCodeMsgSendTooFrequent: "消息发送过于频繁",
	ErrCodeMsgIdempotentDup:   "消息幂等重复",
	ErrCodeMsgBlocked:         "对方已拉黑",
	ErrCodeMsgInValidParam:    "消息非法",
	ErrCodeMsgNotFound:        "消息不存在",
	ErrCodeMsgRecallTimeout:   "撤回超时，仅支持撤回 2 分钟内的消息",
	ErrCodeMsgRecallForbidden: "无权限撤回该消息",
	ErrCodeMsgNotFriend:       "仅好友之间可发送单聊消息",
}

// MsgError 消息服务业务错误：logic 层直接返回，Gateway 捕获后映射为统一响应码
type MsgError struct {
	Code int64
	Msg  string
}

func (e *MsgError) Error() string {
	return fmt.Sprintf("code=%d, message=%s", e.Code, e.Msg)
}

func NewMsgError(code int64) *MsgError {
	return &MsgError{Code: code, Msg: MsgErrMsg[code]}
}