package logic

import (
	"context"
	"encoding/json"
	"im-platform/app/gateway/rpc/internal/conn"
	"im-platform/app/gateway/rpc/internal/svc"
	"im-platform/app/gateway/rpc/internal/protocol"
	"im-platform/app/message/rpc/messageclient"
	"strconv"
	"time"

	"github.com/zeromicro/go-zero/core/logx"
	"google.golang.org/grpc/metadata"
)

// 注入到 conn.ReadPump 的回调，在独立 goroutine 里执行，避免阻塞读泵
func HandleFrame(svcCtx *svc.ServiceContext, c *conn.Conn, data []byte) {
	frameType, payload, err := protocol.DecodeFrame(data)
	if err != nil {
		logx.Errorf("上行帧解码失败 | err=%v", err)
		return
	}
	switch frameType {
	case protocol.FrameSendMessage:
		var in messageclient.SendMessageReq
		if json.Unmarshal(payload, &in) != nil {
			return
		}
		// sender_id 不信任客户端,以握手鉴权写入 conn 的 userId 为准
		in.SenderId = c.UserId()
		if in.SenderId <= 0 {
			logx.Errorf("未鉴权连接上行帧,丢弃 | frame=send userid=%d", in.SenderId)
			return
		}
		ctx, cancel := rpcCtx(c, 5*time.Second)
		defer cancel()
		resp, err := svcCtx.Message.SendMessage(ctx, &in)
		if err != nil {
			sendErrorFrame(c, frameType, err)
			return
		}
		// 发送结果下行 ACK:复用 FrameSendMessage 帧型,客户端按 client_msg_id 匹配
		if b, jerr := json.Marshal(resp); jerr == nil {
			c.Send(protocol.EncodeFrame(protocol.FrameSendMessage, b))
		}
	case protocol.FrameAck:
		// -> svcCtx.MessageRpc.AckMessage
		var in messageclient.AckMessageReq
		if json.Unmarshal(payload, &in) != nil {
			return
		}
		// user_id 不信任客户端,以握手鉴权写入 conn 的 userId 为准
		in.UserId = c.UserId()
		if in.UserId <= 0 {
			logx.Errorf("未鉴权连接上行帧,丢弃 | frame=ack userid=%d", in.UserId)
			return
		}
		ctx, cancel := rpcCtx(c, 5*time.Second)
		defer cancel()
		if _, err := svcCtx.Message.AckMessage(ctx, &in); err != nil {
			sendErrorFrame(c, frameType, err)
		}
	case protocol.FrameSyncRequest:
		// -> svcCtx.MessageRpc.SyncMessages
		var in messageclient.SyncMessageReq
		if json.Unmarshal(payload, &in) != nil {
			return
		}
		in.UserId = c.UserId()
		if in.UserId <= 0 {
			logx.Errorf("未鉴权连接上行帧,丢弃 | frame=sync userid=%d", in.UserId)
			return
		}
		// 同步可能带大补发批次,给更长的调用级超时
		ctx, cancel := rpcCtx(c, 30*time.Second)
		defer cancel()
		resp, err := svcCtx.Message.SyncMessage(ctx, &in)
		if err != nil {
			sendErrorFrame(c, frameType, err)
			return
		}
		// 同步结果下发:复用 FrameSyncRequest 帧型,客户端按 conv_id+server_seq 合并
		if b, jerr := json.Marshal(resp); jerr == nil {
			c.Send(protocol.EncodeFrame(protocol.FrameSyncRequest, b))
		}
	}
}

// sendErrorFrame 通用错误回包(FrameError 0x1F):上行 RPC 失败必须回执,
// 否则客户端发送失败永远无感知,只能等超时
func sendErrorFrame(c *conn.Conn, frameType byte, err error) {
	payload, _ := json.Marshal(map[string]any{
		"frame":   frameType, // 出错的请求帧型
		"message": err.Error(),
	})
	c.Send(protocol.EncodeFrame(protocol.FrameError, payload))
}

// rpcCtx 构造带鉴权 metadata 与调用级超时的 RPC 上下文。
// MsgRpc 的 Timeout:0 在 go-zero v1.10.3 中表示完全不设 deadline,
// 若 message rpc 挂起,读泵的 goroutine 会集体假死,必须在调用点自带超时
func rpcCtx(c *conn.Conn, timeout time.Duration) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	md := metadata.Pairs("x-user-id", strconv.FormatInt(c.UserId(), 10))
	return metadata.NewOutgoingContext(ctx, md), cancel
}
