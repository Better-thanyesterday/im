package logic

import (
	"context"
	"encoding/json"
	"im-platform/app/gateway/api/conn"
	"im-platform/app/gateway/api/internal/svc"
	"im-platform/app/gateway/api/protocol"
	"im-platform/app/message/rpc/messageclient"
	"strconv"

	"github.com/zeromicro/go-zero/core/logx"
	"google.golang.org/grpc/metadata"
)

// 注入到 conn.ReadPump 的回调，在独立 goroutine 里执行，避免阻塞读泵
func HandleFrame(svcCtx *svc.ServiceContext, c *conn.Conn, data []byte) {
	frameType, payload, err := protocol.DecodeFrame(data)
	if err != nil {
		logx.Errorf("decode frame err: %v", err)
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
			logx.Errorf("unauthenticated conn send frame, drop it, userid=%d", in.SenderId)
			return
		}
		svcCtx.Message.SendMessage(context.Background(), &in)
	case protocol.FrameAck:
		// -> svcCtx.MessageRpc.AckMessage
		var in messageclient.AckMessageReq
		if json.Unmarshal(payload, &in) != nil {
			return
		}
		// user_id 不信任客户端,以握手鉴权写入 conn 的 userId 为准
		in.UserId = c.UserId()
		if in.UserId <= 0 {
			logx.Errorf("unauthenticated conn ack frame, drop it, userid=%d", in.UserId)
			return
		}
		svcCtx.Message.AckMessage(withUserCtx(c), &in)
	case protocol.FrameSyncRequest:
		// -> svcCtx.MessageRpc.SyncMessages
		var in messageclient.SyncMessageReq
		if json.Unmarshal(payload, &in) != nil {
			return
		}
		in.UserId = c.UserId()
		if in.UserId <= 0 {
			logx.Errorf("unauthenticated conn sync frame, drop it, userid=%d", in.UserId)
			return
		}
		svcCtx.Message.SyncMessage(withUserCtx(c), &in)
	}
}

// withUserCtx 把握手鉴权得到的 userId 注入 gRPC metadata,供 message rpc 校验
func withUserCtx(c *conn.Conn) context.Context {
	md := metadata.Pairs("x-user-id", strconv.FormatInt(c.UserId(), 10))
	return metadata.NewOutgoingContext(context.Background(), md)
}
