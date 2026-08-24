package logic

import (
	"context"
	"encoding/json"
	"im-platform/app/gateway/api/conn"
	"im-platform/app/gateway/api/internal/svc"
	"im-platform/app/gateway/api/protocol"
	"im-platform/app/message/rpc/messageclient"

	"github.com/zeromicro/go-zero/core/logx"
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
		svcCtx.Message.SendMessage(context.Background(), &in)
	case protocol.FrameAck:
		// -> svcCtx.MessageRpc.AckMessage
		var in messageclient.AckMessageReq
		svcCtx.Message.AckMessage(context.Background(), &in)
	case protocol.FrameSyncRequest:
		// -> svcCtx.MessageRpc.SyncMessages
		var in messageclient.SyncMessageReq
		svcCtx.Message.SyncMessage(context.Background(), &in)
	}
}
