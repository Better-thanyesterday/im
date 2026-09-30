//go:build ignore

package main

import (
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"im-platform/app/message/rpc/message"

	"github.com/google/uuid"
)

func main() {
	msgid := uuid.NewString()
	payload, _ := json.Marshal(message.SendMessageReq{
		ClientMsgId: msgid,
		SenderId:    746262395202048000,
		Isgroup:     false,
		ToUid:       746263247023247360,
		MsgType:     1,
		Body: &message.MessageBody{
			Id:          1,
			ConvId:      "746262395202048000:746263247023247360",
			SenderId:    746262395202048000,
			ClientMsgId: "550e8400-e29b-41d4-a716-446655440000",
			MsgType:     1,
			Content: &message.MessageContent{
				Text: "hello world",
			},
		},
		Extra: &message.MessageExtra{},
	})

	frame := make([]byte, 5+len(payload))
	frame[0] = 0x02 // FrameSendMessage
	binary.BigEndian.PutUint32(frame[1:5], uint32(len(payload)))
	copy(frame[5:], payload)

	fmt.Println(hex.EncodeToString(frame))
	buildgroupmsg()
}

func buildgroupmsg() {
	msgid := uuid.NewString()
	payload, _ := json.Marshal(message.SendMessageReq{
		ClientMsgId: msgid,
		SenderId:    746262395202048000,
		Isgroup:     true,
		ToUid:       748110407792594944,
		MsgType:     1,
		Body: &message.MessageBody{
			Id:          1,
			ConvId:      "group_748110407792594944",
			SenderId:    746262395202048000,
			ClientMsgId: msgid,
			MsgType:     1,
			Content: &message.MessageContent{
				Text: "hello group",
			},
		},
		Extra: &message.MessageExtra{},
	})

	frame := make([]byte, 5+len(payload))
	frame[0] = 0x02 // FrameSendMessage
	binary.BigEndian.PutUint32(frame[1:5], uint32(len(payload)))
	copy(frame[5:], payload)

	fmt.Println(hex.EncodeToString(frame))
}
