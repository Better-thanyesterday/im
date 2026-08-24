package main

import (
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"im-platform/app/message/rpc/message"
)

func main() {
	payload, _ := json.Marshal(message.SendMessageReq{
		ClientMsgId: "114411111131e81110-e19b-21d3-a716-446655410000",
		SenderId:    746262395202048000,
		Isgroup:     false,
		ToUid:       746263247023247360,
		MsgType:     1,
		Body: &message.MessageBody{
			Id:     1,
			ConvId: "746262395202048000:746263247023247360",
			SenderId: 746262395202048000,
			ClientMsgId: "550e8400-e29b-41d4-a716-446655440000",
			MsgType: 1,
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
}
