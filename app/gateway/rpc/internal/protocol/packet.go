package protocol

import (
	"encoding/binary"
	"errors"
)
const (
    FrameHeartbeat   byte = 0x01 // 心跳（pong 已由 conn 层处理，上行可不发）
    FrameSendMessage byte = 0x02 // 上行：发消息
    FrameAck         byte = 0x03 // 上行：ACK
    FrameSyncRequest byte = 0x04 // 上行：断线同步
    FramePush        byte = 0x10 // 下行：推送消息
    FrameKick        byte = 0x11 // 下行：踢下线/顶号通知
	FrameError       byte = 0x1F // 通用错误回包
)

// 建议扩展：[type:1][req_id:4][len:4][payload]
// 帧格式: [type:1] [length:4 big-endian] [payload]
func EncodeFrame(frameType byte, payload []byte) []byte {
    buf := make([]byte, 5+len(payload))
    buf[0] = frameType
    binary.BigEndian.PutUint32(buf[1:5], uint32(len(payload)))
    copy(buf[5:], payload)
    return buf
}

// DecodeFrame 解析二进制帧 [type:1][len:4][payload],与 EncodeFrame 对称。
// 原实现先做 hex 解码,任何二进制帧都解析失败
func DecodeFrame(data []byte) (frameType byte, payload []byte, err error) {
    if len(data) < 5 {
        return 0, nil, errors.New("frame too short")
    }
    n := binary.BigEndian.Uint32(data[1:5])
    if int(n) != len(data)-5 {
        return 0, nil, errors.New("frame length mismatch")
    }
    return data[0], data[5:], nil
}

