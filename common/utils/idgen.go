package utils

import (
	"fmt"
	"net"
	"os"
	"sync"
	"time"

	"github.com/zeromicro/go-zero/core/logx"
)

type Snowflake struct {
	mu        sync.Mutex
	lastTimestamp int64
	workNode      int64
	sequence  int64
}

const (
	workNodeBits  = 10
	sequenceBits  = 12
	workNodeMax   = -1 ^ (-1 << workNodeBits)
	sequenceMask  = -1 ^ (-1 << sequenceBits)
	timestampShift = workNodeBits + sequenceBits
	workNodeShift  = sequenceBits
	epoch          = int64(1609459200000) // 2021-01-01 00:00:00 UTC in milliseconds
)

func NewSnowflake(worknode int64)(*Snowflake,error){
	if worknode<0||worknode>workNodeMax{
		return nil,fmt.Errorf("worknode must be between 0 and %d",workNodeMax)
	}
	return &Snowflake{
		lastTimestamp: 0,
		workNode:  worknode,
		sequence:  0,
	},nil
}

// NewSnowflakeOrAuto workNode>0 时按配置创建,否则自动派生
func NewSnowflakeOrAuto(workNode int64) (*Snowflake, error) {
	if workNode > 0 {
		return NewSnowflake(workNode)
	}
	return NewSnowflakeAuto()
}

// NewSnowflakeAuto 从本机 IP 末两段 + 进程号自动派生 workNode。
// 各服务硬编码同一个 WorkNode(如全是 1)时,多副本部署生成的 msgId 必然撞号;
// 自动派生在同一 IP 上按 PID 区分副本,跨机按 IP 区分。冲突概率极低但仍非零,
// 严格场景应改用 etcd/DB 分配节点号
func NewSnowflakeAuto() (*Snowflake, error) {
	node, err := deriveWorkNode()
	if err != nil {
		return nil, err
	}
	sf, err := NewSnowflake(node)
	if err != nil {
		return nil, err
	}
	logx.Infof("snowflake auto worknode=%d", node)
	return sf, nil
}

// AdvertiseHost 返回本机对外可达的非回环 IPv4;找不到返回空串
func AdvertiseHost() string {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return ""
	}
	for _, addr := range addrs {
		ipnet, ok := addr.(*net.IPNet)
		if !ok || ipnet.IP.IsLoopback() {
			continue
		}
		if ip4 := ipnet.IP.To4(); ip4 != nil {
			return ip4.String()
		}
	}
	return ""
}

func deriveWorkNode() (int64, error) {
	ipStr := AdvertiseHost()
	if ipStr == "" {
		// 找不到非回环 IPv4(极端环境):退化为 PID 派生
		return int64(os.Getpid()) % (workNodeMax + 1), nil
	}
	ip4 := net.ParseIP(ipStr).To4()
	// 末两段组合打散跨网段,再混入 PID 区分同机多副本
	v := int64(ip4[2])*251 + int64(ip4[3])*31 + int64(os.Getpid())
	return v % (workNodeMax + 1), nil
}

func (s *Snowflake) NextID() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := s.now()

	// 时钟回拨处理：如果当前时间小于上次时间戳，说明发生时钟回拨
	if now < s.lastTimestamp {
		// 策略：等待时钟追上（容忍5ms内的回拨，超过则报错）
		offset := s.lastTimestamp - now
		if offset <= 5 {
			time.Sleep(time.Duration(offset) * time.Millisecond)
			now = s.now()
		} else {
			// 严重时钟回拨，可以选择panic或返回错误
			// 生产环境建议接入告警系统
			panic(fmt.Sprintf("clock moved backwards: %d ms", offset))
		}
	}

	if now == s.lastTimestamp {
		// 同一毫秒内，序列号递增
		s.sequence = (s.sequence + 1) & sequenceMask
		if s.sequence == 0 {
			// 序列号溢出，等待下一毫秒
			for now <= s.lastTimestamp {
				now = s.now()
			}
		}
	} else {
		// 不同毫秒，序列号重置为0
		s.sequence = 0
	}

	s.lastTimestamp = now

	// 组合ID: (时间戳差 << 22) | (workerID << 12) | sequence
	id := ((now - epoch) << timestampShift) | (s.workNode << workNodeShift) | s.sequence
	return id
}

// now 获取当前时间戳（毫秒）
func (s *Snowflake) now() int64 {
	return time.Now().UnixMilli()
}

func (s *Snowflake) NextIDStr() string {
	return fmt.Sprintf("%d", s.NextID())
}

// ParseID 解析ID，提取时间戳、workerID、序列号
func ParseID(id int64) (timestamp int64, workerID int64, sequence int64) {
	timestamp = (id >> timestampShift) + epoch
	workerID = (id >> workNodeShift) & workNodeMax
	sequence = id & sequenceMask
	return
}

func GetIDTimestamp(id int64) time.Time {
	ts, _, _ := ParseID(id)
	return time.UnixMilli(ts)
}