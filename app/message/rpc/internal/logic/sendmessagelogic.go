package logic

import (
	"context"
	"errors"
	"github.com/lib/pq"
	"github.com/zeromicro/go-zero/core/logx"
	"im-platform/app/message/rpc/internal/svc"
	"im-platform/app/message/rpc/message"
	"im-platform/common/constants"
	"strconv"
	"strings"
)

type SendMessageLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewSendMessageLogic(ctx context.Context, svcCtx *svc.ServiceContext) *SendMessageLogic {
	return &SendMessageLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

// 消息统一入口
func (l *SendMessageLogic) SendMessage(in *message.SendMessageReq) (*message.SendMessageResp, error) {
	// todo: add your logic here and delete this line
	//参数校验
	if err := l.validate(in); err != nil {
		return nil, err
	}
	// 3. 分布式限流：单用户 30 msg/s（窗口 1s）
	// if err := l.checkRateLimit(in.SenderId); err != nil {
	// 	l.Infof("rate limit hit, uid=%d", in.SenderId)
	// 	return nil, constants.NewErrCode(constants.ErrMsgRateLimit) // 300001
	// }

	// 4. 幂等检查（Redis 第一层）
	//    命中 → 查 PG 返回原消息（保证客户端重试拿到相同结果）
	isDup, existMsg, err := l.svcCtx.DedupModel.CheckAndGet(l.ctx, in.Body.ConvId, in.ClientMsgId)
	if err != nil {
		logx.Errorf("dedup check err: %v", err)
		// Redis 故障不阻断，降级到下游 PG 唯一索引兜底
	} else if isDup {
		l.Infof("dedup hit, client_msg_id=%s", in.ClientMsgId)
		return &message.SendMessageResp{
			MsgId:    existMsg.Id,
			SeqId:    existMsg.Seqid,
			ConvId:   existMsg.Convid,
			SendTime: existMsg.Sendtime.UnixMilli(),
			Isdup: isDup,
		}, nil
	}

	// 5. 类型分发：单聊 / 群聊
	var resp *message.SendMessageResp
	switch in.Isgroup {
	case false:
		singleLogic := NewSingleChatLogic(l.ctx, l.svcCtx)
		resp, err = singleLogic.Send(in, in.Body.ConvId)
	case true:
		groupLogic := NewGroupChatLogic(l.ctx, l.svcCtx)
		resp, err = groupLogic.Send(in, in.Body.ConvId)
	default:
		return nil, constants.NewMsgError(constants.ErrCodeMsgInValidParam)
	}

	// 6. 并发兜底：如果因重复请求导致 PG 唯一索引冲突，降级为查询返回
	if err != nil {
		var pgErr *pq.Error
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			msg, dbErr := l.svcCtx.MessagesModel.FindByClientMsgId(l.ctx, in.Body.ConvId, in.ClientMsgId)
			if dbErr != nil {
				return nil, dbErr
			}
			return &message.SendMessageResp{
				MsgId:    msg.Id,
				SeqId:    msg.Seqid,
				ConvId:   msg.Convid,
				SendTime: msg.Sendtime.UnixMilli(),
			}, nil
		}
		return nil, err
	}

	// 7. 异步写入幂等缓存（不阻塞响应）
	go func() {
		ctx := context.Background()
		if err := l.svcCtx.DedupModel.Set(ctx, in.Body.ConvId, in.ClientMsgId); err != nil {
			logx.Errorf("dedup set err: %v", err)
		}
	}()
	return resp, nil
}

func (l *SendMessageLogic) validate(in *message.SendMessageReq) error {
	if in.ClientMsgId == "" || in.SenderId <= 0 || in.Body == nil {
		return constants.NewMsgError(constants.ErrCodeMsgInValidParam)
	}
	if len(in.Body.ConvId) <= 0 {
		return constants.NewMsgError(constants.ErrCodeMsgInValidParam)
	}
	switch in.Isgroup {
	case false:
		if !isGroupConv(in.Body.ConvId) {
			parts := strings.Split(in.Body.ConvId, ":")
			if len(parts) != 2 {
				return constants.NewMsgError(constants.ErrCodeMsgInValidParam)
			}
			min_uid, err1 := strconv.ParseInt(parts[0], 10, 64)
			max_uid, err2 := strconv.ParseInt(parts[1], 10, 64)
			if min_uid <= 0 || max_uid <= 0 || err1 != nil || err2 != nil {
				return constants.NewMsgError(constants.ErrCodeMsgInValidParam)
			}
		} else {
			return constants.NewMsgError(constants.ErrCodeMsgInValidParam)
		}
	case true:
		if isGroupConv(in.Body.ConvId) {
			groupId, err := strconv.ParseInt(strings.TrimPrefix(in.Body.ConvId, "group_"), 10, 64)
			if err != nil || groupId <= 0 {
				return constants.NewMsgError(constants.ErrCodeMsgInValidParam)
			}
		} else {
			return constants.NewMsgError(constants.ErrCodeMsgInValidParam)
		}
	default:
		return constants.NewMsgError(constants.ErrCodeMsgInValidParam)
	}
	return nil
}

func isGroupConv(convId string) bool {
	return strings.HasPrefix(convId, "group_")
}

// checkRateLimit 基于 Redis 滑动窗口的分布式限流
// func (l *SendMessageLogic) checkRateLimit(userId int64) error {
// 	key := fmt.Sprintf("im:ratelimit:send:%d", userId)
// 	const (
// 		windowMs int64 = 1000 // 1 秒窗口
// 		limit    int64 = 30   // 30 条/秒（生产可配到 etcd）
// 	)
// 	now := time.Now().UnixMilli()
// 	member := fmt.Sprintf("%d-%s", now, idgen.RandomString(6))

// 	// Lua 保证：清理过期窗口 → 计数 → 添加（全原子）
// 	lua := `
// 		local key = KEYS[1]
// 		local window = tonumber(ARGV[1])
// 		local limit = tonumber(ARGV[2])
// 		local now = tonumber(ARGV[3])
// 		local member = ARGV[4]

// 		redis.call('ZREMRANGEBYSCORE', key, 0, now - window)
// 		local current = redis.call('ZCARD', key)
// 		if current >= limit then
// 			return 0
// 		end
// 		redis.call('ZADD', key, now, member)
// 		redis.call('EXPIRE', key, math.ceil(window / 1000) + 1)
// 		return 1
// 	`
// 	val, err := l.svcCtx.Redis.Eval(lua, []string{key},
// 		strconv.FormatInt(windowMs, 10),
// 		strconv.FormatInt(limit, 10),
// 		strconv.FormatInt(now, 10),
// 		member,
// 	)
// 	if err != nil {
// 		logx.Errorf("rate limit eval err: %v", err)
// 		// 限流器故障，保守降级为放行（避免误杀正常消息）
// 		return nil
// 	}
// 	if val.(int64) == 0 {
// 		return constants.NewErrCode(constants.ErrMsgRateLimit)
// 	}
// 	return nil
// }
