package models

import (
	"context"
	"fmt"

	"github.com/zeromicro/go-zero/core/stores/sqlx"
)

var _ MessagesModel = (*customMessagesModel)(nil)

type (
	// MessagesModel is an interface to be customized, add more methods here,
	// and implement the added methods in customMessagesModel.
	MessagesModel interface {
		messagesModel
		InsertIgnore(ctx context.Context, data *Messages) error
		MaxSeq(ctx context.Context, convId string) (int64, error)
	}

	customMessagesModel struct {
		*defaultMessagesModel
	}
)

// NewMessagesModel returns a model for the database table.
func NewMessagesModel(conn sqlx.SqlConn) MessagesModel {
	return &customMessagesModel{
		defaultMessagesModel: newMessagesModel(conn),
	}
}

// InsertIgnore 幂等插入:命中任意唯一键((convid,seqid)/(client_msg_id)/id)时静默跳过。
// Kafka 消费端必须幂等,Kafka 重投不能变成唯一键冲突风暴
func (m *customMessagesModel) InsertIgnore(ctx context.Context, data *Messages) error {
	query := fmt.Sprintf("insert into %s (%s) values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12) on conflict do nothing",
		m.table, messagesRowsExpectAutoSet)
	_, err := m.conn.ExecCtx(ctx, query, data.Id, data.Convid, data.Clientmsgid, data.Senderid, data.Msgtype, data.Content, data.Seqid, data.Sendtime, data.Status, data.Recalledby, data.RecalledAt, data.Extra)
	return err
}

// MaxSeq 会话当前最大 seq,以 messages 表为准。
// Redis im:seq 计数器是 +100 批量预分配的 padding 值,直接当 serverSeq 用
// 会造成 diff 虚高、反复空拉
func (m *customMessagesModel) MaxSeq(ctx context.Context, convId string) (int64, error) {
	query := fmt.Sprintf("select coalesce(max(seqid), 0) from %s where convid = $1", m.table)
	var maxSeq int64
	err := m.conn.QueryRowCtx(ctx, &maxSeq, query, convId)
	return maxSeq, err
}
