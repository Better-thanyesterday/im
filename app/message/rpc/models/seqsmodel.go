package models

import (
	"context"
	"fmt"

	"github.com/zeromicro/go-zero/core/stores/sqlx"
)

var _ SeqsModel = (*customSeqsModel)(nil)

type (
	// SeqsModel is an interface to be customized, add more methods here,
	// and implement the added methods in customSeqsModel.
	SeqsModel interface {
		seqsModel
		withSession(session sqlx.Session) SeqsModel
		UpsertMaxSeq(ctx context.Context, convId string, maxSeq int64) error
	}

	customSeqsModel struct {
		*defaultSeqsModel
	}
)

// NewSeqsModel returns a model for the database table.
func NewSeqsModel(conn sqlx.SqlConn) SeqsModel {
	return &customSeqsModel{
		defaultSeqsModel: newSeqsModel(conn),
	}
}

func (m *customSeqsModel) withSession(session sqlx.Session) SeqsModel {
	return NewSeqsModel(sqlx.NewSqlConnFromSession(session))
}

// UpsertMaxSeq 消费端持久化 seq 事件:只推进不回退(GREATEST),可安全重放。
// 生成版 CustomQueryRowCtx 是"+1 分配器",消费端误用它会让 PG 与 Redis 永久脱节
func (m *customSeqsModel) UpsertMaxSeq(ctx context.Context, convId string, maxSeq int64) error {
	query := fmt.Sprintf(`INSERT INTO %s (conv_id, max_seq, updated_at)
		VALUES ($1, $2, NOW())
		ON CONFLICT (conv_id) DO UPDATE
		SET max_seq = GREATEST(%s.max_seq, EXCLUDED.max_seq),
		    updated_at = NOW()
		WHERE %s.max_seq < EXCLUDED.max_seq`, m.table, m.table, m.table)
	_, err := m.conn.ExecCtx(ctx, query, convId, maxSeq)
	return err
}
