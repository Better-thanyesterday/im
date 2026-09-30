package models

import (
	"context"
	"fmt"

	"github.com/zeromicro/go-zero/core/stores/sqlx"
)

var _ InboxesModel = (*customInboxesModel)(nil)

type (
	// InboxesModel is an interface to be customized, add more methods here,
	// and implement the added methods in customInboxesModel.
	InboxesModel interface {
		inboxesModel
		withSession(session sqlx.Session) InboxesModel
		BatchInsertIgnore(ctx context.Context, rows []*Inboxes) error
		MarkConvRead(ctx context.Context, userId int64, convId string, uptoSeq int64) error
	}

	customInboxesModel struct {
		*defaultInboxesModel
	}
)

// NewInboxesModel returns a model for the database table.
func NewInboxesModel(conn sqlx.SqlConn) InboxesModel {
	return &customInboxesModel{
		defaultInboxesModel: newInboxesModel(conn),
	}
}

func (m *customInboxesModel) withSession(session sqlx.Session) InboxesModel {
	return NewInboxesModel(sqlx.NewSqlConnFromSession(session))
}

// BatchInsertIgnore 幂等批量插入:命中唯一键((user_id,msg_id) 等)的行静默跳过。
// Kafka 重投时整批不会因个别重复行而整体失败
func (m *customInboxesModel) BatchInsertIgnore(ctx context.Context, rows []*Inboxes) error {
	if len(rows) == 0 {
		return nil
	}
	cols := 6
	placeholders := buildPlaceholders(len(rows), cols)
	query := fmt.Sprintf("INSERT INTO %s (userid, msgid, convid, seqid, isread, status) VALUES %s ON CONFLICT DO NOTHING",
		m.table, placeholders)
	args := make([]interface{}, 0, len(rows)*cols)
	for _, r := range rows {
		args = append(args, r.Userid, r.Msgid, r.Convid, r.Seqid, r.Isread, r.Status)
	}
	_, err := m.conn.ExecCtx(ctx, query, args...)
	return err
}

// MarkConvRead 按已读水位(seq)批量置已读,替代逐条更新。
// 未读数的持久层真源:unread = count where isread=false
func (m *customInboxesModel) MarkConvRead(ctx context.Context, userId int64, convId string, uptoSeq int64) error {
	query := fmt.Sprintf("update %s set isread = true, readtime = now() where userid = $1 and convid = $2 and isread = false and seqid <= $3", m.table)
	_, err := m.conn.ExecCtx(ctx, query, userId, convId, uptoSeq)
	return err
}
