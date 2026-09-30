package models

import (
	"context"
	"fmt"
	"strings"

	"github.com/zeromicro/go-zero/core/stores/sqlx"
)

var _ GroupmembersModel = (*customGroupmembersModel)(nil)

type (
	// GroupmembersModel is an interface to be customized, add more methods here,
	// and implement the added methods in customGroupmembersModel.
	GroupmembersModel interface {
		groupmembersModel
		withSession(session sqlx.Session) GroupmembersModel
		BatchInsertIgnore(ctx context.Context, rows []*Groupmembers) ([]int64, error)
	}

	customGroupmembersModel struct {
		*defaultGroupmembersModel
	}
)

// NewGroupmembersModel returns a model for the database table.
func NewGroupmembersModel(conn sqlx.SqlConn) GroupmembersModel {
	return &customGroupmembersModel{
		defaultGroupmembersModel: newGroupmembersModel(conn),
	}
}

func (m *customGroupmembersModel) withSession(session sqlx.Session) GroupmembersModel {
	return NewGroupmembersModel(sqlx.NewSqlConnFromSession(session))
}

// BatchInsertIgnore 批量插入成员(替代循环单条 INSERT),已存在的跳过,
// 返回实际插入的 user_id 列表(调用方据此得出 failed 名单)
func (m *customGroupmembersModel) BatchInsertIgnore(ctx context.Context, rows []*Groupmembers) ([]int64, error) {
	if len(rows) == 0 {
		return nil, nil
	}
	cols := 8
	placeholders := buildPlaceholders(len(rows), cols)
	query := fmt.Sprintf(`insert into %s (id, group_id, user_id, role, group_nickname, join_time, mute_until, last_ack_seq)
		values %s on conflict do nothing returning user_id`, m.table, placeholders)
	args := make([]interface{}, 0, len(rows)*cols)
	for _, r := range rows {
		args = append(args, r.Id, r.GroupId, r.UserId, r.Role, r.GroupNickname, r.JoinTime, r.MuteUntil, r.LastAckSeq)
	}
	var inserted []struct {
		UserId int64 `db:"user_id"`
	}
	if err := m.conn.QueryRowsCtx(ctx, &inserted, query, args...); err != nil {
		return nil, err
	}
	ids := make([]int64, 0, len(inserted))
	for _, r := range inserted {
		ids = append(ids, r.UserId)
	}
	return ids, nil
}

// buildPlaceholders 生成多行 VALUES 占位符 "($1,$2,...),($n,...)"
func buildPlaceholders(n, cols int) string {
	var b strings.Builder
	for i := 0; i < n; i++ {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString("(")
		for j := 0; j < cols; j++ {
			if j > 0 {
				b.WriteString(", ")
			}
			fmt.Fprintf(&b, "$%d", i*cols+j+1)
		}
		b.WriteString(")")
	}
	return b.String()
}
