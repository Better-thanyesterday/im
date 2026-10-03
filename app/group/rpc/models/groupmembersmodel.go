package models

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

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
		GetUserGroupIds(ctx context.Context, userId int64) ([]int64, error)
		FindByGroupAndUser(ctx context.Context, groupId, userId int64) (*Groupmembers, error)
		UpdateMuteUntil(ctx context.Context, groupId, userId int64, muteUntil time.Time) error
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

// GetUserGroupIds 用户所在的全部群 ID
func (m *customGroupmembersModel) GetUserGroupIds(ctx context.Context, userId int64) ([]int64, error) {
	query := fmt.Sprintf("select group_id from %s where user_id = $1", m.table)
	var ids []int64
	err := m.conn.QueryRowsCtx(ctx, &ids, query, userId)
	return ids, err
}

// FindByGroupAndUser 查单个成员行(角色/禁言判断用),不存在返回 ErrNotFound
func (m *customGroupmembersModel) FindByGroupAndUser(ctx context.Context, groupId, userId int64) (*Groupmembers, error) {
	query := fmt.Sprintf("select %s from %s where group_id = $1 and user_id = $2 limit 1", groupmembersRows, m.table)
	var resp Groupmembers
	err := m.conn.QueryRowCtx(ctx, &resp, query, groupId, userId)
	switch err {
	case nil:
		return &resp, nil
	case sqlx.ErrNotFound:
		return nil, ErrNotFound
	default:
		return nil, err
	}
}

// UpdateMuteUntil 设置/解除禁言:muteUntil 为零值时置 NULL(解除)。
// 不递增 member_version:禁言不影响成员列表,CheckMember 缓存靠自身 60s TTL 收敛
func (m *customGroupmembersModel) UpdateMuteUntil(ctx context.Context, groupId, userId int64, muteUntil time.Time) error {
	query := fmt.Sprintf("update %s set mute_until = $1, updated_at = now() where group_id = $2 and user_id = $3", m.table)
	res, err := m.conn.ExecCtx(ctx, query,
		sql.NullTime{Time: muteUntil, Valid: !muteUntil.IsZero()},
		groupId, userId,
	)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return ErrNotFound
	}
	return nil
}

// updateRole 定向更新单个成员角色(供 SetMemberRoleTx/TransferOwnerTx 在事务内经 session 调用)
func (m *defaultGroupmembersModel) updateRole(ctx context.Context, groupId, userId, role int64) error {
	query := fmt.Sprintf("update %s set role = $1, updated_at = now() where group_id = $2 and user_id = $3", m.table)
	res, err := m.conn.ExecCtx(ctx, query, role, groupId, userId)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return ErrNotFound
	}
	return nil
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
