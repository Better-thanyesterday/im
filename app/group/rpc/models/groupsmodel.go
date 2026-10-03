package models

import (
	"context"
	"database/sql"
	"fmt"

	"im-platform/app/group/rpc/group"

	"github.com/zeromicro/go-zero/core/stores/sqlx"
)

var _ GroupsModel = (*customGroupsModel)(nil)

type (
	// GroupsModel is an interface to be customized, add more methods here,
	// and implement the added methods in customGroupsModel.
	GroupsModel interface {
		groupsModel
		withSession(session sqlx.Session) GroupsModel
		IncrMembers(ctx context.Context, groupId, delta int64) error
		CreateGroupTx(ctx context.Context, g *Groups, members []*Groupmembers) error
		BumpVersion(ctx context.Context, groupId int64) error
		UpdateGroupInfoFields(ctx context.Context, groupId int64, name, avatar, notice *string, invitePermission, joinApproval, maxMember *int64) error
		MarkDissolved(ctx context.Context, groupId int64) error
		RemoveMemberTx(ctx context.Context, groupId, userId int64) error
		TransferOwnerTx(ctx context.Context, groupId, oldOwnerId, newOwnerId int64) error
		SetMemberRoleTx(ctx context.Context, groupId, userId, role int64) error
		JoinGroupTx(ctx context.Context, groupId int64, member *Groupmembers) (bool, error)
	}

	customGroupsModel struct {
		*defaultGroupsModel
	}
)

// NewGroupsModel returns a model for the database table.
func NewGroupsModel(conn sqlx.SqlConn) GroupsModel {
	return &customGroupsModel{
		defaultGroupsModel: newGroupsModel(conn),
	}
}

func (m *customGroupsModel) withSession(session sqlx.Session) GroupsModel {
	return NewGroupsModel(sqlx.NewSqlConnFromSession(session))
}

// CreateGroupTx 事务内建群 + 插入群主与初始成员,任一步失败整体回滚,
// 避免成员插入失败时留下没有群主的孤儿群
func (m *customGroupsModel) CreateGroupTx(ctx context.Context, g *Groups, members []*Groupmembers) error {
	return m.conn.TransactCtx(ctx, func(ctx context.Context, session sqlx.Session) error {
		sconn := sqlx.NewSqlConnFromSession(session)
		gm := &defaultGroupsModel{conn: sconn, table: m.table}
		if _, err := gm.Insert(ctx, g); err != nil {
			return err
		}
		mm := newGroupmembersModel(sconn)
		for _, mem := range members {
			if _, err := mm.Insert(ctx, mem); err != nil {
				return err
			}
		}
		return nil
	})
}

// IncrMembers 原子增减成员数并递增 member_version(驱动成员列表缓存失效)。
// 单条 SQL 自增,避免读-改-写在并发入群/退群时丢计数;
// 生成版 Update 是全列覆盖,不能只传 member_count
func (m *customGroupsModel) IncrMembers(ctx context.Context, groupId, delta int64) error {
	query := fmt.Sprintf("update %s set member_count = member_count + $1, member_version = member_version + 1, updated_at = now() where id = $2", m.table)
	res, err := m.conn.ExecCtx(ctx, query, delta, groupId)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return ErrNotFound
	}
	return nil
}

// BumpVersion 仅递增 member_version(驱动成员列表缓存失效)。
// 成员增删走 RemoveMemberTx/JoinGroupTx(内部已带版本递增),该方法供独立场景使用
func (m *customGroupsModel) BumpVersion(ctx context.Context, groupId int64) error {
	query := fmt.Sprintf("update %s set member_version = member_version + 1, updated_at = now() where id = $1", m.table)
	res, err := m.conn.ExecCtx(ctx, query, groupId)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return ErrNotFound
	}
	return nil
}

// UpdateGroupInfoFields 定向更新群资料:指针为 nil 的字段保持原值(COALESCE),
// 避免生成版 Update 全列覆盖把 owner_id/status 等清零
func (m *customGroupsModel) UpdateGroupInfoFields(ctx context.Context, groupId int64, name, avatar, notice *string, invitePermission, joinApproval, maxMember *int64) error {
	nullStr := func(s *string) sql.NullString {
		if s == nil {
			return sql.NullString{}
		}
		return sql.NullString{String: *s, Valid: true}
	}
	nullInt := func(i *int64) sql.NullInt64 {
		if i == nil {
			return sql.NullInt64{}
		}
		return sql.NullInt64{Int64: *i, Valid: true}
	}
	query := fmt.Sprintf(`update %s set
		name = coalesce($1, name),
		avatar = coalesce($2, avatar),
		notice = coalesce($3, notice),
		invite_permission = coalesce($4, invite_permission),
		join_approval = coalesce($5, join_approval),
		max_member = coalesce($6, max_member),
		updated_at = now()
		where id = $7`, m.table)
	res, err := m.conn.ExecCtx(ctx, query,
		nullStr(name), nullStr(avatar), nullStr(notice),
		nullInt(invitePermission), nullInt(joinApproval), nullInt(maxMember),
		groupId,
	)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return ErrNotFound
	}
	return nil
}

// MarkDissolved 标记群解散(软删:status + dissolved_at),同时递增 member_version
// 让所有成员的成员列表/群信息缓存失效
func (m *customGroupsModel) MarkDissolved(ctx context.Context, groupId int64) error {
	query := fmt.Sprintf(`update %s set status = 2, dissolved_at = now(),
		member_version = member_version + 1, updated_at = now() where id = $1`, m.table)
	res, err := m.conn.ExecCtx(ctx, query, groupId)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return ErrNotFound
	}
	return nil
}

// RemoveMemberTx 事务内退群/踢人:删成员行 + member_count-1 + member_version+1,
// 任一步失败整体回滚,避免计数与成员行不一致
func (m *customGroupsModel) RemoveMemberTx(ctx context.Context, groupId, userId int64) error {
	return m.conn.TransactCtx(ctx, func(ctx context.Context, session sqlx.Session) error {
		sconn := sqlx.NewSqlConnFromSession(session)
		res, err := sconn.ExecCtx(ctx,
			fmt.Sprintf("delete from %s where group_id = $1 and user_id = $2", `"public"."groupmembers"`),
			groupId, userId,
		)
		if err != nil {
			return err
		}
		if n, _ := res.RowsAffected(); n == 0 {
			return ErrNotFound
		}
		_, err = sconn.ExecCtx(ctx,
			fmt.Sprintf("update %s set member_count = member_count - 1, member_version = member_version + 1, updated_at = now() where id = $1", m.table),
			groupId,
		)
		return err
	})
}

// TransferOwnerTx 事务内转让群主:owner_id 换人(带 owner 条件防并发重复转让) +
// 新群主 role→OWNER + 旧群主降为普通成员 + 版本递增
func (m *customGroupsModel) TransferOwnerTx(ctx context.Context, groupId, oldOwnerId, newOwnerId int64) error {
	return m.conn.TransactCtx(ctx, func(ctx context.Context, session sqlx.Session) error {
		sconn := sqlx.NewSqlConnFromSession(session)
		res, err := sconn.ExecCtx(ctx,
			fmt.Sprintf("update %s set owner_id = $1, member_version = member_version + 1, updated_at = now() where id = $2 and owner_id = $3", m.table),
			newOwnerId, groupId, oldOwnerId,
		)
		if err != nil {
			return err
		}
		if n, _ := res.RowsAffected(); n == 0 {
			// owner_id 条件不满足 = 操作者已不是群主(并发转让)或群不存在
			return fmt.Errorf("only owner can transfer group")
		}
		mm := &defaultGroupmembersModel{conn: sconn, table: `"public"."groupmembers"`}
		if err := mm.updateRole(ctx, groupId, newOwnerId, int64(group.GroupRole_OWNER)); err != nil {
			return fmt.Errorf("new owner is not a member: %w", err)
		}
		return mm.updateRole(ctx, groupId, oldOwnerId, int64(group.GroupRole_MEMBER))
	})
}

// SetMemberRoleTx 事务内设/撤管理员:改 role + member_version+1(驱动成员缓存失效)
func (m *customGroupsModel) SetMemberRoleTx(ctx context.Context, groupId, userId, role int64) error {
	return m.conn.TransactCtx(ctx, func(ctx context.Context, session sqlx.Session) error {
		sconn := sqlx.NewSqlConnFromSession(session)
		mm := &defaultGroupmembersModel{conn: sconn, table: `"public"."groupmembers"`}
		if err := mm.updateRole(ctx, groupId, userId, role); err != nil {
			return err
		}
		res, err := sconn.ExecCtx(ctx,
			fmt.Sprintf("update %s set member_version = member_version + 1, updated_at = now() where id = $1", m.table),
			groupId,
		)
		if err != nil {
			return err
		}
		if n, _ := res.RowsAffected(); n == 0 {
			return ErrNotFound
		}
		return nil
	})
}

// JoinGroupTx 事务内直接入群(免审批):插入成员行(on conflict do nothing),
// 真正插入时 member_count+1、版本+1;返回 false 表示已是成员
func (m *customGroupsModel) JoinGroupTx(ctx context.Context, groupId int64, member *Groupmembers) (bool, error) {
	var inserted bool
	err := m.conn.TransactCtx(ctx, func(ctx context.Context, session sqlx.Session) error {
		sconn := sqlx.NewSqlConnFromSession(session)
		query := fmt.Sprintf(`insert into %s (id, group_id, user_id, role, group_nickname, join_time, mute_until, last_ack_seq)
			values ($1,$2,$3,$4,$5,$6,$7,$8) on conflict do nothing returning user_id`, `"public"."groupmembers"`)
		var uid int64
		err := sconn.QueryRowCtx(ctx, &uid, query,
			member.Id, member.GroupId, member.UserId, member.Role,
			member.GroupNickname, member.JoinTime, member.MuteUntil, member.LastAckSeq,
		)
		if err == sqlx.ErrNotFound {
			inserted = false
			return nil
		}
		if err != nil {
			return err
		}
		inserted = true
		_, err = sconn.ExecCtx(ctx,
			fmt.Sprintf("update %s set member_count = member_count + 1, member_version = member_version + 1, updated_at = now() where id = $1", m.table),
			groupId,
		)
		return err
	})
	return inserted, err
}
