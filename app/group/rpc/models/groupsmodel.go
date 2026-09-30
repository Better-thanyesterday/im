package models

import (
	"context"
	"fmt"

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
