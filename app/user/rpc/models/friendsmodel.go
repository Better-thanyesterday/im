package models

import (
	"context"
	"fmt"

	"github.com/zeromicro/go-zero/core/stores/sqlx"
)

var _ FriendsModel = (*customFriendsModel)(nil)

type (
	// FriendsModel is an interface to be customized, add more methods here,
	// and implement the added methods in customFriendsModel.
	FriendsModel interface {
		friendsModel
		withSession(session sqlx.Session) FriendsModel
		ExistsFriend(ctx context.Context, userId, friendId int64) (bool, error)
		IsBlocked(ctx context.Context, userId, targetId int64) (bool, error)
		UpdateRelationStatus(ctx context.Context, userId, friendId int64, status int64) error
		DeleteRelation(ctx context.Context, userId, friendId int64) error
	}

	customFriendsModel struct {
		*defaultFriendsModel
	}
)

// NewFriendsModel returns a model for the database table.
func NewFriendsModel(conn sqlx.SqlConn) FriendsModel {
	return &customFriendsModel{
		defaultFriendsModel: newFriendsModel(conn),
	}
}

func (m *customFriendsModel) withSession(session sqlx.Session) FriendsModel {
	return NewFriendsModel(sqlx.NewSqlConnFromSession(session))
}

// ExistsFriend 判断双方是否为已接受的好友（status=1 正常，2=被拉黑）
func (m *customFriendsModel) ExistsFriend(ctx context.Context, userId, friendId int64) (bool, error) {
	query := fmt.Sprintf("select count(1) from %s where user_id = $1 and friend_id = $2 and status = 1", m.table)
	var n int64
	if err := m.conn.QueryRowCtx(ctx, &n, query, userId, friendId); err != nil {
		return false, err
	}
	return n > 0, nil
}

// IsBlocked 判断 userId 是否拉黑了 targetId（user_id 维度的拉黑标记，status=2）
func (m *customFriendsModel) IsBlocked(ctx context.Context, userId, targetId int64) (bool, error) {
	query := fmt.Sprintf("select count(1) from %s where user_id = $1 and friend_id = $2 and status = 2", m.table)
	var n int64
	if err := m.conn.QueryRowCtx(ctx, &n, query, userId, targetId); err != nil {
		return false, err
	}
	return n > 0, nil
}

// UpdateRelationStatus 定向更新某条好友关系的 status（1=正常 2=拉黑）。
// 生成版 Update 的 where 只有 friend_id 且全列覆盖,会误伤其他用户的关系行
func (m *customFriendsModel) UpdateRelationStatus(ctx context.Context, userId, friendId int64, status int64) error {
	query := fmt.Sprintf("update %s set status = $1 where user_id = $2 and friend_id = $3", m.table)
	res, err := m.conn.ExecCtx(ctx, query, status, userId, friendId)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return ErrNotFound
	}
	return nil
}

// DeleteRelation 删除 A/B 双向的好友关系行。
// 生成版 Delete 的 where 只有 friend_id,会删掉所有用户与该好友的关系
func (m *customFriendsModel) DeleteRelation(ctx context.Context, userId, friendId int64) error {
	query := fmt.Sprintf("delete from %s where (user_id = $1 and friend_id = $2) or (user_id = $2 and friend_id = $1)", m.table)
	res, err := m.conn.ExecCtx(ctx, query, userId, friendId)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return ErrNotFound
	}
	return nil
}
