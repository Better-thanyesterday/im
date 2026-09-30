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
		InsertRelation(ctx context.Context, userId, friendId int64, remark string) error
		FindFriendsByUserId(ctx context.Context, userId int64, keyword string, page, pageSize int32) ([]friendInfoRow, error)
		CountFriends(ctx context.Context, userId int64, keyword string) (int64, error)
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

// InsertRelation 写入双向好友关系(status=1),remark 是接受方对申请方的备注
func (m *customFriendsModel) InsertRelation(ctx context.Context, userId, friendId int64, remark string) error {
	query := fmt.Sprintf(`insert into %s (user_id, friend_id, status, friend_group_id, remark)
		values ($1, $2, 1, null, $3), ($2, $1, 1, null, '')`, m.table)
	_, err := m.conn.ExecCtx(ctx, query, userId, friendId, remark)
	return err
}

// FindFriendsByUserId 分页查询好友列表(从 _gen.go 迁入并修复:
// 原实现 FROM friends_$suffix 字面量未替换、LIMIT/OFFSET 未绑定参数、
// Sprintf 内 % 未转义触发 go vet 报错)
func (m *customFriendsModel) FindFriendsByUserId(ctx context.Context, userId int64, keyword string, page, pageSize int32) ([]friendInfoRow, error) {
	if page <= 0 {
		page = 1
	}
	if pageSize <= 0 {
		pageSize = 20
	}
	offset := (page - 1) * pageSize
	query := fmt.Sprintf(`SELECT
		f.friend_id AS user_id,
		COALESCE(u.nickname, '') AS nickname,
		COALESCE(u.avatar, '') AS avatar,
		COALESCE(f.remark, '') AS remark,
		COALESCE(f.friend_group_id, 0) AS friend_group_id,
		EXTRACT(epoch FROM f.created_at) * 1000 AS created_at
	FROM %s f
	LEFT JOIN users u ON f.friend_id = u.id
	WHERE f.user_id = $1
	  AND f.status = 1
	  AND ($2 = '' OR u.nickname ILIKE '%%' || $2 || '%%' OR f.remark ILIKE '%%' || $2 || '%%')
	ORDER BY f.created_at DESC
	LIMIT $3 OFFSET $4`, m.table)
	var resp []friendInfoRow
	err := m.conn.QueryRowsCtx(ctx, &resp, query, userId, keyword, pageSize, offset)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// CountFriends 好友总数(与 FindFriendsByUserId 同过滤条件),
// 供分页 Total 使用;原实现直接返回当前页行数,前端永远翻不到页
func (m *customFriendsModel) CountFriends(ctx context.Context, userId int64, keyword string) (int64, error) {
	query := fmt.Sprintf(`select count(*)
	FROM %s f
	LEFT JOIN users u ON f.friend_id = u.id
	WHERE f.user_id = $1
	  AND f.status = 1
	  AND ($2 = '' OR u.nickname ILIKE '%%' || $2 || '%%' OR f.remark ILIKE '%%' || $2 || '%%')`, m.table)
	var n int64
	err := m.conn.QueryRowCtx(ctx, &n, query, userId, keyword)
	return n, err
}
