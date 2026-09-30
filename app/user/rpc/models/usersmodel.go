package models

import (
	"context"
	"fmt"
	"time"

	"github.com/zeromicro/go-zero/core/stores/sqlx"
)

var _ UsersModel = (*customUsersModel)(nil)

type (
	// UsersModel is an interface to be customized, add more methods here,
	// and implement the added methods in customUsersModel.
	UsersModel interface {
		usersModel
		withSession(session sqlx.Session) UsersModel
		UpdatePassword(ctx context.Context, id int64, passwordHash string) error
		UpdateProfileFields(ctx context.Context, id int64, nickname, avatar, signature, gender, region string, birthday time.Time) error
	}

	customUsersModel struct {
		*defaultUsersModel
	}
)

// NewUsersModel returns a model for the database table.
func NewUsersModel(conn sqlx.SqlConn) UsersModel {
	return &customUsersModel{
		defaultUsersModel: newUsersModel(conn),
	}
}

func (m *customUsersModel) withSession(session sqlx.Session) UsersModel {
	return NewUsersModel(sqlx.NewSqlConnFromSession(session))
}

// Update 覆写生成版的 Update:生成版是全列覆盖且 where id=0 会静默 no-op,
// 这里先拒绝 Id<=0 的残缺结构体,全列覆盖语义保留给"FindOne 后改字段再写回"的调用方
func (m *customUsersModel) Update(ctx context.Context, newData *Users) error {
	if newData.Id <= 0 {
		return fmt.Errorf("users update: id must be positive, got %d", newData.Id)
	}
	return m.defaultUsersModel.Update(ctx, newData)
}

// UpdatePassword 定向更新密码,避免全列覆盖把 email/phone/nickname 等清零
func (m *customUsersModel) UpdatePassword(ctx context.Context, id int64, passwordHash string) error {
	query := fmt.Sprintf("update %s set password_hash = $1 where id = $2", m.tableName())
	res, err := m.conn.ExecCtx(ctx, query, passwordHash, id)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return ErrNotFound
	}
	return nil
}

// UpdateProfileFields 定向更新资料字段,只影响所列列
func (m *customUsersModel) UpdateProfileFields(ctx context.Context, id int64, nickname, avatar, signature, gender, region string, birthday time.Time) error {
	if id <= 0 {
		return fmt.Errorf("users update profile: id must be positive, got %d", id)
	}
	query := fmt.Sprintf("update %s set nickname = $1, avatar = $2, signature = $3, gender = $4, region = $5, birthday = $6 where id = $7", m.tableName())
	res, err := m.conn.ExecCtx(ctx, query, nickname, avatar, signature, gender, region, birthday, id)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return ErrNotFound
	}
	return nil
}
