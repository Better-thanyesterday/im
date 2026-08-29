package models

import "github.com/zeromicro/go-zero/core/stores/sqlx"

var _ FriendappliesModel = (*customFriendappliesModel)(nil)

type (
	// FriendappliesModel is an interface to be customized, add more methods here,
	// and implement the added methods in customFriendappliesModel.
	FriendappliesModel interface {
		friendappliesModel
		withSession(session sqlx.Session) FriendappliesModel
	}

	customFriendappliesModel struct {
		*defaultFriendappliesModel
	}
)

// NewFriendappliesModel returns a model for the database table.
func NewFriendappliesModel(conn sqlx.SqlConn) FriendappliesModel {
	return &customFriendappliesModel{
		defaultFriendappliesModel: newFriendappliesModel(conn),
	}
}

func (m *customFriendappliesModel) withSession(session sqlx.Session) FriendappliesModel {
	return NewFriendappliesModel(sqlx.NewSqlConnFromSession(session))
}
