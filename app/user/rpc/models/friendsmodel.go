package models

import "github.com/zeromicro/go-zero/core/stores/sqlx"

var _ FriendsModel = (*customFriendsModel)(nil)

type (
	// FriendsModel is an interface to be customized, add more methods here,
	// and implement the added methods in customFriendsModel.
	FriendsModel interface {
		friendsModel
		withSession(session sqlx.Session) FriendsModel
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
