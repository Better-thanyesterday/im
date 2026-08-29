package models

import "github.com/zeromicro/go-zero/core/stores/sqlx"

var _ FriendgroupsModel = (*customFriendgroupsModel)(nil)

type (
	// FriendgroupsModel is an interface to be customized, add more methods here,
	// and implement the added methods in customFriendgroupsModel.
	FriendgroupsModel interface {
		friendgroupsModel
		withSession(session sqlx.Session) FriendgroupsModel
	}

	customFriendgroupsModel struct {
		*defaultFriendgroupsModel
	}
)

// NewFriendgroupsModel returns a model for the database table.
func NewFriendgroupsModel(conn sqlx.SqlConn) FriendgroupsModel {
	return &customFriendgroupsModel{
		defaultFriendgroupsModel: newFriendgroupsModel(conn),
	}
}

func (m *customFriendgroupsModel) withSession(session sqlx.Session) FriendgroupsModel {
	return NewFriendgroupsModel(sqlx.NewSqlConnFromSession(session))
}
