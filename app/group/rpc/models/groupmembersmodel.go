package models

import "github.com/zeromicro/go-zero/core/stores/sqlx"

var _ GroupmembersModel = (*customGroupmembersModel)(nil)

type (
	// GroupmembersModel is an interface to be customized, add more methods here,
	// and implement the added methods in customGroupmembersModel.
	GroupmembersModel interface {
		groupmembersModel
		withSession(session sqlx.Session) GroupmembersModel
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
