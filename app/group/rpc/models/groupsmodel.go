package models

import "github.com/zeromicro/go-zero/core/stores/sqlx"

var _ GroupsModel = (*customGroupsModel)(nil)

type (
	// GroupsModel is an interface to be customized, add more methods here,
	// and implement the added methods in customGroupsModel.
	GroupsModel interface {
		groupsModel
		withSession(session sqlx.Session) GroupsModel
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
