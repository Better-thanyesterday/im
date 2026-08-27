package models

import "github.com/zeromicro/go-zero/core/stores/sqlx"

var _ GroupsettingsModel = (*customGroupsettingsModel)(nil)

type (
	// GroupsettingsModel is an interface to be customized, add more methods here,
	// and implement the added methods in customGroupsettingsModel.
	GroupsettingsModel interface {
		groupsettingsModel
		withSession(session sqlx.Session) GroupsettingsModel
	}

	customGroupsettingsModel struct {
		*defaultGroupsettingsModel
	}
)

// NewGroupsettingsModel returns a model for the database table.
func NewGroupsettingsModel(conn sqlx.SqlConn) GroupsettingsModel {
	return &customGroupsettingsModel{
		defaultGroupsettingsModel: newGroupsettingsModel(conn),
	}
}

func (m *customGroupsettingsModel) withSession(session sqlx.Session) GroupsettingsModel {
	return NewGroupsettingsModel(sqlx.NewSqlConnFromSession(session))
}
