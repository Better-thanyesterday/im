package models

import "github.com/zeromicro/go-zero/core/stores/sqlx"

var _ GroupappliesModel = (*customGroupappliesModel)(nil)

type (
	// GroupappliesModel is an interface to be customized, add more methods here,
	// and implement the added methods in customGroupappliesModel.
	GroupappliesModel interface {
		groupappliesModel
		withSession(session sqlx.Session) GroupappliesModel
	}

	customGroupappliesModel struct {
		*defaultGroupappliesModel
	}
)

// NewGroupappliesModel returns a model for the database table.
func NewGroupappliesModel(conn sqlx.SqlConn) GroupappliesModel {
	return &customGroupappliesModel{
		defaultGroupappliesModel: newGroupappliesModel(conn),
	}
}

func (m *customGroupappliesModel) withSession(session sqlx.Session) GroupappliesModel {
	return NewGroupappliesModel(sqlx.NewSqlConnFromSession(session))
}
