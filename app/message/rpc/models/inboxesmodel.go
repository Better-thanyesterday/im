package models

import "github.com/zeromicro/go-zero/core/stores/sqlx"

var _ InboxesModel = (*customInboxesModel)(nil)

type (
	// InboxesModel is an interface to be customized, add more methods here,
	// and implement the added methods in customInboxesModel.
	InboxesModel interface {
		inboxesModel
		withSession(session sqlx.Session) InboxesModel
	}

	customInboxesModel struct {
		*defaultInboxesModel
	}
)

// NewInboxesModel returns a model for the database table.
func NewInboxesModel(conn sqlx.SqlConn) InboxesModel {
	return &customInboxesModel{
		defaultInboxesModel: newInboxesModel(conn),
	}
}

func (m *customInboxesModel) withSession(session sqlx.Session) InboxesModel {
	return NewInboxesModel(sqlx.NewSqlConnFromSession(session))
}
