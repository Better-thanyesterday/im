package models

import (
	"github.com/zeromicro/go-zero/core/stores/cache"
	"github.com/zeromicro/go-zero/core/stores/sqlx"
)

var _ InboxesModel = (*customInboxesModel)(nil)

type (
	// InboxesModel is an interface to be customized, add more methods here,
	// and implement the added methods in customInboxesModel.
	InboxesModel interface {
		inboxesModel
	}

	customInboxesModel struct {
		*defaultInboxesModel
	}
)

// NewInboxesModel returns a model for the database table.
func NewInboxesModel(conn sqlx.SqlConn, c cache.CacheConf, opts ...cache.Option) InboxesModel {
	return &customInboxesModel{
		defaultInboxesModel: newInboxesModel(conn, c, opts...),
	}
}
