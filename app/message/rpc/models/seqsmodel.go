package models

import (
	"github.com/zeromicro/go-zero/core/stores/cache"
	"github.com/zeromicro/go-zero/core/stores/sqlx"
)

var _ SeqsModel = (*customSeqsModel)(nil)

type (
	// SeqsModel is an interface to be customized, add more methods here,
	// and implement the added methods in customSeqsModel.
	SeqsModel interface {
		seqsModel
	}

	customSeqsModel struct {
		*defaultSeqsModel
	}
)

// NewSeqsModel returns a model for the database table.
func NewSeqsModel(conn sqlx.SqlConn, c cache.CacheConf, opts ...cache.Option) SeqsModel {
	return &customSeqsModel{
		defaultSeqsModel: newSeqsModel(conn, c, opts...),
	}
}
