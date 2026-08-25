package models

import "github.com/zeromicro/go-zero/core/stores/sqlx"

var _ SeqsModel = (*customSeqsModel)(nil)

type (
	// SeqsModel is an interface to be customized, add more methods here,
	// and implement the added methods in customSeqsModel.
	SeqsModel interface {
		seqsModel
		withSession(session sqlx.Session) SeqsModel
	}

	customSeqsModel struct {
		*defaultSeqsModel
	}
)

// NewSeqsModel returns a model for the database table.
func NewSeqsModel(conn sqlx.SqlConn) SeqsModel {
	return &customSeqsModel{
		defaultSeqsModel: newSeqsModel(conn),
	}
}

func (m *customSeqsModel) withSession(session sqlx.Session) SeqsModel {
	return NewSeqsModel(sqlx.NewSqlConnFromSession(session))
}
