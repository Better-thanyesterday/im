package models

import (
	"context"
	"fmt"

	"github.com/zeromicro/go-zero/core/stores/sqlx"
)

var _ GroupappliesModel = (*customGroupappliesModel)(nil)

type (
	// GroupappliesModel is an interface to be customized, add more methods here,
	// and implement the added methods in customGroupappliesModel.
	GroupappliesModel interface {
		groupappliesModel
		withSession(session sqlx.Session) GroupappliesModel
		HandleApply(ctx context.Context, applyId, handlerId, status int64) error
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

// HandleApply 审批入群申请:status 2-接受 3-拒绝;定向更新并带 status=1 前置条件
// (防重复处理/并发审批),生成版 Update 是全列覆盖不能用。
// 0 行受影响 = 申请不存在或已被处理
func (m *customGroupappliesModel) HandleApply(ctx context.Context, applyId, handlerId, status int64) error {
	query := fmt.Sprintf("update %s set status = $1, handler_id = $2, handled_at = now() where id = $3 and status = 1", m.table)
	res, err := m.conn.ExecCtx(ctx, query, status, handlerId, applyId)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return ErrNotFound
	}
	return nil
}
