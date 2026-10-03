package models

import (
	"context"
	"fmt"

	"github.com/zeromicro/go-zero/core/stores/sqlx"
)

var _ FriendappliesModel = (*customFriendappliesModel)(nil)

type (
	// FriendappliesModel is an interface to be customized, add more methods here,
	// and implement the added methods in customFriendappliesModel.
	FriendappliesModel interface {
		friendappliesModel
		withSession(session sqlx.Session) FriendappliesModel
		InsertApply(ctx context.Context, data *Friendapplies) (int64, error)
		AcceptFriendTx(ctx context.Context, applyId, applicantId, targetId int64, remark string) error
		RejectFriend(ctx context.Context, applyId, operatorId int64) error
	}

	customFriendappliesModel struct {
		*defaultFriendappliesModel
	}
)

// NewFriendappliesModel returns a model for the database table.
func NewFriendappliesModel(conn sqlx.SqlConn) FriendappliesModel {
	return &customFriendappliesModel{
		defaultFriendappliesModel: newFriendappliesModel(conn),
	}
}

func (m *customFriendappliesModel) withSession(session sqlx.Session) FriendappliesModel {
	return NewFriendappliesModel(sqlx.NewSqlConnFromSession(session))
}

// RejectFriend 拒绝好友申请:定向更新(带 status=1 前置条件防重复处理),
// 不动好友关系表;0 行受影响 = 申请不存在或已处理
func (m *customFriendappliesModel) RejectFriend(ctx context.Context, applyId, operatorId int64) error {
	query := fmt.Sprintf("update %s set status = 3, handler_id = $1, handled_at = now() where id = $2 and status = 1", m.table)
	res, err := m.conn.ExecCtx(ctx, query, operatorId, applyId)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return ErrNotFound
	}
	return nil
}

// InsertApply 插入申请并返回申请行 id(生成版 Insert 只返回 sql.Result,
// Postgres 下 lib/pq 不支持 LastInsertId)
func (m *customFriendappliesModel) InsertApply(ctx context.Context, data *Friendapplies) (int64, error) {
	query := fmt.Sprintf("insert into %s (applicant_id, target_id, apply_reason, status) values ($1, $2, $3, $4) returning id", m.table)
	var id int64
	err := m.conn.QueryRowCtx(ctx, &id, query, data.ApplicantId, data.TargetId, data.ApplyReason, data.Status)
	return id, err
}

// AcceptFriendTx 事务内完成接受申请:定向更新申请状态(带 status=1 前置条件,
// 防重复处理) + 写入双向好友关系。任一步失败整体回滚。
// 生成版 Update 是全列覆盖且原调用方没传 where 用的 Id,一直静默 no-op
func (m *customFriendappliesModel) AcceptFriendTx(ctx context.Context, applyId, applicantId, targetId int64, remark string) error {
	return m.conn.TransactCtx(ctx, func(ctx context.Context, session sqlx.Session) error {
		sconn := sqlx.NewSqlConnFromSession(session)
		upd := fmt.Sprintf("update %s set status = $1, handler_remark = $2, handled_at = now() where id = $3 and status = 1", m.table)
		res, err := sconn.ExecCtx(ctx, upd, 2, remark, applyId)
		if err != nil {
			return err
		}
		if n, _ := res.RowsAffected(); n == 0 {
			return ErrNotFound // 申请不存在或已被处理
		}
		fm := NewFriendsModel(sconn)
		return fm.InsertRelation(ctx, applicantId, targetId, remark)
	})
}
