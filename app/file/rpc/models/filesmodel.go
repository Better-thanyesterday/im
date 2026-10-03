package models

import (
	"context"
	"fmt"

	"github.com/zeromicro/go-zero/core/stores/sqlx"
)

var _ FilesModel = (*customFilesModel)(nil)

// 文件状态常量(与 proto GetFileMetaResp.status 对齐)
const (
	FileStatusNormal  int64 = 1 // 正常
	FileStatusAudit   int64 = 2 // 审核中(审核接入前不使用)
	FileStatusRefused int64 = 3 // 审核拒绝
	FileStatusDeleted int64 = 4 // 已删除
)

type (
	// FilesModel is an interface to be customized, add more methods here,
	// and implement the added methods in customFilesModel.
	FilesModel interface {
		filesModel
		withSession(session sqlx.Session) FilesModel
		UpdateStatus(ctx context.Context, id string, status int64) error
		UpdateAudit(ctx context.Context, id string, auditType, auditStatus int64, detail string, fileStatus int64) error
	}

	customFilesModel struct {
		*defaultFilesModel
	}
)

// NewFilesModel returns a model for the database table.
func NewFilesModel(conn sqlx.SqlConn) FilesModel {
	return &customFilesModel{
		defaultFilesModel: newFilesModel(conn),
	}
}

func (m *customFilesModel) withSession(session sqlx.Session) FilesModel {
	return NewFilesModel(sqlx.NewSqlConnFromSession(session))
}

// UpdateStatus 定向更新状态(CompleteUpload 的状态流转用),避免生成版 Update 全列覆盖
func (m *customFilesModel) UpdateStatus(ctx context.Context, id string, status int64) error {
	query := fmt.Sprintf("update %s set status = $2 where id = $1", m.table)
	res, err := m.conn.ExecCtx(ctx, query, id, status)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return ErrNotFound
	}
	return nil
}

// UpdateAudit 写入审核结果(审核服务接入后使用)
func (m *customFilesModel) UpdateAudit(ctx context.Context, id string, auditType, auditStatus int64, detail string, fileStatus int64) error {
	query := fmt.Sprintf("update %s set audit_type = $2, audit_status = $3, audit_detail = $4, audited_at = now(), status = $5 where id = $1", m.table)
	res, err := m.conn.ExecCtx(ctx, query, id, auditType, auditStatus, detail, fileStatus)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return ErrNotFound
	}
	return nil
}
