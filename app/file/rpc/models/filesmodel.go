package models

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/zeromicro/go-zero/core/stores/sqlx"
)

// FilesModel 文件元数据模型（纯 sqlx，与 message 服务 models 风格一致）
type (
	FilesModel interface {
		Insert(ctx context.Context, data *Files) error
		FindOne(ctx context.Context, id string) (*Files, error)
		UpdateStatus(ctx context.Context, id string, status int64) error
		UpdateAudit(ctx context.Context, id string, auditType, auditStatus int64, detail string, fileStatus int64) error
		Delete(ctx context.Context, id string) error
	}

	defaultFilesModel struct {
		conn  sqlx.SqlConn
		table string
	}

	Files struct {
		Id          string         `db:"id"` // 雪花 file_id（字符串，对外暴露）
		FileName    string         `db:"file_name"`
		FileSize    int64          `db:"file_size"`
		MimeType    string         `db:"mime_type"`
		FileType    int64          `db:"file_type"` // 1-图片 2-语音 3-视频 4-文件
		UploaderId  int64          `db:"uploader_id"`
		UploadTime  time.Time      `db:"upload_time"`
		Status      int64          `db:"status"` // 1-正常 2-审核中 3-审核拒绝 4-已删除
		ObjectKey   string         `db:"object_key"`
		Bucket      string         `db:"bucket"`
		Thumbnails  sql.NullString `db:"thumbnails"` // JSON: {"origin":"...","thumb":"..."}
		AuditType   sql.NullInt64  `db:"audit_type"`
		AuditStatus sql.NullInt64  `db:"audit_status"`
		AuditDetail sql.NullString `db:"audit_detail"`
		AuditedAt   sql.NullTime   `db:"audited_at"`
	}
)

// 文件状态常量（与 proto GetFileMetaResp.status 对齐）
const (
	FileStatusNormal  int64 = 1 // 正常
	FileStatusAudit   int64 = 2 // 审核中
	FileStatusRefused int64 = 3 // 审核拒绝
	FileStatusDeleted int64 = 4 // 已删除
)

func NewFilesModel(conn sqlx.SqlConn) FilesModel {
	return &defaultFilesModel{
		conn:  conn,
		table: `"public"."files"`,
	}
}

func (m *defaultFilesModel) Insert(ctx context.Context, data *Files) error {
	query := `insert into %s (id, file_name, file_size, mime_type, file_type, uploader_id, upload_time, status, object_key, bucket)
		values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`
	_, err := m.conn.ExecCtx(ctx, fmt.Sprintf(query, m.table),
		data.Id, data.FileName, data.FileSize, data.MimeType, data.FileType,
		data.UploaderId, data.UploadTime, data.Status, data.ObjectKey, data.Bucket)
	return err
}

func (m *defaultFilesModel) FindOne(ctx context.Context, id string) (*Files, error) {
	var resp Files
	query := fmt.Sprintf("select id, file_name, file_size, mime_type, file_type, uploader_id, upload_time, status, object_key, bucket, thumbnails, audit_type, audit_status, audit_detail, audited_at from %s where id = $1 limit 1", m.table)
	err := m.conn.QueryRowCtx(ctx, &resp, query, id)
	switch err {
	case nil:
		return &resp, nil
	case sqlx.ErrNotFound:
		return nil, ErrNotFound
	default:
		return nil, err
	}
}

func (m *defaultFilesModel) UpdateStatus(ctx context.Context, id string, status int64) error {
	query := fmt.Sprintf("update %s set status = $2 where id = $1", m.table)
	_, err := m.conn.ExecCtx(ctx, query, id, status)
	return err
}

func (m *defaultFilesModel) UpdateAudit(ctx context.Context, id string, auditType, auditStatus int64, detail string, fileStatus int64) error {
	query := fmt.Sprintf("update %s set audit_type = $2, audit_status = $3, audit_detail = $4, audited_at = NOW(), status = $5 where id = $1", m.table)
	_, err := m.conn.ExecCtx(ctx, query, id, auditType, auditStatus, detail, fileStatus)
	return err
}

func (m *defaultFilesModel) Delete(ctx context.Context, id string) error {
	query := fmt.Sprintf("delete from %s where id = $1", m.table)
	_, err := m.conn.ExecCtx(ctx, query, id)
	return err
}
