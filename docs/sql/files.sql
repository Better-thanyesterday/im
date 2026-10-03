-- file 服务建表脚本(与 app/file/rpc/models/filesmodel.go 的 Files 结构体严格对齐)
-- 目标库:im_file(见 app/file/rpc/etc/file.yaml)
-- 执行:psql -h 127.0.0.1 -U postgres -d im_file -f docs/sql/files.sql

CREATE TABLE IF NOT EXISTS public.files (
    id           varchar(32)  PRIMARY KEY,                      -- 雪花 file_id(字符串对外暴露)
    file_name    varchar(255) NOT NULL DEFAULT '',               -- 原始文件名
    file_size    bigint       NOT NULL DEFAULT 0,                -- 字节数
    mime_type    varchar(128) NOT NULL DEFAULT '',               -- MIME 类型
    file_type    int          NOT NULL DEFAULT 4,                -- 1-图片 2-语音 3-视频 4-文件
    uploader_id  bigint       NOT NULL,                          -- 上传者用户 ID
    upload_time  timestamptz  NOT NULL DEFAULT now(),            -- 上传时间
    status       int          NOT NULL DEFAULT 1,                -- 1-正常 2-审核中 3-审核拒绝 4-已删除
    object_key   varchar(512) NOT NULL DEFAULT '',               -- 对象存储 key
    bucket       varchar(128) NOT NULL DEFAULT '',               -- 存储桶
    thumbnails   jsonb        NULL,                              -- {"origin":"...","thumb":"..."}
    audit_type   bigint       NULL,                              -- 审核类型
    audit_status bigint       NULL,                              -- 审核结果状态
    audit_detail text         NULL,                              -- 审核详情
    audited_at   timestamptz  NULL                               -- 审核时间
);

COMMENT ON TABLE  public.files            IS '文件元数据表';
COMMENT ON COLUMN public.files.id          IS '雪花 file_id(字符串)';
COMMENT ON COLUMN public.files.file_type   IS '1-图片 2-语音 3-视频 4-文件';
COMMENT ON COLUMN public.files.status      IS '1-正常 2-审核中 3-审核拒绝 4-已删除';
COMMENT ON COLUMN public.files.thumbnails  IS '缩略图 JSON';
COMMENT ON COLUMN public.files.audit_type  IS '审核类型';
COMMENT ON COLUMN public.files.audit_status IS '审核结果状态';

-- 查询索引:按上传者列文件、按时间排序
CREATE INDEX IF NOT EXISTS idx_files_uploader_id ON public.files (uploader_id);
CREATE INDEX IF NOT EXISTS idx_files_upload_time ON public.files (upload_time DESC);
