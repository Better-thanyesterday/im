-- ============================================================================
-- message 服务建表脚本 (目标库: im_message)
-- 与 app/message/rpc/models/ 的模型结构对齐,与 2026-10 活库结构核对生成。
-- 幂等可重跑。
-- 执行: psql -h 127.0.0.1 -U postgres -d im_message -f docs/sql/message.sql
-- ============================================================================

-- ==================== messages 消息主表 ====================
-- content 为 MessageContent 的 JSONB;媒体消息只携带 file_id 引用,文件本体在 MinIO
CREATE TABLE IF NOT EXISTS public.messages (
    id          bigint        PRIMARY KEY,                             -- 雪花 msg_id
    convid      varchar(64)   NOT NULL,                                -- 单聊 min:max / 群聊 group_{gid}
    clientmsgid varchar(64)   NOT NULL,                                -- 客户端幂等键(UUID v4)
    senderid    bigint        NOT NULL,
    msgtype     smallint      NOT NULL,                                -- 1-文本 2-图片 3-语音 4-视频 5-文件 6-位置 7-系统
    content     jsonb         NOT NULL,
    seqid       bigint        NOT NULL,                                -- 会话内单调递增(同步锚点)
    sendtime    timestamptz   NOT NULL,
    status      smallint      NOT NULL DEFAULT 1,                      -- 1-正常 2-已撤回 3-已删除
    recalledby  bigint,                                               -- 撤回者(撤回审计)
    recalled_at timestamptz,
    extra       jsonb,                                                -- @提及/回复引用等扩展
    created_at  timestamptz   NOT NULL DEFAULT now()
);
-- 双幂等锚点:Kafka 重投(InsertIgnore)与客户端重试(23505 兜底反查)依赖
CREATE UNIQUE INDEX IF NOT EXISTS uk_messages_conv_seq ON public.messages (convid, seqid);
CREATE UNIQUE INDEX IF NOT EXISTS uk_messages_client   ON public.messages (clientmsgid);
-- 同步主路径:按会话+seq 范围拉取(FindBySeqRange / getServerSeq 的 max)
CREATE INDEX IF NOT EXISTS idx_messages_conv_time ON public.messages (convid, sendtime);

-- ==================== inboxes 收件箱表(写扩散) ====================
-- 小群写扩散:每个成员一行(含发送方 isread=true 的水位行);
-- (userid, msgid) 唯一是 BatchInsertIgnore 幂等(Kafka 重投不产生重复行/未读不翻倍)的前提
CREATE TABLE IF NOT EXISTS public.inboxes (
    id       bigint        PRIMARY KEY,
    userid   bigint        NOT NULL,
    msgid    bigint        NOT NULL,
    convid   varchar(64)   NOT NULL,
    seqid    bigint        NOT NULL,
    isread   boolean       NOT NULL DEFAULT false,
    readtime timestamptz,
    status   smallint      NOT NULL DEFAULT 1 CHECK (status IN (1, 2)),   -- 1-正常 2-已删除
    created_at timestamptz NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX IF NOT EXISTS uk_inboxes_user_msg ON public.inboxes (userid, msgid);
CREATE INDEX IF NOT EXISTS idx_inboxes_msgid ON public.inboxes (msgid);
-- 部分索引:只服务"未读查询/已读水位推进/会话投递水位"三条热路径
CREATE INDEX IF NOT EXISTS idx_inboxes_unread        ON public.inboxes (userid, convid, seqid) WHERE status = 1 AND isread = false;
CREATE INDEX IF NOT EXISTS idx_inboxes_user_conv_seq ON public.inboxes (userid, convid, seqid DESC) WHERE status = 1;

-- ==================== seqs 会话序号表 ====================
-- conv_id 主键;Redis 故障时的兜底分配器与消费端 upsert 目标(GREATEST 只进不退)
CREATE TABLE IF NOT EXISTS public.seqs (
    conv_id    varchar(64) PRIMARY KEY,
    max_seq    bigint      NOT NULL DEFAULT 0,
    updated_at timestamptz NOT NULL DEFAULT now()
);

-- ============================================================================
-- ★ 存量库修复(此前 inboxes 缺 (userid, msgid) 唯一索引,Kafka 重投会插重复行)。
--   全新库无需执行本节。步骤:
--   1) 检测重复:
--      SELECT userid, msgid, count(*) FROM public.inboxes
--      GROUP BY userid, msgid HAVING count(*) > 1;
--   2) 去重(保留每组 id 最小的一行):
--      DELETE FROM public.inboxes a USING public.inboxes b
--       WHERE a.userid = b.userid AND a.msgid = b.msgid AND a.id > b.id;
--   3) 建唯一索引:
--      CREATE UNIQUE INDEX IF NOT EXISTS uk_inboxes_user_msg ON public.inboxes (userid, msgid);
-- ============================================================================
