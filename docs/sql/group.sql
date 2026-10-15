-- ============================================================================
-- group 服务建表脚本 (目标库: im_group)
-- 与 app/group/rpc/models/ 的模型结构对齐,与 2026-10 活库结构核对生成。
-- 幂等可重跑。
-- 执行: psql -h 127.0.0.1 -U postgres -d im_group -f docs/sql/group.sql
-- ============================================================================

-- ==================== groups 群表 ====================
-- id 为雪花 ID;软解散:status=2 + dissolved_at,成员行保留作历史
CREATE TABLE IF NOT EXISTS public.groups (
    id                bigint        PRIMARY KEY,
    name              varchar(128)  NOT NULL,
    avatar            varchar(512),
    owner_id          bigint        NOT NULL,
    notice            varchar(1024),
    member_count      integer       NOT NULL DEFAULT 1,                -- 原子自增维护(IncrMembers/事务方法),勿读-改-写
    max_member        integer       NOT NULL DEFAULT 500,
    member_version    bigint        NOT NULL DEFAULT 1,                -- 成员列表版本号,任何成员行变更 +1(客户端/服务缓存失效锚点)
    group_type        smallint      NOT NULL DEFAULT 1,                -- 1-普通群 2-企业群
    status            smallint      NOT NULL DEFAULT 1,                -- 1-正常 2-已解散
    invite_permission smallint      NOT NULL DEFAULT 1,                -- 1-所有人可邀请 2-仅管理员
    join_approval     smallint      NOT NULL DEFAULT 1,                -- 1-免审批 2-需审批
    created_at        timestamptz   NOT NULL DEFAULT now(),
    updated_at        timestamptz   NOT NULL DEFAULT now(),
    dissolved_at      timestamptz
);
CREATE INDEX IF NOT EXISTS idx_groups_owner  ON public.groups (owner_id, status);
CREATE INDEX IF NOT EXISTS idx_groups_status ON public.groups (status, created_at);

-- ==================== groupmembers 群成员表 ====================
-- (group_id, user_id) 唯一:同一成员在群内只有一行,是 BatchInsertIgnore/
-- JoinGroupTx 的 ON CONFLICT 生效前提
CREATE TABLE IF NOT EXISTS public.groupmembers (
    id             bigint        PRIMARY KEY,                          -- 雪花 ID
    group_id       bigint        NOT NULL,
    user_id        bigint        NOT NULL,
    role           smallint      NOT NULL DEFAULT 3,                   -- 1-群主 2-管理员 3-普通成员
    group_nickname varchar(64),
    join_time      timestamptz   NOT NULL DEFAULT now(),
    mute_until     timestamptz,                                        -- NULL=未禁言
    last_ack_seq   bigint        NOT NULL DEFAULT 0,
    created_at     timestamptz   NOT NULL DEFAULT now(),
    updated_at     timestamptz   NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX IF NOT EXISTS uk_group_members_group_user ON public.groupmembers (group_id, user_id);
CREATE INDEX IF NOT EXISTS idx_group_members_user ON public.groupmembers (user_id, role);

-- ==================== groupsettings 成员个人群设置表 ====================
CREATE TABLE IF NOT EXISTS public.groupsettings (
    id             bigint       PRIMARY KEY,
    user_id        bigint       NOT NULL,
    group_id       bigint       NOT NULL,
    is_mute        boolean      NOT NULL DEFAULT false,
    is_top         boolean      NOT NULL DEFAULT false,
    group_nickname varchar(64),
    notice_read_at timestamptz,
    created_at     timestamptz  NOT NULL DEFAULT now(),
    updated_at     timestamptz  NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_group_settings_user_top ON public.groupsettings (user_id, is_top DESC, updated_at);

-- ==================== groupapplies 入群申请表 ====================
CREATE TABLE IF NOT EXISTS public.groupapplies (
    id           bigint       PRIMARY KEY,                             -- 雪花 ID
    group_id     bigint       NOT NULL,
    applicant_id bigint       NOT NULL,
    inviter_id   bigint,                                              -- 邀请入群时的邀请人(主动申请为空)
    apply_type   smallint     NOT NULL DEFAULT 1,                     -- 1-主动申请 2-受邀
    apply_reason varchar(256),
    status       smallint     NOT NULL DEFAULT 1,                     -- 1-待处理 2-已接受 3-已拒绝
    handler_id   bigint,                                              -- 审批人
    created_at   timestamptz  NOT NULL DEFAULT now(),
    handled_at   timestamptz
);
CREATE INDEX IF NOT EXISTS idx_group_applies_applicant ON public.groupapplies (applicant_id, status);
CREATE INDEX IF NOT EXISTS idx_group_applies_group     ON public.groupapplies (group_id, status, created_at);
-- 部分唯一:同人对同群只允许一条待处理申请(HandleApply 幂等返回原 apply_id 依赖它)
CREATE UNIQUE INDEX IF NOT EXISTS uk_group_applies_pending
    ON public.groupapplies (group_id, applicant_id) WHERE status = 1;

-- ============================================================================
-- ★ 存量库修复(此前 groupmembers 缺 (group_id, user_id) 唯一索引,
--   重复邀请会插入重复成员行)。全新库无需执行本节。步骤:
--   1) 检测重复(必须先看): 
--      SELECT group_id, user_id, count(*) FROM public.groupmembers
--      GROUP BY group_id, user_id HAVING count(*) > 1;
--   2) 去重(保留每组 id 最小的一行;若群不存在重复行则 0 行受影响):
--      DELETE FROM public.groupmembers a USING public.groupmembers b
--       WHERE a.group_id = b.group_id AND a.user_id = b.user_id AND a.id > b.id;
--   3) 去重后同步群计数(member_count 与实际行数对齐),再建唯一索引:
--      CREATE UNIQUE INDEX IF NOT EXISTS uk_group_members_group_user
--          ON public.groupmembers (group_id, user_id);
-- ============================================================================
