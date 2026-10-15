-- ============================================================================
-- user 服务建表脚本 (目标库: im_user)
-- 与 app/user/rpc/models/ 的模型结构对齐,与 2026-10 活库结构核对生成。
-- 幂等可重跑(CREATE ... IF NOT EXISTS)。
-- 执行: psql -h 127.0.0.1 -U postgres -d im_user -f docs/sql/user.sql
-- ============================================================================

-- 好友搜索 ILIKE '%kw%' 依赖 pg_trgm 加速(GetFriends 的 nickname/remark 模糊匹配)
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- ==================== users 用户表 ====================
-- id 为雪花 ID,由服务端生成,无自增默认值
CREATE TABLE IF NOT EXISTS public.users (
    id            bigint        PRIMARY KEY,
    account       bigint        NOT NULL,                          -- 账号 ID(注册时下发,登录可用)
    phone         varchar(20)   NOT NULL,
    email         varchar(128)  NOT NULL,
    password_hash varchar(255)  NOT NULL,                          -- bcrypt
    nickname      varchar(64)   NOT NULL,
    avatar        varchar(255)  NOT NULL DEFAULT '',
    signature     varchar(255)  NOT NULL DEFAULT '',
    -- 注:历史上为 varchar,代码按数字字符串读写(strconv.ParseInt);新部署可改 smallint,需同步代码
    gender        varchar(10)   NOT NULL DEFAULT '',
    birthday      date          NOT NULL DEFAULT '1970-01-01',
    region        varchar(255)  NOT NULL DEFAULT '',
    status        smallint,                                        -- NULL/1-正常 2-冻结 3-注销
    created_at    timestamp     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at    timestamp     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    deleted_at    timestamp                                        -- 软删标记(唯一索引引用)
);

-- 手机/邮箱唯一:仅约束未注销账号(注销后可释放凭证供重新注册)
CREATE UNIQUE INDEX IF NOT EXISTS uk_users_phone ON public.users (phone) WHERE deleted_at IS NULL;
CREATE UNIQUE INDEX IF NOT EXISTS uk_users_email ON public.users (email) WHERE deleted_at IS NULL;

-- ==================== devices 登录设备表 ====================
CREATE TABLE IF NOT EXISTS public.devices (
    id                 bigint        PRIMARY KEY,
    userid             bigint        NOT NULL REFERENCES public.users(id),
    devicetype         smallint      NOT NULL CHECK (devicetype BETWEEN 1 AND 5),  -- 1-iOS 2-Android 3-Web 4-PC 5-小程序
    deviceid           varchar(128)  NOT NULL,
    devicename         varchar(128),
    location           varchar(256),
    login_at           timestamptz,
    logout_at          timestamptz,
    last_active_at     timestamptz,
    access_token_hash  varchar(512),                                    -- 预留:token 哈希落库
    refresh_token_hash varchar(512),                                    -- 预留
    status             smallint      NOT NULL DEFAULT 1 CHECK (status BETWEEN 1 AND 3), -- 1-正常 2-失效 3-被踢
    created_at         timestamptz   NOT NULL DEFAULT now(),
    updated_at         timestamptz   NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_devices_user ON public.devices (userid);

-- ==================== friends 好友关系表 ====================
-- 复合主键 (user_id, friend_id):同一好友对全局唯一,任何人都可加 B 为好友;
-- 双向关系 = 两行(A→B 与 B→A),由 AcceptFriendTx 同事务写入
CREATE TABLE IF NOT EXISTS public.friends (
    user_id        bigint       NOT NULL,                             -- 关系的属主
    friend_id      bigint       NOT NULL,                             -- 好友
    status         smallint     NOT NULL DEFAULT 1,                   -- 1-正常 2-已拉黑
    friend_group_id bigint,                                           -- 所属好友分组(可空)
    remark         varchar(64),                                       -- 属主给好友的备注
    created_at     timestamptz  NOT NULL DEFAULT now(),
    updated_at     timestamptz  NOT NULL DEFAULT now(),
    PRIMARY KEY (user_id, friend_id)
);
CREATE INDEX IF NOT EXISTS idx_friends_friend_id ON public.friends (friend_id);
-- 备注/昵称模糊搜索(GetFriends keyword)加速
CREATE INDEX IF NOT EXISTS idx_friends_remark_trgm ON public.friends USING gin (remark gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_users_nickname_trgm ON public.users USING gin (nickname gin_trgm_ops);

-- ==================== friendgroups 好友分组表 ====================
CREATE TABLE IF NOT EXISTS public.friendgroups (
    id         bigint       PRIMARY KEY,
    user_id    bigint       NOT NULL,
    name       varchar(64)  NOT NULL,
    sort_order integer      NOT NULL DEFAULT 0,
    created_at timestamptz  NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_friend_groups_user ON public.friendgroups (user_id, sort_order);

-- ==================== friendapplies 好友申请表 ====================
CREATE TABLE IF NOT EXISTS public.friendapplies (
    id             bigint       PRIMARY KEY,
    applicant_id   bigint       NOT NULL,
    target_id      bigint       NOT NULL,
    apply_reason   varchar(256),
    status         smallint     NOT NULL DEFAULT 1,                  -- 1-待处理 2-已接受 3-已拒绝
    handler_remark varchar(64),                                       -- 接受时的备注名
    created_at     timestamptz  NOT NULL DEFAULT now(),
    handled_at     timestamptz
);
CREATE INDEX IF NOT EXISTS idx_friend_applies_applicant ON public.friendapplies (applicant_id, created_at);
CREATE INDEX IF NOT EXISTS idx_friend_applies_target    ON public.friendapplies (target_id, status, created_at);
-- 部分唯一:同一申请人对同一目标只允许一条"待处理"申请,已处理的不再拦截
CREATE UNIQUE INDEX IF NOT EXISTS uk_friend_applies_pending
    ON public.friendapplies (applicant_id, target_id) WHERE status = 1;

-- ============================================================================
-- ★ 存量库修复(2026-10 之前的库 friends 主键误建为单列 friend_id,
--   导致同一用户只能被一个人加为好友)。全新库无需执行本节。
--   复合主键包含 friend_id,存量数据必然满足唯一性,可直接切换:
-- ============================================================================
-- ALTER TABLE public.friends DROP CONSTRAINT friends_pkey;
-- ALTER TABLE public.friends ADD  CONSTRAINT friends_pkey PRIMARY KEY (user_id, friend_id);
