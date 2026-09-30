-- ILIKE '%kw%' 无法走普通 B-tree 索引(全表扫描)。
-- 需要 pg_trgm + GIN 索引支持:
--   users.nickname:好友搜索按昵称模糊匹配
--   friends.remark:好友搜索按备注模糊匹配
-- 执行:psql -d im_user -f docs/sql/pg_trgm.sql

CREATE EXTENSION IF NOT EXISTS pg_trgm;

CREATE INDEX IF NOT EXISTS idx_users_nickname_trgm ON public.users USING gin (nickname gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_friends_remark_trgm ON public.friends USING gin (remark gin_trgm_ops);

-- 验证:EXPLAIN SELECT ... WHERE nickname ILIKE '%abc%' 应显示 Bitmap Index Scan
