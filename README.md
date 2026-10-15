# IM 即时通讯平台

> 基于 go-zero 的单仓微服务 IM 系统：WebSocket 长连接接入、单聊/群聊、消息同步、已读回执、消息撤回、文件分片上传。

[![Go Version](https://img.shields.io/badge/Go-1.25+-00ADD8?logo=go)](https://go.dev/)
[![go-zero](https://img.shields.io/badge/go--zero-v1.10+-blue)](https://go-zero.dev/)
[![Status](https://img.shields.io/badge/status-开发中-orange)]()

> ⚠️ **本项目处于开发阶段**：核心链路已跑通并联调，但接口、WS 协议与配置格式仍可能调整；生产部署、单元测试尚未就绪。当前 README 面向**本地开发与联调**。

---

## 📖 项目简介

单仓微服务架构，目标是覆盖 **接入 → 投递 → 存储 → 推送** 的完整 IM 链路：

- **im-api**：统一 HTTP API 层（REST :18888），聚合用户/群组/文件/消息能力，统一 Token 鉴权。
- **gateway**：WebSocket 网关（:18890），长连接接入、心跳保活、同设备互踢。
- **五个 RPC 服务**：`user` / `group` / `message` / `push` / `file`，按服务分库（PostgreSQL）。
- **消息链路**：`WriteDiffBundle` 单载荷经 Kafka（单 topic 单消费组）异步落库，会话内有序、幂等可重放。
- **桌面客户端**：[im-wails](https://github.com/your-org/im-wails)（Wails v2 + Vue 3，独立仓库 正在开发中）。

---

## 🚧 当前进度

### ✅ 已实现（可联调）

- 接入层：REST 40+ 端点统一鉴权，身份从 token 注入 ctx，不信任请求体
- 单聊 / 群聊文本消息：发送 → seq 分配 → Kafka 异步落库 → 实时推送 / 离线补拉全链路
- 消息同步：会话内单调 seq + 客户端驱动增量拉取，Redis 离线信箱按会话拆分
- 已读回执：会话级已读水位 + 收件箱批量置读 + 未读数联动清理
- 消息撤回：仅发送者可撤，落库 `recalled_by/recalled_at`
- 好友全链路：申请 / 接受 / 拒绝 / 删除 / 拉黑 / 搜索
- 群管理：建群 / 邀请 / 退群 / 踢人 / 禁言 / 设管理员 / 转让群主 / 解散 / 入群申请与审批（三级权限模型）
- 媒体消息：file_id 引用文件服务，发送链路校验文件存在/可用/归属
- 文件分片上传：S3 原生 Multipart，预签名直传 MinIO
- WS 网关：分桶连接管理、心跳、重连、顶号 FrameKick 通知
- 可靠性：消费端幂等（InsertIgnore / UpsertMaxSeq）、失败批次后台补偿、Redis SETNX 发送去重
- 可观测性：OTLP → Jaeger 全链路追踪，DevServer 暴露 Prometheus 指标与 pprof

### 🚧 半成品（待补全）

- 撤回的对方实时感知（目前落库 status=2，对方经 Sync 才能感知）
- 富媒体展示：客户端发图片/文件的上传三步流程未接入
- `im.event.notify` 事件广播 topic：已定义，尚无消费者（上下线/群变更通知未通）

### 📋 后续规划

统一收敛至文末 **[Roadmap](#-roadmap)**，按版本里程碑跟踪。

---

## 🧱 技术栈

| 类别 | 技术 |
|------|------|
| 语言 | Go 1.25 |
| 微服务框架 | go-zero（REST / zRPC） |
| 通信 | gRPC + Protobuf、WebSocket（gorilla） |
| 注册发现 / 配置 | etcd |
| 消息队列 | Kafka（IBM/sarama） |
| 数据库 | PostgreSQL |
| 缓存 | Redis |
| 对象存储 | MinIO / S3（aws-sdk-go-v2） |
| 分布式 ID | Snowflake（按 IP+PID 自动派生节点号） |
| 链路追踪 | OpenTelemetry + Jaeger |
| 桌面客户端 | Wails v2 + Vue 3（独立仓库） |

---

## 🏗️ 系统架构

```
                ┌───────────────────────────────┐
                │   客户端 (Wails 桌面 / Web)    │
                └───────┬───────────────┬───────┘
                        │ REST :18888   │ WebSocket :18890
                        ▼               │
        ┌───────────────────────┐       │
        │  im-api (HTTP BFF)    │       │
        │  统一鉴权 · API 聚合   │       │
        └──┬────────┬────────┬──┘       │
           │gRPC    │        │          │
           ▼        ▼        ▼          ▼
   ┌──────────┐ ┌─────────┐ ┌────────┐ ┌────────────────────┐
   │ user.rpc │ │group.rpc│ │file.rpc│ │  gateway.rpc       │
   │  :18002  │ │ :18003  │ │ :18005 │ │  :18001 (WS:18890) │
   └──────────┘ └─────────┘ └───┬────┘ └─────────▲──────────┘
        │            │          │                │ PushToConn
        │            │          ▼                │ BatchPushToConn
        │            │       MinIO/S3            │
        ▼            ▼                           │
   ┌──────────────────────┐                      │
   │     message.rpc      │                      │
   │       :18004         │                      │
   └──────┬─────────┬─────┘                      │
          │         │ 发送链路                    │
          │         └──────────────┐             │
          ▼                        ▼             │
   ┌─────────────┐          ┌────────────┐       │
   │    Kafka    │          │  push.rpc  │───────┘
   │im.msg.persist│         │   :18006   │  查 Redis 在线表
   └──────┬──────┘          │  (推送服务) │  直连各 gateway 实例
          │ 单消费组         └────────────┘
          ▼
   ┌─────────────┐     ┌──────────────────────────┐
   │ msg→seq→inbox│    │ PostgreSQL × N 库         │
   │ 按序落库     │     │ Redis(在线表/水位/未读)    │
   └─────────────┘     └──────────────────────────┘
```

---

## 📁 目录结构

```
im-platform/
├── app/
│   ├── im/api/              # HTTP API 层(REST :18888,统一鉴权与 API 聚合)
│   ├── gateway/
│   │   └── rpc/             # WebSocket 网关(gRPC :18001 + WS :18890)
│   │       ├── internal/conn/       # 连接管理器(分桶)
│   │       ├── internal/protocol/   # 二进制帧协议 [type:1][len:4][payload]
│   │       ├── internal/handler/    # WS 握手
│   │       └── internal/logic/      # 上行帧转发/连接注册
│   ├── user/rpc/            # 用户服务 :18002(账号/资料/设备/好友/黑名单)
│   ├── group/rpc/           # 群组服务 :18003(建群/成员管理/审批/权限)
│   ├── message/rpc/         # 消息服务 :18004(发送/同步/回执/撤回/seq)
│   │   └── internal/logic/  # 含 Kafka 消费落库与后台补偿器
│   ├── push/rpc/            # 推送服务 :18006(在线路由/批量投递/离线信箱/未读)
│   └── file/rpc/            # 文件服务 :18005(分片上传/元数据/预签名)
├── common/                  # 公共库
│   ├── constants/           # 常量与业务错误码
│   ├── middleware/          # 鉴权中间件(Token 校验/身份注入)
│   ├── mq/                  # Kafka 封装(sarama 生产/消费)
│   ├── hotconfig/           # etcd watch 热配置
│   └── utils/               # TokenManager/Snowflake/SeqCache 等
├── docs/
│   ├── jaeger-v2.yaml       # Jaeger v2 配置
│   └── sql/files.sql        # 文件服务建表脚本(其余服务表 DDL 待补)
├── test/binarygen.go        # WS 二进制帧联调工具
├── go.mod
└── README.md
```

---

## 🚀 本地开发与联调

> 以下流程面向**开发环境**，不是生产部署指南。

### 环境依赖

- Go 1.25+ / etcd 3.5+ / Kafka 3.x / PostgreSQL 15+ / Redis 7.x / MinIO / Jaeger（可选）

### 1. 初始化数据库

```bash
# 按各服务 yaml 中的 DSN 分库:im_user / im_group / im_message / im_file
createdb im_file   # 其余库同理

# 目前仓库内 DDL 仅覆盖文件服务,其余服务表结构参照各自 models 手工建表
psql -h 127.0.0.1 -U postgres -d im_file -f docs/sql/files.sql
```

### 2. 检查配置

各服务配置在 `app/<service>/rpc/etc/*.yaml`（HTTP 层在 `app/im/api/etc/gateway-api.yaml`），按需修改 etcd / PostgreSQL / Redis / Kafka / MinIO 地址。

### 3. 启动服务（建议按此顺序）

```bash
go run app/user/rpc/user.go         -f app/user/rpc/etc/user.yaml
go run app/group/rpc/group.go       -f app/group/rpc/etc/group.yaml
go run app/message/rpc/message.go   -f app/message/rpc/etc/message.yaml
go run app/file/rpc/file.go         -f app/file/rpc/etc/file.yaml
go run app/push/rpc/push.go         -f app/push/rpc/etc/push.yaml
go run app/gateway/rpc/gateway.go   -f app/gateway/rpc/etc/gateway.yaml
go run app/im/api/api.go            -f app/im/api/etc/gateway-api.yaml
```

### 4. 验证

```bash
curl http://127.0.0.1:18888/health

# 注册 + 登录
curl -X POST http://127.0.0.1:18888/register -d '{"phone":"13800000000","password":"your-password"}'
curl -X POST http://127.0.0.1:18888/login    -d '{"phone":"13800000000","password":"your-password"}'
# → {"token":"..."},后续请求携带 Authorization: Bearer <token>

# WebSocket 接入(浏览器无法带自定义 Header,token 走 query)
# ws://127.0.0.1:18890/ws?token=<token>
```

---

## 🔑 核心设计

### 消息链路（message.rpc）

- **seq**：Redis `INCRBY 100` 批量预取 + 本地号段缓存，PG 兜底，会话内单调递增且 key 永不过期。
- **落库**：`WriteDiffBundle`(msg→seq→inbox) 单载荷发 Kafka（key=conv_id 保证同会话分区有序），单消费组按序落库，写操作全幂等，Kafka 重投安全。
- **补偿**：Kafka 与 PG 兜底同时失败时整批转后台补偿器退避重试，丢弃计数告警。
- **已读**：`(会话, 阅读者)` 级水位 Lua 只进不退，收件箱按水位批量置读。

### WS 网关（gateway）

- 帧协议 `[type:1][len:4 大端][payload JSON]`：上行 0x02 发消息 / 0x03 回执 / 0x04 同步，下行 0x10 推送 / 0x11 踢下线 / 0x1F 错误。
- **离线补拉由客户端驱动**：建连后客户端发 `FrameSync`（带各会话本地水位）——多设备下只有客户端知道自己的进度，服务端不隐式补拉。
- 同设备互踢：先推 `FrameKick` 再断开，被顶设备可区分"被顶号"与"网络断"。

### 推送（push.rpc）

- 两级在线表：uid 级注册表 Hash + 设备级活性 key（90s 心跳续期）。
- GatewayPool 连接池：读写锁 + 双重检查，连续失败 3 次才逐出。
- 投递语义：任一设备实时送达即成功；全失败/离线转会话级离线信箱并计未读。

### 文件（file.rpc）

- 三步流：`CreateUploadTask`（file_id + 分片预签名 URL）→ 客户端直传 MinIO → `CompleteUpload`（ETag 合并）。
- 消息只引用 `content.file_id`，下载走预签名 URL（默认 15 分钟）；发送链路由 message.rpc 反查校验文件归属与状态。

---

## 🔭 可观测性与动态配置

六服务均内置 go-zero `ServiceConf` 的观测能力，**纯 yaml 开关、零代码接入**。

### 链路追踪（Jaeger，已启用）

所有服务已配置 `Telemetry`（OTLP gRPC → `127.0.0.1:4317`），跨服务调用自动串联 trace：

```bash
# 启动 Jaeger v2(OTel Collector 格式,内存存储,联调用)
jaeger --config docs/jaeger-v2.yaml
# UI: http://localhost:16686   生产把 memstore 换 ES,Sampler 降 0.1
```

### 指标 + 按需 pprof（DevServer，已启用）

每个服务一个调试口，`/metrics`（Prometheus 格式）+ `/debug/pprof` + `/healthz`：

| 服务 | user | message | group | push | file | gateway |
|------|------|---------|-------|------|------|---------|
| DevServer 端口 | 6061 | 6062 | 6063 | 6064 | 6065 | 6066 |

```bash
curl 127.0.0.1:6062/metrics                          # Prometheus 抓取
go tool pprof -http=:8080 http://127.0.0.1:6062/debug/pprof/profile?seconds=30   # CPU 火焰图
curl 127.0.0.1:6062/debug/pprof/goroutine?debug=1    # goroutine 泄漏排查
```

> ⚠️ pprof 端口只对内网，生产部署改 `Host` 或 `EnablePprof: false`。

### 持续 profiling（Pyroscope，已预留未启用）

各 yaml 中 `# Profiling:` 注释块已按服务预写好，起 `docker run -p 4040:4040 grafana/pyroscope` 后取消注释重启即可，CPU 超 70% 才自动采样。

### 动态配置（common/hotconfig，etcd watch）

运行时改配置不重启：watch 一个 etcd key，`PUT` 即原子替换配置快照，解析失败保留旧值。

```bash
etcdctl put im/hotcfg/message '{"send_rate_limit": 100, "log_level": "debug"}'
```

接入方法见包注释 `common/hotconfig/hotconfig.go`（泛型 + 变更回调，适合限流阈值/日志级别）。

---

## ⚠️ 已知限制

- 各 yaml 中的数据库/MinIO 凭证为**明文开发默认值**，不可用于生产，公开部署前务必环境变量化。
- 仓库内 DDL 仅覆盖 `files` 表，其余服务的建表脚本待补齐。
- 无单元测试与 CI；质量把关目前依赖 `go build` / `go vet`。
- 消息撤回、上下线、群变更暂无实时推送（事件 topic `im.event.notify` 已预留未接线）。

---

## 📝 配置速查

**端口规划**：接入层（对客户端）用 `188xx` 段；内部 RPC 按"被依赖 → 主动"的层级顺序用 `1800x` 段：

| 服务 | 端口 | 说明 |
|------|------|------|
| im-api (REST) | 18888 | HTTP API 层，对客户端 |
| gateway (WS) | 18890 | WebSocket 长连接，对客户端 |
| gateway.rpc | 18001 | 网关内部 gRPC（供 push 直连） |
| user.rpc | 18002 | 用户服务 |
| group.rpc | 18003 | 群组服务 |
| message.rpc | 18004 | 消息服务 |
| file.rpc | 18005 | 文件服务 |
| push.rpc | 18006 | 推送服务（依赖链最顶端） |

| 配置项 | 位置 | 说明 |
|--------|------|------|
| 监听端口 | `app/*/etc/*.yaml` `ListenOn` | 各服务监听端口 |
| etcd | `Etcd.Hosts` / `Key` | 服务注册与发现 |
| PostgreSQL | `Postgres.DataSource` | 按服务分库 |
| Redis | `RedisCache.Host` | 缓存 / 在线表 / 水位 |
| Kafka | `Kafka.Brokers` | 异步落库通道 |
| MinIO | `app/file/rpc/etc/file.yaml` `Minio` | 对象存储端点与桶 |
| Token TTL | `app/im/api/etc/gateway-api.yaml` `Token` | 登录态时长(小时) |
| 链路追踪 | `Telemetry` | OTLP Endpoint 与采样率 |

---

## 🧪 开发检查

```bash
go build ./...     # 编译
go vet ./...       # 静态检查
go run test/binarygen.go   # WS 二进制帧联调样例
```

---

## 🗺️ Roadmap

> 按里程碑组织；带"（已预留）"的条目表示代码/配置/协议层已埋点，只差接线。

### v0.1 — 生产可用基线

- [ ] 建表脚本全量入库：user/group/message 服务 DDL 补齐至 `docs/sql/`（pg_trgm 索引脚本已有）
- [ ] Docker Compose 一键部署：etcd / Kafka / PostgreSQL / Redis / MinIO / Jaeger 全家桶 + 各服务健康检查
- [ ] 配置安全化：数据库/MinIO 凭证环境变量化，接入密管（当前为明文开发默认值）
- [ ] 单元测试与 CI 流水线（`go build` / `go vet` / `go test` / 生成代码一致性检查）
- [ ] `im.event.notify` 消费者接线（已预留）：上下线 / 群变更实时通知
- [ ] 撤回实时感知：对方在线时推撤回事件帧，不再等 Sync 兜底
- [ ] 集中式限流：Redis 滑动窗口替代进程内桶（阈值经 hotconfig 热更新，已预留）
- [ ] 压测基线报告：WS 长连接数 / 消息吞吐 / 推送 P99（wrk + 自研帧工具）沉淀为文档

### v0.2 — 消息与多端体验增强

- [ ] 会话列表服务端化：多端消息漫游，新设备登录即得完整会话与未读
- [ ] 富媒体闭环：客户端接入文件三步上传流，图片/语音/视频/文件消息端到端可发
- [ ] 已读回执发送方实时感知：阅读方 Ack 后向发送方推已读事件（协议帧已预留）
- [ ] 好友在线状态订阅：上下线事件经 `im.event.notify` 推给关注者
- [ ] 消息搜索：服务端全文检索（pg_trgm / full-text，索引脚本已预留）
- [ ] Web 演示客户端：轻量 Web 端（复用 REST + WS 协议）降低联调门槛

### v1.0 — 平台化与治理

- [ ] 离线推送通道：APNs / FCM 厂商通道，App 被杀也能收到新消息提醒
- [ ] 内容审核接入：文件与消息双审核（审核状态机已预留），审核拒绝自动撤回
- [ ] 运营后台与治理：封禁 / 审计日志 / 敏感词
- [ ] 告警体系：Prometheus 告警规则 + 钉钉/飞书/Webhook 通知渠道，值班轮转
- [ ] 持续 profiling 上线：Pyroscope（配置已预留），与 Grafana 联动
- [ ] v1.0 稳定版发布：协议冻结，语义化版本

### 长期展望

- 端到端加密（密钥协商 + 消息体加密，服务端仅存密文）
- 音视频通话（WebRTC 信令复用现有 WS 网关）
- 消息冷热分层：历史消息归档 ClickHouse / 对象存储，检索与成本兼得
- 多租户 / SaaS 化：租户隔离与配额
- 跨区域异地多活：按用户分片的单元化部署

