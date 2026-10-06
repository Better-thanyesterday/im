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
- **桌面客户端**：[im-wails](https://github.com/your-org/im-wails)（Wails v2 + Vue 3，独立仓库）。

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

### 🚧 半成品（有后端、待补全）

- 撤回的对方实时感知（目前落库 status=2，对方经 Sync 才能感知）
- 入群申请审批：后端 RPC 完整，客户端尚无 UI
- 富媒体展示：客户端发图片/文件的上传三步流程未接入
- `im.event.notify` 事件广播 topic：已定义，尚无消费者（上下线/群变更通知未通）

### 📋 规划中（未开始）

- [ ] 内容审核（文件与消息，审核状态机已预留）
- [ ] 生产部署：Docker Compose / K8s、配置环境变量化（当前 yaml 含明文开发凭证）
- [ ] 单元测试与 CI（当前仅 `go build` / `go vet` 把关）
- [ ] 多端消息漫游、会话列表服务端化（当前由客户端本地组装）
- [ ] 限流阈值 etcd 热更新（hotconfig 包已就绪，未接线）

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

## ⚠️ 已知限制

- 各 yaml 中的数据库/MinIO 凭证为**明文开发默认值**，不可用于生产，公开部署前务必环境变量化。
- 仓库内 DDL 仅覆盖 `files` 表，其余服务的建表脚本待补齐。
- 无单元测试与 CI；质量把关目前依赖 `go build` / `go vet`。
- 消息撤回、上下线、群变更暂无实时推送（事件 topic `im.event.notify` 已预留未接线）。

---

## 📝 配置速查

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

## 📄 许可证

License 待定（计划 MIT）。

## 📧 联系方式

- 作者：lancelot-G
- 项目地址：https://github.com/your-org/im-platform （替换为实际仓库地址）
