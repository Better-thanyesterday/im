# IM 即时通讯平台

> 基于 go-zero 的单仓微服务架构，提供 WebSocket 长连接接入、单聊/群聊、消息同步、已读回执、消息撤回、文件分片上传等完整 IM 能力。

[![Go Version](https://img.shields.io/badge/Go-1.25+-00ADD8?logo=go)](https://go.dev/)
[![go-zero](https://img.shields.io/badge/go--zero-v1.10+-blue)](https://go-zero.dev/)
[![License](https://img.shields.io/badge/license-MIT-green)](LICENSE)

---

## 📖 项目简介

本平台是一个面向高并发场景的即时通讯系统，采用 **单仓微服务** 架构，包含：

- **HTTP API 层（im-api）**：统一 REST 入口，聚合用户/群组/文件/消息能力，统一 Token 鉴权。
- **WebSocket 网关（gateway）**：长连接接入、心跳保活、同设备互踢、客户端驱动的离线同步。
- **五个 RPC 服务**：`user、group、message、push、file`
- **完整消息链路**：消息以 `WriteDiffBundle` 单载荷经 Kafka 落库，单 topic 单消费组保证会话内有序、不丢、可重放。
- **文件服务**：基于 MinIO/S3 原生 Multipart 分片上传，客户端直传，服务端只管元数据与预签名。

覆盖从 **接入 → 投递 → 存储 → 推送** 的完整 IM 链路。

---

## ✨ 功能特性

- **单聊 / 群聊**：支持文本、图片、语音、视频、文件、位置等消息类型；媒体消息以 `file_id` 引用文件服务，发送链路校验文件存在/可用/归属。
- **消息同步**：会话内单调递增 `seq`，客户端携带本地水位主动 `sync` 增量补拉，Redis 离线信箱按会话拆分加速重连。
- **已读回执**：`(会话, 阅读者)` 级已读水位 + PG 收件箱批量置读 + 未读数联动清理。
- **消息撤回**：仅发送者可撤，软删除保留 `recalled_by/recalled_at` 审计字段。
- **在线状态**：Redis 两层在线表（uid 级注册表 Hash + 设备级活性 key，90s 心跳续期）。
- **可靠投递**：先落库后扩散、Kafka 异步解耦、`InsertIgnore` 幂等消费、Redis SETNX 幂等去重、失败批次后台补偿器。
- **WebSocket 网关**：自研分桶 ConnManager，支持心跳、断线重连、顶号下线（FrameKick 通知被顶设备）。
- **群管理**：退群/踢人/禁言/设管理员/转让群主/解散/入群申请与审批，三级权限模型（群主/管理员/成员），成员变更联动 `member_version` 缓存失效。
- **群聊投递优化**：`BatchDeliver` 受限并发 + 批量写连接 + 群成员版本缓存。
- **文件分片上传**：S3 原生 Multipart，≥5MiB/片，预签名直传 MinIO。
- **安全与鉴权**：Token + Redis 黑名单（登出/踢设备即时失效）、登录限流、会话归属校验、好友/拉黑前置校验、gRPC metadata 透传身份二次校验。
- **可观测性**：OpenTelemetry 全链路追踪（OTLP → Jaeger），go-zero 内建 Prometheus 指标与 pprof。

---

## 🧱 技术栈

| 类别 | 技术 |
|------|------|
| 语言 | Go 1.25 |
| 微服务框架 | go-zero（REST API / zRPC） |
| 通信 | gRPC + Protobuf、WebSocket（gorilla） |
| 注册发现 / 配置 | etcd |
| 消息队列 | Kafka（IBM/sarama） |
| 数据库 | PostgreSQL |
| 缓存 | Redis |
| 对象存储 | MinIO / S3（aws-sdk-go-v2） |
| 鉴权 | Redis Token（哈希存储 + 黑名单） |
| 分布式 ID | Snowflake（按 IP+PID 自动派生节点号） |
| 链路追踪 | OpenTelemetry + Jaeger |
| 桌面客户端 | Wails v2 + Vue 3（独立仓库 `im-wails`） |

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
   │  :18884  │ │ :18885  │ │ :18085 │ │  :18081 (WS:18890) │
   └──────────┘ └─────────┘ └───┬────┘ └─────────▲──────────┘
        │            │          │                │ PushToConn
        │            │          ▼                │ BatchPushToConn
        │            │       MinIO/S3            │
        ▼            ▼                           │
   ┌──────────────────────┐                      │
   │     message.rpc      │                      │
   │       :18082         │                      │
   └──────┬─────────┬─────┘                      │
          │         │ 发送链路                    │
          │         └──────────────┐             │
          ▼                        ▼             │
   ┌─────────────┐          ┌────────────┐       │
   │    Kafka    │          │  push.rpc  │───────┘
   │im.msg.persist│         │   :18883   │  查 Redis 在线表
   └──────┬──────┘          │  (推送服务) │  直连各 gateway 实例
          │ 单消费组         └────────────┘
          ▼
   ┌─────────────┐     ┌──────────────────────────┐
   │ msg→seq→inbox│    │ PostgreSQL × N 库         │
   │ 按序落库     │     │ Redis(在线表/水位/未读)    │
   └─────────────┘     └──────────────────────────┘
```

- **接入**：REST 业务操作走 `im-api`；实时收发/同步/回执走 WebSocket 长连接。
- **投递**：消息经 gateway 转发到 message.rpc 分配 seq 后，单 topic 单载荷异步落库；实时推送由 push.rpc 查在线表后直连目标 gateway。
- **存储**：PostgreSQL 按服务分库（`im_user` / `im_group` / `im_message` / `im_file`）。

---

## 📁 目录结构

```
im-platform/
├── app/
│   ├── im/api/              # HTTP API 层(REST :18888,统一鉴权与 API 聚合)
│   ├── gateway/
│   │   └── rpc/             # WebSocket 网关(gRPC :18081 + WS :18890)
│   │       ├── internal/conn/       # 连接管理器(分桶)
│   │       ├── internal/protocol/   # 二进制帧协议 [type:1][len:4][payload]
│   │       ├── internal/handler/    # WS 握手
│   │       └── internal/logic/      # 上行帧转发/连接注册
│   ├── user/rpc/            # 用户服务 :18884(账号/资料/设备/好友/黑名单)
│   ├── group/rpc/           # 群组服务 :18885(建群/成员管理/审批/权限)
│   ├── message/rpc/         # 消息服务 :18082(发送/同步/回执/撤回/seq)
│   │   └── internal/logic/  # 含 Kafka 消费落库与后台补偿器
│   ├── push/rpc/            # 推送服务 :18883(在线路由/批量投递/离线信箱/未读)
│   └── file/rpc/            # 文件服务 :18085(分片上传/元数据/预签名)
├── common/                  # 公共库
│   ├── constants/           # 常量与业务错误码
│   ├── middleware/          # 鉴权中间件(Token 校验/身份注入)
│   ├── mq/                  # Kafka 封装(sarama 生产/消费/协议)
│   ├── hotconfig/           # etcd watch 热配置
│   └── utils/               # TokenManager/Snowflake/SeqCache 等
├── docs/
│   ├── jaeger-v2.yaml       # Jaeger v2 配置
│   └── sql/files.sql        # 文件服务建表脚本
├── test/binarygen.go        # WS 二进制帧联调工具
├── go.mod
└── README.md
```

---

## 🚀 快速开始

### 环境依赖

- Go 1.25+
- etcd 3.5+
- Kafka 3.x
- PostgreSQL 15+
- Redis 7.x
- MinIO（或任意 S3 兼容对象存储）
- Jaeger 2.x（可选，链路追踪）

### 1. 初始化数据库

```bash
# 各服务分库:im_user / im_group / im_message / im_file(按 app/<svc>/etc/*.yaml 中的 DSN)
createdb im_file   # 其余库同理

# 文件服务表结构
psql -h 127.0.0.1 -U postgres -d im_file -f docs/sql/files.sql
```

### 2. 检查服务配置

各服务配置位于 `app/<service>/rpc/etc/*.yaml`（HTTP 层在 `app/im/api/etc/gateway-api.yaml`），按需修改 etcd / PostgreSQL / Redis / Kafka 地址：

```yaml
# app/message/rpc/etc/message.yaml(节选)
ListenOn: 0.0.0.0:18082

Etcd:
  Hosts:
    - 127.0.0.1:2379
  Key: message.rpc

Postgres:
  DataSource: postgres://user:pass@127.0.0.1:5432/im_message?sslmode=disable

Kafka:
  Brokers:
    - 127.0.0.1:9092
```

### 3. 启动服务

建议按 依赖服务 → 业务 rpc → 接入层 的顺序启动：

```bash
# 业务 RPC 服务
go run app/user/rpc/user.go         -f app/user/rpc/etc/user.yaml
go run app/group/rpc/group.go       -f app/group/rpc/etc/group.yaml
go run app/message/rpc/message.go   -f app/message/rpc/etc/message.yaml
go run app/file/rpc/file.go         -f app/file/rpc/etc/file.yaml

# 推送服务(依赖 Redis 在线表)
go run app/push/rpc/push.go         -f app/push/rpc/etc/push.yaml

# WebSocket 网关(REST :18888 的长连接入口是这里提供的 :18890)
go run app/gateway/rpc/gateway.go   -f app/gateway/rpc/etc/gateway.yaml

# HTTP API 层
go run app/im/api/api.go            -f app/im/api/etc/gateway-api.yaml
```

### 4. 验证

```bash
# 健康检查
curl http://127.0.0.1:18888/health

# 注册 + 登录
curl -X POST http://127.0.0.1:18888/register -d '{"phone":"13800000000","password":"your-password"}'
curl -X POST http://127.0.0.1:18888/login    -d '{"phone":"13800000000","password":"your-password"}'
# → {"token":"..."},后续请求携带 Authorization: Bearer <token>

# WebSocket 接入(token 走 query,浏览器 WebSocket 无法携带自定义 Header)
# ws://127.0.0.1:18890/ws?token=<token>
```

---

## 🔑 核心模块说明

### 1. im-api(HTTP API 层)

- **API 聚合**：40+ 端点覆盖账号/资料/设备/好友/群组/文件/消息撤回，统一错误与鉴权。
- **身份不信任请求体**：`creator_id`/`operator_id` 等一律由鉴权中间件注入 ctx。
- **WS 无法带 Header**：`/ws` 路径支持 query token 回退，网关放行带 token 的跨源握手（桌面 WebView 场景）。

### 2. WebSocket 网关（gateway）

- **连接管理**：分桶 ConnManager，读写泵分离，心跳 ping 30s / 读超时 90s。
- **同设备互踢**：重复登录先向旧连接推 `FrameKick` 再断开，被顶设备可区分"被顶号"与"网络断"。
- **客户端驱动的离线同步**：建连后客户端主动发 `FrameSync`（携带各会话本地水位），服务端不做隐式补拉——多设备下只有客户端知道自己的进度。
- **二进制帧协议**：`[type:1][len:4 大端][payload JSON]`，上行 0x02 发消息 / 0x03 回执 / 0x04 同步，下行 0x10 推送 / 0x11 踢下线 / 0x1F 错误。

### 3. 消息服务（message.rpc）

- **seq 生成**：Redis `INCRBY 100` 批量预取 + 本地号段缓存，PG 兜底分配，会话内单调递增且永不过期。
- **可靠落库**：`WriteDiffBundle`(msg→seq→inbox) 单载荷发 Kafka（key=conv_id 保证同会话分区有序），单消费组按序落库，全部写操作幂等（`InsertIgnore`/`UpsertMaxSeq`/`BatchInsertIgnore`），Kafka 重投安全。
- **失败补偿**：Kafka 与 PG 兜底同时失败时，整批转入后台补偿器退避重试，丢弃计数告警。
- **已读水位**：`im:read:{conv}:{reader}` Lua 只进不退，收件箱按水位批量置读。

### 4. 推送服务（push.rpc）

- **两级在线表**：`im:online:{uid}` Hash（注册表）+ `im:online:{uid}:{device}` 活性 key（90s 心跳续期），崩溃设备不残留。
- **GatewayPool**：按 gateway 地址维护 gRPC 连接池，读写锁 + 双重检查，连续失败 3 次才逐出（避免多设备共享连接被单次失败连坐）。
- **投递语义**：任意设备实时送达即成功；全失败/离线转会话级离线信箱并计未读；Notify 类轻量通知不落信箱。

### 5. 文件服务（file.rpc）

- **S3 Multipart 三步流**：`CreateUploadTask`(返回 file_id + 各分片预签名 PUT URL) → 客户端直传 MinIO → `CompleteUpload`(带各片 ETag 合并)。
- **元数据独立**：files 表只存元数据与 object_key，下载一律走预签名 URL（默认 15 分钟）。
- **安全**：删除仅限上传者；媒体消息发送时由 message.rpc 反查校验文件归属与状态。

---

## 🔒 安全与鉴权

- Token 登录态存 Redis，支持黑名单——登出/踢设备即时失效。
- 登录接口限流（5 次 / 15 分钟）防爆破。
- 会话归属校验：`conv_id` 必须由发送/接收双方构成，防止向任意会话写消息。
- 好友关系与拉黑前置校验（单聊发送链路）。
- WS 握手 Origin 校验：同源放行；跨源必须携带显式 token（无 cookie 环境凭证，CSWSH 无可乘面）。
- gRPC 链路经 metadata 透传用户身份，服务端二次校验，不信任客户端传入的 uid。

## 📊 可观测性

- **链路追踪**：go-zero Telemetry → OTLP → Jaeger（配置见 `docs/jaeger-v2.yaml`）。
- **指标**：go-zero 内建 Prometheus 指标（DevServer），RPC/REST 调用与错误率开箱即得。
- **日志约定**：中文描述 + 英文 `key=value` 结构化字段，便于检索。

## 🧪 测试与联调

```bash
# 编译验证
go build ./...

# 静态检查
go vet ./...

# 单元测试
go test ./...

# WebSocket 二进制帧联调工具(构造上行帧样例)
go run test/binarygen.go
```

## 📝 配置说明

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

> ⚠️ 当前各 yaml 中的数据库/MinIO 凭证为本地开发默认值，生产部署请改用环境变量注入并启用 TLS。


## 📧 联系方式

- 作者：Better-thanyesterday
- 项目地址：https://github.com/Better-thanyesterday/im
