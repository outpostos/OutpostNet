# 配置参考

本文档覆盖 OutpostNet 所有组件的配置项。

> **1.0 用户**：大多数场景只需要 `outpostnet-agent`，直接看第一节即可。

---

## outpostnet-agent

Agent 是 OutpostNet 的核心组件，负责跨主机/跨区域的 QUIC 加密隧道。一个二进制，通过 `role` 配置区分 server 和 client 角色（类似 frp 的 frps + frpc）。

### YAML 配置文件

```yaml
# 必填：当前区域名称
region: edge-region-1

# QUIC 隧道列表
tunnels:
  - remote_region: cloud       # 对端区域名称
    role: server               # server（接受连接）或 client（主动连接）
    listen_addr: 0.0.0.0:4242  # role=server 时必填：监听地址
    # remote_addr: host:port   # role=client 时必填：对端地址

# 导出给远程的服务（本机上的服务）
exports:
  - name: my-api               # 服务名称
    local_addr: 127.0.0.1:8080 # 本地服务地址
    protocol: tcp              # tcp 或 udp

# 从远程导入的服务（映射到本机端口）
imports:
  - name: cloud-db             # 远程服务名称
    listen_addr: 127.0.0.1:5432 # 本地监听地址
    remote_region: cloud        # 哪个区域导出了此服务
    protocol: tcp               # tcp 或 udp

# 多跳中继规则（可选）
relays:
  - service: far-edge-svc      # 要中继的服务名称
    from_region: far-edge      # 来源区域
    to_region: cloud           # 目标区域
    max_hops: 3                # 最大跳数（1-5，默认 3）

# 认证配置（可选，推荐启用）
auth:
  token: ""                    # 共享密钥，两端必须一致，空字符串表示禁用

# 持久化配置
persistence:
  dir: /var/lib/outpostnet     # 持久化目录，默认 /var/lib/outpostnet
  enabled: true                # 是否启用，默认 true

# Prometheus 指标
metrics:
  addr: :9090                  # 指标地址，默认 :9090

# 健康检查端点
health_addr: :8082             # /healthz 和 /readyz，默认 :8082

# TLS 配置（可选）
tls:
  cert_file: /path/to/cert.pem
  key_file: /path/to/key.pem
  insecure: false              # 跳过 TLS 验证（仅开发用）
```

### 字段说明

#### `tunnels[]`

| 字段 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `remote_region` | string | 是 | 对端区域名称 |
| `role` | string | 是 | `server`（监听，等待对方连接）或 `client`（主动连接到对方） |
| `listen_addr` | string | role=server 时必填 | 监听地址，格式 `host:port` |
| `remote_addr` | string | role=client 时必填 | 对端地址，格式 `host:port` |

> **与 frp 的对应关系**：`role: server` 相当于 frps，`role: client` 相当于 frpc。

#### `exports[]`

| 字段 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `name` | string | 是 | 服务名称 |
| `local_addr` | string | 是 | 本地服务地址，格式 `host:port` |
| `protocol` | string | 是 | `tcp` 或 `udp` |

#### `imports[]`

| 字段 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `name` | string | 是 | 要导入的远程服务名称 |
| `listen_addr` | string | 是 | 本地监听地址，格式 `host:port` |
| `remote_region` | string | 是 | 该服务由哪个区域导出 |
| `protocol` | string | 是 | `tcp` 或 `udp` |

#### `relays[]`

中继规则让当前 agent 作为流量中转节点，用于三区域或更多区域的场景。

| 字段 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `service` | string | 是 | 要中继的服务名称 |
| `from_region` | string | 是 | 来源区域 |
| `to_region` | string | 是 | 目标区域（必须有对应的 tunnel） |
| `max_hops` | int | 否 | 最大跳数，1-5，默认 3 |

#### `auth`

| 字段 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `token` | string | `""` | 共享认证密钥，两端必须一致，空字符串禁用认证 |

#### `persistence`

| 字段 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `dir` | string | `/var/lib/outpostnet` | 持久化文件目录 |
| `enabled` | bool | `true` | 是否启用配置持久化 |

#### `tls`

| 字段 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `cert_file` | string | — | TLS 证书路径 |
| `key_file` | string | — | TLS 私钥路径 |
| `insecure` | bool | `false` | 跳过证书验证（仅开发环境） |

> **注意**：`insecure: true` 不应在生产环境使用。

### 命令行参数

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `--config <path>` | — | YAML 配置文件路径 |
| `--region <name>` | — | 区域名称（覆盖配置文件） |
| `--metrics-addr <addr>` | `:9090` | 指标地址（覆盖配置文件） |
| `--health-addr <addr>` | `:8082` | 健康检查地址（覆盖配置文件） |
| `--persistence-dir <path>` | `/var/lib/outpostnet` | 持久化目录（覆盖配置文件） |

### 健康检查端点

| 端点 | 说明 |
|------|------|
| `GET /healthz` | 存活检查 |
| `GET /readyz` | 就绪检查 |
| `GET /metrics` | Prometheus 指标 |

---

## 进阶：outpostnet-server + outpostnet-sidecar

> 以下组件用于 **单主机多应用** 场景——同一台主机上有多个应用需要通过隧道对外暴露或导入服务。基本的跨主机隧道场景不需要这些组件。

### outpostnet-server

Server 运行在单台主机上，通过 Unix domain socket 管理多个 sidecar 的服务注册和路由。

```yaml
# 必填：Unix socket 监听路径
listen: /var/run/outpostnet/server.sock

# 可选：日志级别，默认 info
log_level: info

# 可选：sidecar 心跳超时，默认 30s
heartbeat_timeout: 30s
```

| 命令行参数 | 默认值 | 说明 |
|------|--------|------|
| `--config <path>` | — | YAML 配置文件路径 |
| `--socket <path>` | `/var/run/outpostnet/server.sock` | Unix socket 路径 |
| `--heartbeat-timeout <duration>` | `30s` | 心跳超时时长 |

### outpostnet-sidecar

Sidecar 运行在每个应用旁边，连接到本机的 server，负责将本地服务导出或将远程服务映射为本地端口。

```yaml
# 必填：server 的 Unix socket 路径
server: /var/run/outpostnet/server.sock

# 可选：sidecar 唯一标识，默认使用主机名
sidecar_id: my-sidecar

# 可选：心跳保活间隔，默认 10s
heartbeat_interval: 10s

# 导出的本地服务
exports:
  - name: my-api
    local_port: 8080
    protocol: tcp

# 从远程导入的服务
imports:
  - name: remote-db
    listen_port: 5432
    protocol: tcp
```

| 命令行参数 | 默认值 | 说明 |
|------|--------|------|
| `--config <path>` | — | YAML 配置文件路径 |
| `--server-socket <path>` | `/var/run/outpostnet/server.sock` | server socket 路径 |
| `--sidecar-id <id>` | 主机名 | sidecar 唯一标识 |
| `--heartbeat-interval <duration>` | `10s` | 心跳间隔 |

---

## 默认值汇总

| 参数 | 默认值 |
|------|--------|
| agent health_addr | `:8082` |
| agent metrics addr | `:9090` |
| agent persistence dir | `/var/lib/outpostnet` |
| agent relay max_hops | `3` |
| server socket | `/var/run/outpostnet/server.sock` |
| server heartbeat_timeout | `30s` |
| sidecar heartbeat_interval | `10s` |
