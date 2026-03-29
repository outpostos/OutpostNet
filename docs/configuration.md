# 配置参考

本文档覆盖 OutpostNet 所有组件的完整配置项。

---

## outpostnet-server

Server 运行在每台主机上，通过 Unix domain socket 管理本机所有 sidecar 的服务注册和路由。

### YAML 配置文件

```yaml
# 必填：Unix socket 监听路径
listen: /var/run/outpostnet/server.sock

# 可选：日志级别，默认 info
# 可选值：debug | info | warn | error
log_level: info

# 可选：sidecar 心跳超时，超时后断开连接，默认 30s
heartbeat_timeout: 30s
```

### 命令行参数

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `--config <path>` | — | YAML 配置文件路径 |
| `--socket <path>` | `/var/run/outpostnet/server.sock` | Unix socket 路径（覆盖配置文件） |
| `--heartbeat-timeout <duration>` | `30s` | 心跳超时时长（覆盖配置文件） |

### 完整示例

```yaml
listen: /var/run/outpostnet/server.sock
log_level: info
heartbeat_timeout: 30s
```

---

## outpostnet-sidecar

Sidecar 运行在应用进程旁边，负责将本地服务导出到节点 server，并将远程服务映射为本地端口。

### YAML 配置文件

```yaml
# 必填：server 的 Unix socket 路径
server: /var/run/outpostnet/server.sock

# 可选：sidecar 唯一标识，默认使用主机名
sidecar_id: my-sidecar

# 可选：心跳保活间隔，默认 10s
heartbeat_interval: 10s

# 可选：导出的本地服务列表
exports:
  - name: my-api        # 服务名称（全局唯一）
    local_port: 8080    # 本地监听端口
    protocol: tcp       # 协议：tcp 或 udp

# 可选：从远程导入的服务列表
imports:
  - name: remote-db     # 远程服务名称
    listen_port: 5432   # 本地监听端口（应用通过此端口访问）
    protocol: tcp       # 协议：tcp 或 udp
```

### 字段说明

#### `exports[]`

| 字段 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `name` | string | 是 | 服务名称，在整个网格中唯一 |
| `local_port` | uint16 | 是 | 本机上该服务监听的端口 |
| `protocol` | string | 是 | `tcp` 或 `udp` |

#### `imports[]`

| 字段 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `name` | string | 是 | 要导入的远程服务名称 |
| `listen_port` | uint16 | 是 | 本机监听端口，应用通过此端口访问远程服务 |
| `protocol` | string | 是 | `tcp` 或 `udp` |

### 命令行参数

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `--config <path>` | — | YAML 配置文件路径 |
| `--server-socket <path>` | `/var/run/outpostnet/server.sock` | server socket 路径（覆盖配置文件） |
| `--sidecar-id <id>` | 主机名 | sidecar 唯一标识（覆盖配置文件） |
| `--heartbeat-interval <duration>` | `10s` | 心跳间隔（覆盖配置文件） |

### 完整示例

```yaml
server: /var/run/outpostnet/server.sock
sidecar_id: app-server-01
heartbeat_interval: 10s

exports:
  - name: web-api
    local_port: 8080
    protocol: tcp
  - name: metrics
    local_port: 9090
    protocol: tcp

imports:
  - name: postgres
    listen_port: 5432
    protocol: tcp
  - name: redis
    listen_port: 6379
    protocol: tcp
```

---

## outpostnet-agent

Agent 运行在每个区域，通过 QUIC 协议管理跨区域加密隧道，支持服务导出、导入和多跳中继。

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

# 导出给远程区域的服务
exports:
  - name: my-api               # 服务名称
    local_addr: 127.0.0.1:8080 # 本地服务地址
    protocol: tcp              # tcp 或 udp

# 从远程区域导入的服务
imports:
  - name: cloud-db             # 远程服务名称
    listen_addr: 127.0.0.1:5432 # 本地监听地址（agent 层面的映射）
    remote_region: cloud        # 哪个区域导出了此服务
    protocol: tcp               # tcp 或 udp

# 多跳中继规则（可选）
relays:
  - service: far-edge-svc      # 要中继的服务名称
    from_region: far-edge      # 来源区域
    to_region: cloud           # 目标区域
    max_hops: 3                # 最大跳数（1-5，默认 3）

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

# 认证配置（可选）
auth:
  token: ""                    # 共享密钥，空字符串表示禁用认证
```

### 字段说明

#### `tunnels[]`

| 字段 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `remote_region` | string | 是 | 对端区域名称 |
| `role` | string | 是 | `server`（接受连接）或 `client`（主动连接） |
| `listen_addr` | string | role=server 时必填 | 监听地址，格式 `host:port` |
| `remote_addr` | string | role=client 时必填 | 对端地址，格式 `host:port` |

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

中继规则用于三区域场景，让当前 agent 作为流量中转节点。

| 字段 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `service` | string | 是 | 要中继的服务名称 |
| `from_region` | string | 是 | 来源区域 |
| `to_region` | string | 是 | 目标区域（必须有对应的 client 角色 tunnel） |
| `max_hops` | int | 否 | 最大跳数，1-5，默认 3 |

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

#### `auth`

| 字段 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `token` | string | `""` | 共享认证密钥，两端必须一致，空字符串禁用认证 |

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

## 默认值汇总

| 参数 | 默认值 |
|------|--------|
| server socket | `/var/run/outpostnet/server.sock` |
| server heartbeat_timeout | `30s` |
| sidecar heartbeat_interval | `10s` |
| agent health_addr | `:8082` |
| agent metrics addr | `:9090` |
| agent persistence dir | `/var/lib/outpostnet` |
| agent relay max_hops | `3` |
