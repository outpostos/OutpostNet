# 快速上手（5 分钟）

本文档演示在两台主机之间建立隧道的最小场景：

- **主机 A**（服务提供方）：运行一个 HTTP 服务，希望将它暴露给主机 B
- **主机 B**（服务消费方）：通过本地端口透明访问主机 A 的服务

整个过程只需要 `outpostnet-agent` 一个二进制，不需要公网 IP，不需要 Kubernetes。

---

## 前提条件

- 两台可以互相通信的主机（局域网或 VPN 均可）
- `outpostnet-agent` 已安装（[下载](https://github.com/outpostos/OutpostNet/releases)）
- 主机 A 上有一个运行在 `127.0.0.1:8080` 的 HTTP 服务（或用 `python3 -m http.server 8080` 临时启动）

---

## 步骤一：在主机 A 上启动 agent（server 角色）

创建 `agent-a.yaml`：

```yaml
region: site-a

tunnels:
  - remote_region: site-b
    role: server
    listen_addr: 0.0.0.0:4242

exports:
  - name: my-http
    local_addr: 127.0.0.1:8080
    protocol: tcp
```

```bash
outpostnet-agent --config agent-a.yaml
```

日志输出类似：
```
2026/03/29 10:00:00 agent starting region=site-a
2026/03/29 10:00:00 tunnel server listening on 0.0.0.0:4242
2026/03/29 10:00:00 exported service my-http -> 127.0.0.1:8080
```

---

## 步骤二：在主机 B 上启动 agent（client 角色）

将 `<HOST_A_IP>` 替换为主机 A 的实际 IP 地址。

创建 `agent-b.yaml`：

```yaml
region: site-b

tunnels:
  - remote_region: site-a
    role: client
    remote_addr: <HOST_A_IP>:4242

imports:
  - name: my-http
    listen_addr: 127.0.0.1:9080
    remote_region: site-a
    protocol: tcp
```

```bash
outpostnet-agent --config agent-b.yaml
```

日志输出类似：
```
2026/03/29 10:00:01 agent starting region=site-b
2026/03/29 10:00:01 tunnel client connecting to <HOST_A_IP>:4242
2026/03/29 10:00:01 imported service my-http -> 127.0.0.1:9080
```

---

## 步骤三：验证连接

在主机 B 上访问本地 9080 端口，流量将透明转发到主机 A 的 8080 端口：

```bash
curl http://127.0.0.1:9080
```

如果配置正确，应该收到主机 A 上 HTTP 服务的响应。

---

## 添加认证（推荐）

在两端的配置中加入相同的 token 即可启用认证：

```yaml
auth:
  token: "my-secret-token"
```

两端的 token 必须一致，否则隧道握手将被拒绝。

---

## 多跳中继

如果主机 A 和主机 C 无法直连，可以通过主机 B 做中继：

```
主机 A ──── QUIC ────► 主机 B (中继) ──── QUIC ────► 主机 C
```

在主机 B 的配置中加入 relay 规则：

```yaml
relays:
  - service: my-http
    from_region: site-a
    to_region: site-c
```

详见 [配置参考](configuration.md) 中的中继配置说明。

---

## 故障排查

**连接超时**

- 检查主机 A 的 4242 端口是否开放（防火墙/安全组）
- 确认 `agent-b.yaml` 中的 IP 地址正确

**服务不可达**

- 确认主机 A 上的应用程序确实在 8080 端口监听
- 检查 agent 日志中是否有 export 注册信息

**查看版本**

```bash
outpostnet-agent version
```

---

## 下一步

- 了解所有配置项：[配置参考](configuration.md)
- 了解架构原理：[架构概览](architecture.md)
- 查看部署示例：[deploy/examples/](../deploy/examples/)
