# 快速上手（5 分钟）

本文档演示在两台主机之间建立隧道的最小场景：

- **主机 A**（服务提供方）：运行一个 HTTP 服务，希望将它暴露给主机 B
- **主机 B**（服务消费方）：通过本地端口透明访问主机 A 的服务

整个过程不需要公网 IP，也不需要 Kubernetes。

---

## 前提条件

- 两台可以互相通信的主机（局域网或 VPN 均可）
- `outpostnet-server`、`outpostnet-sidecar`、`outpostnet-agent` 二进制文件已安装
- 主机 A 上有一个运行在 `127.0.0.1:8080` 的 HTTP 服务（或用 `python3 -m http.server 8080` 临时启动）

---

## 步骤一：在主机 A 上启动 server

Server 负责管理本机所有 sidecar 的注册和路由，每台主机启动一个。

```bash
# 主机 A
outpostnet-server --socket /tmp/outpostnet.sock
```

日志输出类似：
```
2026/03/29 10:00:00 server listening on /tmp/outpostnet.sock
```

---

## 步骤二：在主机 A 上启动 sidecar（导出服务）

创建配置文件 `sidecar-a.yaml`：

```yaml
server: /tmp/outpostnet.sock
sidecar_id: host-a

exports:
  - name: my-http
    local_port: 8080
    protocol: tcp
```

启动 sidecar：

```bash
# 主机 A
outpostnet-sidecar --config sidecar-a.yaml
```

日志输出类似：
```
2026/03/29 10:00:01 connected to server, sidecar_id=host-a
2026/03/29 10:00:01 registered 1 export(s)
```

---

## 步骤三：在主机 B 上启动 agent（跨主机隧道）

Agent 通过 QUIC 协议在两台主机之间建立加密隧道。

**主机 A** 上启动 agent（服务端角色，监听隧道连接）：

创建 `agent-a.yaml`：

```yaml
region: region-a

tunnels:
  - remote_region: region-b
    role: server
    listen_addr: 0.0.0.0:4242

exports:
  - name: my-http
    local_addr: 127.0.0.1:8080
    protocol: tcp
```

```bash
# 主机 A
outpostnet-agent --config agent-a.yaml
```

**主机 B** 上启动 agent（客户端角色，连接到主机 A）：

将 `<HOST_A_IP>` 替换为主机 A 的实际 IP 地址。

创建 `agent-b.yaml`：

```yaml
region: region-b

tunnels:
  - remote_region: region-a
    role: client
    remote_addr: <HOST_A_IP>:4242

imports:
  - name: my-http
    listen_addr: 127.0.0.1:9080
    remote_region: region-a
    protocol: tcp
```

```bash
# 主机 B
outpostnet-agent --config agent-b.yaml
```

---

## 步骤四：验证连接

在主机 B 上访问本地 9080 端口，流量将透明转发到主机 A 的 8080 端口：

```bash
# 主机 B
curl http://127.0.0.1:9080
```

如果配置正确，应该收到主机 A 上 HTTP 服务的响应。

---

## 故障排查

**连接超时**

- 检查主机 A 的 4242 端口是否开放（防火墙/安全组）
- 确认 `agent-b.yaml` 中的 IP 地址正确

**服务不可达**

- 确认主机 A 上的应用程序确实在 8080 端口监听
- 检查 sidecar 日志是否有注册失败信息

**查看版本**

```bash
outpostnet-server version
outpostnet-agent version
outpostnet-sidecar version
```

---

## 下一步

- 了解所有配置项：[docs/configuration.md](configuration.md)
- 了解架构原理：[docs/architecture.md](architecture.md)
- 查看完整部署示例：[deploy/examples/](../deploy/examples/)
