# Quick Start (5 Minutes)

This guide shows the simplest scenario: tunneling a service between two hosts.

- **Host A** (service provider): Runs an HTTP service you want to expose
- **Host B** (service consumer): Accesses Host A's service through a local port

One binary (`outpostnet-agent`), no public IP required, no Kubernetes needed.

---

## Prerequisites

- Two hosts that can communicate (LAN or VPN)
- `outpostnet-agent` installed ([download](https://github.com/outpostos/OutpostNet/releases))
- An HTTP service running on Host A at `127.0.0.1:8080` (or start one with `python3 -m http.server 8080`)

---

## Step 1: Start agent on Host A (server role)

Create `agent-a.yaml`:

```yaml
auth:
  token: changeme

tunnels:
  - role: server
    listen_addr: 0.0.0.0:4242

exports:
  - name: my-http
    local_addr: 127.0.0.1:8080
    protocol: tcp
```

```bash
outpostnet-agent --config agent-a.yaml
```

Expected log output:
```
2026/03/29 10:00:00 tunnel server listening on 0.0.0.0:4242
2026/03/29 10:00:00 exported service my-http -> 127.0.0.1:8080
```

---

## Step 2: Start agent on Host B (client role)

Replace `<HOST_A_IP>` with Host A's actual IP address.

Create `agent-b.yaml`:

```yaml
auth:
  token: changeme

tunnels:
  - role: client
    remote_addr: <HOST_A_IP>:4242

imports:
  - name: my-http
    listen_addr: 127.0.0.1:9080
    protocol: tcp
```

```bash
outpostnet-agent --config agent-b.yaml
```

Expected log output:
```
2026/03/29 10:00:01 tunnel client connecting to <HOST_A_IP>:4242
2026/03/29 10:00:01 imported service my-http -> 127.0.0.1:9080
```

---

## Step 3: Verify

On Host B, access local port 9080 — traffic forwards transparently to Host A's port 8080:

```bash
curl http://127.0.0.1:9080
```

You should receive the response from Host A's HTTP service.

---

## Authentication

Both agents must use the same `auth.token`. If the tokens don't match, the tunnel handshake is rejected.

```yaml
auth:
  token: "my-secret-token"
```

---

## Troubleshooting

**Connection timeout**

- Check that port 4242 is open on Host A (firewall / security group)
- Verify the IP address in `agent-b.yaml` is correct

**Service unreachable**

- Confirm the application on Host A is listening on port 8080
- Check agent logs for export registration messages

**Check version**

```bash
outpostnet-agent version
```

---

## Next Steps

- All configuration options: [Configuration Reference](configuration.md)
- Architecture details: [Architecture Overview](architecture.md)
- Deployment examples: [deploy/examples/](../deploy/examples/)
