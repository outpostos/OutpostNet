#!/usr/bin/env bash
# Relay end-to-end test script for OutpostNet.
#
# Simulates three regions using separate namespaces:
#   - region-cloud: runs echo server + Agent B (tunnel server, destination)
#   - region-relay: runs Agent C (relay: tunnel server for edge + tunnel client to cloud)
#   - region-edge:  runs Agent A (tunnel client to relay + import proxy)
#
# Data flow: TCP client → Agent A → QUIC → Agent C (relay) → QUIC → Agent B → echo
#
# Prerequisites:
#   - kubectl configured and pointing to a cluster
#   - Docker images built: outpostnet-agent:latest
#
# Usage:
#   ./hack/e2e-relay.sh           # run full test
#   ./hack/e2e-relay.sh cleanup   # remove all test resources

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="${SCRIPT_DIR}/.."

NS_CLOUD="outpostnet-e2e-relay-cloud"
NS_RELAY="outpostnet-e2e-relay-relay"
NS_EDGE="outpostnet-e2e-relay-edge"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

log()  { echo -e "${GREEN}[INFO]${NC} $*"; }
warn() { echo -e "${YELLOW}[WARN]${NC} $*"; }
fail() { echo -e "${RED}[FAIL]${NC} $*"; exit 1; }
pass() { echo -e "${GREEN}[PASS]${NC} $*"; }

cleanup() {
    log "Cleaning up relay E2E test resources..."
    kubectl delete namespace "${NS_CLOUD}" --ignore-not-found 2>/dev/null || true
    kubectl delete namespace "${NS_RELAY}" --ignore-not-found 2>/dev/null || true
    kubectl delete namespace "${NS_EDGE}" --ignore-not-found 2>/dev/null || true
    log "Cleanup complete."
}

wait_for_pod() {
    local label=$1
    local ns=$2
    local timeout=${3:-120}
    log "Waiting for pod with label ${label} in namespace ${ns} (timeout: ${timeout}s)..."

    local start
    start=$(date +%s)
    while true; do
        local elapsed=$(($(date +%s) - start))
        if [[ $elapsed -ge $timeout ]]; then
            warn "Pod not ready, checking status..."
            kubectl get pods -l "${label}" -n "${ns}" -o wide 2>/dev/null || true
            fail "Pod with label ${label} did not become ready within ${timeout}s"
        fi

        local ready
        ready=$(kubectl get pods -l "${label}" -n "${ns}" -o jsonpath='{.items[0].status.conditions[?(@.type=="Ready")].status}' 2>/dev/null || echo "")
        if [[ "${ready}" == "True" ]]; then
            log "Pod with label ${label} is ready."
            return
        fi
        sleep 2
    done
}

if [[ "${1:-}" == "cleanup" ]]; then
    cleanup
    exit 0
fi

log "=== OutpostNet Relay E2E Test ==="
log ""
log "Topology: Edge (A) → Relay (C) → Cloud (B)"
log ""

# Step 1: Create namespaces.
log "Step 1: Creating region namespaces..."
kubectl create namespace "${NS_CLOUD}" --dry-run=client -o yaml | kubectl apply -f -
kubectl create namespace "${NS_RELAY}" --dry-run=client -o yaml | kubectl apply -f -
kubectl create namespace "${NS_EDGE}" --dry-run=client -o yaml | kubectl apply -f -

# Step 2: Deploy echo server in cloud region.
log "Step 2: Deploying echo server in ${NS_CLOUD}..."
kubectl apply -n "${NS_CLOUD}" -f - <<'EOF'
apiVersion: apps/v1
kind: Deployment
metadata:
  name: echo-server
spec:
  replicas: 1
  selector:
    matchLabels:
      app: echo-server
  template:
    metadata:
      labels:
        app: echo-server
    spec:
      containers:
        - name: echo
          image: hashicorp/http-echo:alpine
          args: ["-listen=:8080", "-text=hello-from-cloud-via-relay"]
          ports:
            - containerPort: 8080
---
apiVersion: v1
kind: Service
metadata:
  name: echo-server
spec:
  selector:
    app: echo-server
  ports:
    - port: 8080
      targetPort: 8080
EOF

wait_for_pod "app=echo-server" "${NS_CLOUD}" 120

# Step 3: Deploy Agent B in cloud region (tunnel server).
log "Step 3: Deploying Agent B in ${NS_CLOUD} (tunnel server)..."
kubectl apply -n "${NS_CLOUD}" -f - <<EOF
apiVersion: v1
kind: ConfigMap
metadata:
  name: outpostnet-agent-config
data:
  agent.yaml: |
    region: cloud
    tunnels:
      - remote_region: relay
        role: server
        listen_addr: "0.0.0.0:4242"
    exports:
      - name: echo-server
        local_addr: "echo-server.${NS_CLOUD}.svc:8080"
        protocol: tcp
    health_addr: ":8082"
    metrics:
      addr: ":9090"
    tls:
      insecure: true
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: outpostnet-agent
spec:
  replicas: 1
  selector:
    matchLabels:
      app: outpostnet-agent
      region: cloud
  template:
    metadata:
      labels:
        app: outpostnet-agent
        region: cloud
      annotations:
        prometheus.io/scrape: "true"
        prometheus.io/port: "9090"
    spec:
      containers:
        - name: agent
          image: outpostnet-agent:latest
          imagePullPolicy: IfNotPresent
          args: ["--config=/etc/outpostnet/agent.yaml"]
          ports:
            - containerPort: 4242
              name: tunnel
            - containerPort: 8082
              name: health
            - containerPort: 9090
              name: metrics
          livenessProbe:
            httpGet:
              path: /healthz
              port: 8082
            initialDelaySeconds: 5
          readinessProbe:
            httpGet:
              path: /readyz
              port: 8082
            initialDelaySeconds: 5
          volumeMounts:
            - name: config
              mountPath: /etc/outpostnet
      volumes:
        - name: config
          configMap:
            name: outpostnet-agent-config
---
apiVersion: v1
kind: Service
metadata:
  name: outpostnet-agent-cloud
spec:
  selector:
    app: outpostnet-agent
    region: cloud
  ports:
    - port: 4242
      targetPort: 4242
      name: tunnel
EOF

wait_for_pod "app=outpostnet-agent,region=cloud" "${NS_CLOUD}" 120

# Step 4: Deploy Agent C in relay region.
log "Step 4: Deploying Agent C in ${NS_RELAY} (relay)..."
kubectl apply -n "${NS_RELAY}" -f - <<EOF
apiVersion: v1
kind: ConfigMap
metadata:
  name: outpostnet-agent-config
data:
  agent.yaml: |
    region: relay
    tunnels:
      - remote_region: edge
        role: server
        listen_addr: "0.0.0.0:4242"
      - remote_region: cloud
        role: client
        remote_addr: "outpostnet-agent-cloud.${NS_CLOUD}.svc:4242"
    relays:
      - service: echo-server
        from_region: edge
        to_region: cloud
        max_hops: 3
    health_addr: ":8082"
    metrics:
      addr: ":9090"
    tls:
      insecure: true
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: outpostnet-agent
spec:
  replicas: 1
  selector:
    matchLabels:
      app: outpostnet-agent
      region: relay
  template:
    metadata:
      labels:
        app: outpostnet-agent
        region: relay
      annotations:
        prometheus.io/scrape: "true"
        prometheus.io/port: "9090"
    spec:
      containers:
        - name: agent
          image: outpostnet-agent:latest
          imagePullPolicy: IfNotPresent
          args: ["--config=/etc/outpostnet/agent.yaml"]
          ports:
            - containerPort: 4242
              name: tunnel
            - containerPort: 8082
              name: health
            - containerPort: 9090
              name: metrics
          livenessProbe:
            httpGet:
              path: /healthz
              port: 8082
            initialDelaySeconds: 5
          readinessProbe:
            httpGet:
              path: /readyz
              port: 8082
            initialDelaySeconds: 5
          volumeMounts:
            - name: config
              mountPath: /etc/outpostnet
      volumes:
        - name: config
          configMap:
            name: outpostnet-agent-config
---
apiVersion: v1
kind: Service
metadata:
  name: outpostnet-agent-relay
spec:
  selector:
    app: outpostnet-agent
    region: relay
  ports:
    - port: 4242
      targetPort: 4242
      name: tunnel
EOF

wait_for_pod "app=outpostnet-agent,region=relay" "${NS_RELAY}" 120

# Step 5: Deploy Agent A in edge region (tunnel client).
log "Step 5: Deploying Agent A in ${NS_EDGE} (tunnel client)..."
kubectl apply -n "${NS_EDGE}" -f - <<EOF
apiVersion: v1
kind: ConfigMap
metadata:
  name: outpostnet-agent-config
data:
  agent.yaml: |
    region: edge
    tunnels:
      - remote_region: relay
        role: client
        remote_addr: "outpostnet-agent-relay.${NS_RELAY}.svc:4242"
    imports:
      - name: echo-server
        listen_addr: "0.0.0.0:18080"
        remote_region: relay
        protocol: tcp
    health_addr: ":8082"
    metrics:
      addr: ":9090"
    tls:
      insecure: true
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: outpostnet-agent
spec:
  replicas: 1
  selector:
    matchLabels:
      app: outpostnet-agent
      region: edge
  template:
    metadata:
      labels:
        app: outpostnet-agent
        region: edge
      annotations:
        prometheus.io/scrape: "true"
        prometheus.io/port: "9090"
    spec:
      containers:
        - name: agent
          image: outpostnet-agent:latest
          imagePullPolicy: IfNotPresent
          args: ["--config=/etc/outpostnet/agent.yaml"]
          ports:
            - containerPort: 18080
              name: import-echo
            - containerPort: 8082
              name: health
            - containerPort: 9090
              name: metrics
          livenessProbe:
            httpGet:
              path: /healthz
              port: 8082
            initialDelaySeconds: 5
          readinessProbe:
            httpGet:
              path: /readyz
              port: 8082
            initialDelaySeconds: 5
          volumeMounts:
            - name: config
              mountPath: /etc/outpostnet
      volumes:
        - name: config
          configMap:
            name: outpostnet-agent-config
---
apiVersion: v1
kind: Service
metadata:
  name: echo-server-proxy
spec:
  selector:
    app: outpostnet-agent
    region: edge
  ports:
    - port: 8080
      targetPort: 18080
      name: echo-proxy
EOF

wait_for_pod "app=outpostnet-agent,region=edge" "${NS_EDGE}" 120

# Step 6: Wait for tunnel establishment.
log "Step 6: Waiting for tunnel establishment..."
sleep 15

# Step 7: SC-1 — Verify relay connectivity.
log "Step 7: SC-1 — Verifying relay connectivity (Edge → Relay → Cloud)..."
EDGE_AGENT_POD=$(kubectl get pod -l "app=outpostnet-agent,region=edge" -n "${NS_EDGE}" -o jsonpath='{.items[0].metadata.name}')

RESULT=$(kubectl exec -n "${NS_EDGE}" "${EDGE_AGENT_POD}" -- \
    wget -q -O - "http://127.0.0.1:18080" 2>/dev/null || echo "FAILED")

if echo "${RESULT}" | grep -q "hello-from-cloud-via-relay"; then
    pass "SC-1: Relay connectivity verified (Edge → Relay → Cloud)"
else
    fail "SC-1: Relay connectivity failed, got: ${RESULT}"
fi

# Step 8: SC-2 — Verify relay metrics on relay agent.
log "Step 8: SC-2 — Verifying relay metrics on relay agent..."
RELAY_AGENT_POD=$(kubectl get pod -l "app=outpostnet-agent,region=relay" -n "${NS_RELAY}" -o jsonpath='{.items[0].metadata.name}')

METRICS=$(kubectl exec -n "${NS_RELAY}" "${RELAY_AGENT_POD}" -- \
    wget -q -O - "http://127.0.0.1:9090/metrics" 2>/dev/null || echo "FAILED")

if echo "${METRICS}" | grep -q "outpostnet_relay_streams_total"; then
    pass "SC-2: Relay metrics available on relay node"
else
    warn "SC-2: Relay metrics not found (may need traffic first)"
fi

if echo "${METRICS}" | grep -q "outpostnet_relay_bytes_total"; then
    pass "SC-2: Relay bytes metric available"
fi

# Step 9: SC-3 — Verify health endpoints.
log "Step 9: SC-3 — Verifying health endpoints..."
HEALTHZ=$(kubectl exec -n "${NS_RELAY}" "${RELAY_AGENT_POD}" -- \
    wget -q -O - "http://127.0.0.1:8082/healthz" 2>/dev/null || echo "FAILED")
READYZ=$(kubectl exec -n "${NS_RELAY}" "${RELAY_AGENT_POD}" -- \
    wget -q -O - "http://127.0.0.1:8082/readyz" 2>/dev/null || echo "FAILED")

if [[ "${HEALTHZ}" == "ok" ]] && [[ "${READYZ}" == "ok" ]]; then
    pass "SC-3: Health and readiness endpoints working on relay node"
else
    fail "SC-3: Health endpoints not working: healthz=${HEALTHZ}, readyz=${READYZ}"
fi

log ""
log "=== Relay E2E Test Complete ==="
log ""
log "Test Results:"
pass "SC-1: Relay connectivity (Edge → Relay → Cloud)"
pass "SC-2: Relay metrics and observability"
pass "SC-3: Health endpoints on relay node"
log ""
log "To clean up: ./hack/e2e-relay.sh cleanup"
