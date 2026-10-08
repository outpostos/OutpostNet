#!/usr/bin/env bash
# End-to-end test script for OutpostNet on Kubernetes.
#
# Prerequisites:
#   - kubectl configured and pointing to a cluster
#   - Docker images built: outpostnet-server:latest, outpostnet-sidecar:latest
#     (docker build --target <image> -t <image>:latest from the repository
#     Dockerfile, see README "Kubernetes"; loaded onto the cluster nodes)
#
# Usage:
#   ./hack/e2e-k8s.sh           # run full test
#   ./hack/e2e-k8s.sh cleanup   # remove all test resources

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEPLOY_DIR="${SCRIPT_DIR}/../deploy"
NAMESPACE="outpostnet-system"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

log()  { echo -e "${GREEN}[INFO]${NC} $*"; }
warn() { echo -e "${YELLOW}[WARN]${NC} $*"; }
fail() { echo -e "${RED}[FAIL]${NC} $*"; exit 1; }

cleanup() {
    log "Cleaning up test resources..."
    kubectl delete -f "${DEPLOY_DIR}/examples/echo-client.yaml" --ignore-not-found 2>/dev/null || true
    kubectl delete -f "${DEPLOY_DIR}/examples/echo-server.yaml" --ignore-not-found 2>/dev/null || true
    kubectl delete -f "${DEPLOY_DIR}/server-daemonset.yaml" --ignore-not-found 2>/dev/null || true
    kubectl delete -f "${DEPLOY_DIR}/server-configmap.yaml" --ignore-not-found 2>/dev/null || true
    kubectl delete -f "${DEPLOY_DIR}/namespace.yaml" --ignore-not-found 2>/dev/null || true
    log "Cleanup complete."
}

wait_for_pod() {
    local name=$1
    local ns=${2:-default}
    local timeout=${3:-60}
    log "Waiting for pod ${name} in namespace ${ns} (timeout: ${timeout}s)..."
    if ! kubectl wait --for=condition=Ready "pod/${name}" -n "${ns}" --timeout="${timeout}s" 2>/dev/null; then
        warn "Pod ${name} not ready, checking status..."
        kubectl get pod "${name}" -n "${ns}" -o wide 2>/dev/null || true
        kubectl describe pod "${name}" -n "${ns}" 2>/dev/null | tail -20 || true
        fail "Pod ${name} did not become ready within ${timeout}s"
    fi
    log "Pod ${name} is ready."
}

wait_for_daemonset() {
    local name=$1
    local ns=$2
    local timeout=${3:-90}
    log "Waiting for DaemonSet ${name} to be ready (timeout: ${timeout}s)..."
    local end_time=$((SECONDS + timeout))
    while [ $SECONDS -lt $end_time ]; do
        local desired
        local ready
        desired=$(kubectl get ds "${name}" -n "${ns}" -o jsonpath='{.status.desiredNumberScheduled}' 2>/dev/null || echo "0")
        ready=$(kubectl get ds "${name}" -n "${ns}" -o jsonpath='{.status.numberReady}' 2>/dev/null || echo "0")
        if [ "${desired}" -gt 0 ] && [ "${desired}" = "${ready}" ]; then
            log "DaemonSet ${name} ready: ${ready}/${desired}"
            return 0
        fi
        sleep 2
    done
    kubectl get ds "${name}" -n "${ns}" -o wide 2>/dev/null || true
    fail "DaemonSet ${name} did not become ready within ${timeout}s"
}

# Handle cleanup-only invocation.
if [ "${1:-}" = "cleanup" ]; then
    cleanup
    exit 0
fi

log "=== OutpostNet K8s E2E Test ==="

# Step 0: Preflight
log "Checking prerequisites..."
command -v kubectl >/dev/null 2>&1 || fail "kubectl not found"
kubectl cluster-info >/dev/null 2>&1 || fail "Cannot reach Kubernetes cluster"

# Step 1: Deploy outpostnet-server
log "--- Step 1: Deploying OutpostNet server ---"
kubectl apply -f "${DEPLOY_DIR}/namespace.yaml"
kubectl apply -f "${DEPLOY_DIR}/server-configmap.yaml"
kubectl apply -f "${DEPLOY_DIR}/server-daemonset.yaml"
wait_for_daemonset "outpostnet-server" "${NAMESPACE}"

# Step 2: Deploy echo-server (Pod B, exporter)
log "--- Step 2: Deploying echo-server (exporter) ---"
kubectl apply -f "${DEPLOY_DIR}/examples/echo-server.yaml"
wait_for_pod "echo-server" "default" 90

# Step 3: Deploy echo-client (Pod A, importer)
log "--- Step 3: Deploying echo-client (importer) ---"
kubectl apply -f "${DEPLOY_DIR}/examples/echo-client.yaml"
wait_for_pod "echo-client" "default" 90

# Step 4: Wait for sidecars to register
log "--- Step 4: Waiting for sidecar registration ---"
sleep 5

# Step 5: Test TCP connectivity
log "--- Step 5: Testing TCP connectivity through OutpostNet ---"
RESULT=$(kubectl exec echo-client -c app -- sh -c 'echo "ping" | nc -w 5 127.0.0.1 9090' 2>&1) || true

if echo "${RESULT}" | grep -q "Hello from echo-server"; then
    log "TCP test PASSED: received '${RESULT}'"
else
    warn "TCP test result: '${RESULT}'"
    log "Checking sidecar logs..."
    kubectl logs echo-server -c sidecar --tail=20 2>/dev/null || true
    kubectl logs echo-client -c sidecar --tail=20 2>/dev/null || true

    # Check server logs
    SERVER_POD=$(kubectl get pods -n "${NAMESPACE}" -l app.kubernetes.io/name=outpostnet-server -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || echo "")
    if [ -n "${SERVER_POD}" ]; then
        kubectl logs "${SERVER_POD}" -n "${NAMESPACE}" --tail=20 2>/dev/null || true
    fi
    fail "TCP connectivity test FAILED"
fi

log "=== All tests PASSED ==="

# Cleanup
log "--- Cleaning up ---"
cleanup

log "E2E test complete."
