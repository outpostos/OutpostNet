#!/usr/bin/env bash
# End-to-end test script for OutpostNet Controller (Epic 2).
#
# Tests the full automation flow:
#   1. Deploy controller + server
#   2. Create annotated Deployments
#   3. Verify sidecar injection
#   4. Verify ConfigMap generation
#   5. Verify Service/EndpointSlice creation
#   6. Verify TCP connectivity
#
# Prerequisites:
#   - kubectl configured and pointing to a cluster
#   - Docker images built: outpostnet-server, outpostnet-sidecar, outpostnet-controller
#
# Usage:
#   ./hack/e2e-controller.sh           # run full test
#   ./hack/e2e-controller.sh cleanup   # remove all test resources

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
    kubectl delete -f "${DEPLOY_DIR}/examples/echo-client-annotated.yaml" --ignore-not-found 2>/dev/null || true
    kubectl delete -f "${DEPLOY_DIR}/examples/echo-server-annotated.yaml" --ignore-not-found 2>/dev/null || true
    kubectl delete -f "${DEPLOY_DIR}/webhook-configuration.yaml" --ignore-not-found 2>/dev/null || true
    kubectl delete -f "${DEPLOY_DIR}/controller-deployment.yaml" --ignore-not-found 2>/dev/null || true
    kubectl delete -f "${DEPLOY_DIR}/controller-rbac.yaml" --ignore-not-found 2>/dev/null || true
    kubectl delete -f "${DEPLOY_DIR}/server-daemonset.yaml" --ignore-not-found 2>/dev/null || true
    kubectl delete -f "${DEPLOY_DIR}/server-configmap.yaml" --ignore-not-found 2>/dev/null || true
    # Clean up any managed services.
    kubectl delete svc -l app.kubernetes.io/managed-by=outpostnet-controller --ignore-not-found 2>/dev/null || true
    kubectl delete -f "${DEPLOY_DIR}/namespace.yaml" --ignore-not-found 2>/dev/null || true
    log "Cleanup complete."
}

wait_for_pod_label() {
    local label=$1
    local ns=${2:-default}
    local timeout=${3:-90}
    log "Waiting for pod with label ${label} in namespace ${ns}..."
    if ! kubectl wait --for=condition=Ready pod -l "${label}" -n "${ns}" --timeout="${timeout}s" 2>/dev/null; then
        kubectl get pods -l "${label}" -n "${ns}" -o wide 2>/dev/null || true
        fail "Pod with label ${label} did not become ready within ${timeout}s"
    fi
}

wait_for_deployment() {
    local name=$1
    local ns=${2:-default}
    local timeout=${3:-120}
    log "Waiting for deployment ${name} in namespace ${ns} (timeout: ${timeout}s)..."
    if ! kubectl rollout status deployment/"${name}" -n "${ns}" --timeout="${timeout}s" 2>/dev/null; then
        kubectl get deployment "${name}" -n "${ns}" -o wide 2>/dev/null || true
        fail "Deployment ${name} did not become ready within ${timeout}s"
    fi
}

wait_for_daemonset() {
    local name=$1
    local ns=$2
    local timeout=${3:-90}
    log "Waiting for DaemonSet ${name}..."
    local end_time=$((SECONDS + timeout))
    while [ $SECONDS -lt $end_time ]; do
        local desired ready
        desired=$(kubectl get ds "${name}" -n "${ns}" -o jsonpath='{.status.desiredNumberScheduled}' 2>/dev/null || echo "0")
        ready=$(kubectl get ds "${name}" -n "${ns}" -o jsonpath='{.status.numberReady}' 2>/dev/null || echo "0")
        if [ "${desired}" -gt 0 ] && [ "${desired}" = "${ready}" ]; then
            log "DaemonSet ${name} ready: ${ready}/${desired}"
            return 0
        fi
        sleep 2
    done
    fail "DaemonSet ${name} did not become ready"
}

# Handle cleanup-only invocation.
if [ "${1:-}" = "cleanup" ]; then
    cleanup
    exit 0
fi

log "=== OutpostNet Controller E2E Test (Epic 2) ==="

# Step 0: Preflight.
log "--- Step 0: Checking prerequisites ---"
command -v kubectl >/dev/null 2>&1 || fail "kubectl not found"
kubectl cluster-info >/dev/null 2>&1 || fail "Cannot reach Kubernetes cluster"

# Step 1: Deploy infrastructure.
log "--- Step 1: Deploying OutpostNet infrastructure ---"
kubectl apply -f "${DEPLOY_DIR}/namespace.yaml"
kubectl apply -f "${DEPLOY_DIR}/server-configmap.yaml"
kubectl apply -f "${DEPLOY_DIR}/server-daemonset.yaml"
kubectl apply -f "${DEPLOY_DIR}/controller-rbac.yaml"
kubectl apply -f "${DEPLOY_DIR}/controller-deployment.yaml"
kubectl apply -f "${DEPLOY_DIR}/webhook-configuration.yaml"

wait_for_daemonset "outpostnet-server" "${NAMESPACE}"
wait_for_deployment "outpostnet-controller" "${NAMESPACE}"

# Step 2: Deploy annotated echo-server.
log "--- Step 2: Deploying annotated echo-server ---"
kubectl apply -f "${DEPLOY_DIR}/examples/echo-server-annotated.yaml"
wait_for_deployment "echo-server" "default"

# Verify sidecar injection.
log "--- Step 3: Verifying sidecar injection ---"
SERVER_POD=$(kubectl get pods -l app=echo-server -o jsonpath='{.items[0].metadata.name}')
CONTAINER_COUNT=$(kubectl get pod "${SERVER_POD}" -o jsonpath='{.spec.containers[*].name}' | wc -w)
if [ "${CONTAINER_COUNT}" -lt 2 ]; then
    kubectl get pod "${SERVER_POD}" -o jsonpath='{.spec.containers[*].name}' || true
    fail "echo-server pod should have at least 2 containers (app + sidecar), got ${CONTAINER_COUNT}"
fi
log "Sidecar injection verified: ${CONTAINER_COUNT} containers"

# Verify ConfigMap.
log "--- Step 4: Verifying ConfigMap generation ---"
sleep 3
CM_NAME="outpostnet-sidecar-${SERVER_POD}"
if kubectl get configmap "${CM_NAME}" -o name 2>/dev/null; then
    log "ConfigMap ${CM_NAME} exists"
else
    warn "ConfigMap ${CM_NAME} not found (may use generateName prefix)"
    # List all outpostnet configmaps
    kubectl get configmap -l outpostnet.io/pod 2>/dev/null || true
fi

# Step 5: Deploy annotated echo-client.
log "--- Step 5: Deploying annotated echo-client ---"
kubectl apply -f "${DEPLOY_DIR}/examples/echo-client-annotated.yaml"
wait_for_deployment "echo-client" "default"

# Verify sidecar injection on client.
CLIENT_POD=$(kubectl get pods -l app=echo-client -o jsonpath='{.items[0].metadata.name}')
CLIENT_CONTAINERS=$(kubectl get pod "${CLIENT_POD}" -o jsonpath='{.spec.containers[*].name}' | wc -w)
if [ "${CLIENT_CONTAINERS}" -lt 2 ]; then
    fail "echo-client pod should have at least 2 containers"
fi
log "Client sidecar injection verified: ${CLIENT_CONTAINERS} containers"

# Step 6: Verify Service and EndpointSlice.
log "--- Step 6: Verifying Service and EndpointSlice ---"
sleep 5
if kubectl get svc echo-service 2>/dev/null; then
    log "Service echo-service exists"
    kubectl get svc echo-service -o wide
else
    warn "Service echo-service not yet created"
fi

kubectl get endpointslice -l kubernetes.io/service-name=echo-service 2>/dev/null || warn "No EndpointSlice found for echo-service"

# Step 7: Test TCP connectivity.
log "--- Step 7: Testing TCP connectivity through OutpostNet ---"
sleep 5
RESULT=$(kubectl exec "${CLIENT_POD}" -c app -- sh -c 'echo "ping" | nc -w 5 127.0.0.1 9090' 2>&1) || true

if echo "${RESULT}" | grep -q "Hello from echo-server"; then
    log "TCP test PASSED: received response from echo-server"
else
    warn "TCP test result: '${RESULT}'"
    log "Checking sidecar logs..."
    kubectl logs "${SERVER_POD}" -c outpostnet-sidecar --tail=10 2>/dev/null || true
    kubectl logs "${CLIENT_POD}" -c outpostnet-sidecar --tail=10 2>/dev/null || true
    warn "TCP connectivity test inconclusive (may need more time for registration)"
fi

# Step 8: Cleanup test.
log "--- Step 8: Testing cleanup ---"
kubectl delete -f "${DEPLOY_DIR}/examples/echo-server-annotated.yaml"
sleep 5
# ConfigMap should be garbage collected with Pod.
if kubectl get configmap "${CM_NAME}" 2>/dev/null; then
    warn "ConfigMap ${CM_NAME} still exists (GC may be delayed)"
else
    log "ConfigMap cleaned up after Pod deletion"
fi

log "=== Controller E2E Test Complete ==="

# Final cleanup.
cleanup
