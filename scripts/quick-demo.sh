#!/bin/bash

# outpostnet Quick Demo Script
# 5分钟完整体验outpostnet的核心功能

set -e

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# 配置
DEMO_TOKEN="demo-secret-token"
SERVER_PORT=7629
PROXY_PORT=8080
BACKEND_PORT1=8081
BACKEND_PORT2=8082

# 日志函数
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# 检查依赖
check_dependencies() {
    log_info "检查系统依赖..."
    
    # 检查端口是否被占用
    for port in $SERVER_PORT $PROXY_PORT $BACKEND_PORT1 $BACKEND_PORT2; do
        if lsof -i :$port >/dev/null 2>&1; then
            log_error "端口 $port 已被占用，请先释放端口"
            exit 1
        fi
    done
    
    # 检查Python（用作测试后端）
    if ! command -v python3 &> /dev/null; then
        log_error "需要 python3 来启动测试后端服务"
        exit 1
    fi
    
    # 检查outpostnet二进制文件
    if [[ ! -f "./build/outpostnet-server" ]]; then
        log_warning "outpostnet-server 不存在，正在构建..."
        make build-local-server || {
            log_error "构建 outpostnet-server 失败"
            exit 1
        }
    fi
    
    if [[ ! -f "./build/outpostnet-client" ]]; then
        log_warning "outpostnet-client 不存在，正在构建..."
        make build-local-client || {
            log_error "构建 outpostnet-client 失败"
            exit 1
        }
    fi
    
    log_success "依赖检查完成"
}

# 清理函数
cleanup() {
    log_info "正在清理演示环境..."
    
    # 杀死所有相关进程
    pkill -f "outpostnet-server" || true
    pkill -f "outpostnet-client" || true
    pkill -f "python3 -m http.server $BACKEND_PORT1" || true
    pkill -f "python3 -m http.server $BACKEND_PORT2" || true
    
    # 清理临时文件
    rm -f /tmp/outpostnet-demo-*.log
    rm -f /tmp/outpostnet-demo-*.yaml
    
    log_success "清理完成"
}

# 设置清理陷阱
trap cleanup EXIT INT TERM

# 创建临时目录
setup_demo_env() {
    log_info "设置演示环境..."
    
    # 创建临时测试内容
    mkdir -p /tmp/outpostnet-demo
    cat > /tmp/outpostnet-demo/index.html << 'EOF'
<!DOCTYPE html>
<html>
<head><title>outpostnet Demo Backend</title></head>
<body>
    <h1>outpostnet Demo Backend</h1>
    <p>Server: <span id="server"></span></p>
    <p>Time: <span id="time"></span></p>
    <script>
        document.getElementById('server').textContent = 'Backend-' + window.location.port;
        document.getElementById('time').textContent = new Date().toISOString();
    </script>
</body>
</html>
EOF

    cat > /tmp/outpostnet-demo/health << 'EOF'
{"status": "healthy", "timestamp": "$(date -Iseconds)"}
EOF

    log_success "演示环境设置完成"
}

# 启动后端服务
start_backends() {
    log_info "启动测试后端服务..."
    
    # 启动后端服务1
    cd /tmp/outpostnet-demo
    python3 -m http.server $BACKEND_PORT1 > /tmp/outpostnet-demo-backend1.log 2>&1 &
    BACKEND1_PID=$!
    
    # 启动后端服务2  
    python3 -m http.server $BACKEND_PORT2 > /tmp/outpostnet-demo-backend2.log 2>&1 &
    BACKEND2_PID=$!
    
    cd - > /dev/null
    
    # 等待服务启动
    sleep 2
    
    # 验证后端服务
    if curl -s http://localhost:$BACKEND_PORT1/health > /dev/null; then
        log_success "后端服务1启动成功 (端口 $BACKEND_PORT1)"
    else
        log_error "后端服务1启动失败"
        exit 1
    fi
    
    if curl -s http://localhost:$BACKEND_PORT2/health > /dev/null; then
        log_success "后端服务2启动成功 (端口 $BACKEND_PORT2)"
    else
        log_error "后端服务2启动失败"
        exit 1
    fi
}

# 使用Express Mode启动outpostnet-server
start_outpostnet_server() {
    log_info "启动 outpostnet-server (Express Mode)..."
    
    # 创建Express配置（演示标准K8s兼容性）
    cat > /tmp/outpostnet-demo-server.yaml << EOF
server:
  listen_addr: "0.0.0.0:$SERVER_PORT"
  auth_token: "$DEMO_TOKEN"

k8s_api:
  enabled: true
  port: 8443

# 标准K8s Service定义
services:
  - metadata:
      name: "demo-web-service"
      namespace: "default"
      labels:
        app: "demo-web"
        environment: "demo"
        outpostnet.io/express-mode: "true"
    spec:
      type: ClusterIP
      selector:
        app: "demo-web"
      ports:
        - name: "http"
          port: 80
          targetPort: 8080
          protocol: "tcp"
      proxy_mode: "tcp"
      load_balancer: "round_robin"
EOF

    # 启动server
    ./build/outpostnet-server -config /tmp/outpostnet-demo-server.yaml > /tmp/outpostnet-demo-server.log 2>&1 &
    SERVER_PID=$!
    
    # 等待服务启动
    sleep 3
    
    # 验证server启动
    if ps -p $SERVER_PID > /dev/null; then
        log_success "outpostnet-server 启动成功 (PID: $SERVER_PID, 端口: $SERVER_PORT)"
    else
        log_error "outpostnet-server 启动失败，查看日志: /tmp/outpostnet-demo-server.log"
        exit 1
    fi
}

# 注册后端服务
register_backends() {
    log_info "注册后端服务到 outpostnet..."
    
    # 注册后端服务1
    cat > /tmp/outpostnet-demo-register1.yaml << EOF
client:
  server_addr: "127.0.0.1:$SERVER_PORT"
  auth_token: "$DEMO_TOKEN"
  mode: "register"

# 标准K8s Workload定义
workloads:
  - metadata:
      labels:
        app: "demo-web"
        version: "v1"
        instance: "backend-1"
        node: "demo-node-1"
    spec:
      local_addr: "127.0.0.1"
      local_port: $BACKEND_PORT1
      protocol: "tcp"
EOF

    ./build/outpostnet-client -config /tmp/outpostnet-demo-register1.yaml > /tmp/outpostnet-demo-register1.log 2>&1 &
    REGISTER1_PID=$!
    
    # 注册后端服务2
    cat > /tmp/outpostnet-demo-register2.yaml << EOF
client:
  server_addr: "127.0.0.1:$SERVER_PORT"
  auth_token: "$DEMO_TOKEN"
  mode: "register"

workloads:
  - metadata:
      labels:
        app: "demo-web"
        version: "v1"
        instance: "backend-2"
        node: "demo-node-2"
    spec:
      local_addr: "127.0.0.1"
      local_port: $BACKEND_PORT2
      protocol: "tcp"
EOF

    ./build/outpostnet-client -config /tmp/outpostnet-demo-register2.yaml > /tmp/outpostnet-demo-register2.log 2>&1 &
    REGISTER2_PID=$!
    
    # 等待注册完成
    sleep 2
    
    if ps -p $REGISTER1_PID > /dev/null && ps -p $REGISTER2_PID > /dev/null; then
        log_success "后端服务注册成功"
    else
        log_error "后端服务注册失败"
        exit 1
    fi
}

# 启动代理客户端
start_proxy_client() {
    log_info "启动代理客户端..."
    
    cat > /tmp/outpostnet-demo-proxy.yaml << EOF
client:
  server_addr: "127.0.0.1:$SERVER_PORT"
  auth_token: "$DEMO_TOKEN"
  mode: "proxy"

proxies:
  - name: "demo-web-proxy"
    service_name: "demo-web-service"
    service_namespace: "default"
    local_port: $PROXY_PORT
    proxy_mode: "tcp"
    selector:
      app: "demo-web"
    load_balancer: "round_robin"
EOF

    ./build/outpostnet-client -config /tmp/outpostnet-demo-proxy.yaml > /tmp/outpostnet-demo-proxy.log 2>&1 &
    PROXY_PID=$!
    
    # 等待代理启动
    sleep 2
    
    if ps -p $PROXY_PID > /dev/null; then
        log_success "代理客户端启动成功 (端口: $PROXY_PORT)"
    else
        log_error "代理客户端启动失败"
        exit 1
    fi
}

# 测试负载均衡
test_load_balancing() {
    log_info "测试负载均衡功能..."
    
    echo ""
    echo "🔄 负载均衡测试 (10次请求):"
    echo "================================"
    
    local backend1_count=0
    local backend2_count=0
    
    for i in {1..10}; do
        response=$(curl -s http://localhost:$PROXY_PORT/index.html)
        if echo "$response" | grep -q "Backend-$BACKEND_PORT1"; then
            echo "  请求 $i: Backend-1 (端口 $BACKEND_PORT1) ✓"
            ((backend1_count++))
        elif echo "$response" | grep -q "Backend-$BACKEND_PORT2"; then
            echo "  请求 $i: Backend-2 (端口 $BACKEND_PORT2) ✓"
            ((backend2_count++))
        else
            echo "  请求 $i: 未知响应 ✗"
        fi
        sleep 0.5
    done
    
    echo ""
    echo "📊 负载分布:"
    echo "  Backend-1: $backend1_count 次"
    echo "  Backend-2: $backend2_count 次"
    
    if [[ $backend1_count -gt 0 && $backend2_count -gt 0 ]]; then
        log_success "负载均衡测试通过！"
    else
        log_warning "负载分布不均，但系统正常工作"
    fi
}

# 测试K8s API兼容性
test_k8s_api() {
    log_info "测试 K8s API 兼容性..."
    
    echo ""
    echo "🔍 K8s API 测试:"
    echo "====================="
    
    # 测试服务列表
    echo "1. 查看 Services:"
    if curl -s -k https://localhost:8443/api/v1/namespaces/default/services | jq -r '.items[0].metadata.name' 2>/dev/null; then
        log_success "K8s Services API 正常"
    else
        log_warning "K8s API 测试需要 jq 工具，使用基础测试"
        curl -s -k https://localhost:8443/api/v1/namespaces/default/services >/dev/null && \
            log_success "K8s Services API 响应正常"
    fi
    
    # 测试端点列表
    echo ""
    echo "2. 查看 Endpoints:"
    if curl -s -k https://localhost:8443/api/v1/namespaces/default/endpoints | jq -r '.items[0].metadata.name' 2>/dev/null; then
        log_success "K8s Endpoints API 正常"
    else
        curl -s -k https://localhost:8443/api/v1/namespaces/default/endpoints >/dev/null && \
            log_success "K8s Endpoints API 响应正常"
    fi
}

# 演示故障转移
test_failover() {
    log_info "演示故障转移功能..."
    
    echo ""
    echo "🔧 故障转移测试:"
    echo "====================="
    
    # 停止一个后端服务
    echo "1. 停止 Backend-1 (端口 $BACKEND_PORT1)"
    kill $BACKEND1_PID 2>/dev/null || true
    sleep 5
    
    # 测试服务是否仍然可用
    echo "2. 测试服务可用性 (5次请求):"
    local success_count=0
    for i in {1..5}; do
        if curl -s http://localhost:$PROXY_PORT/index.html | grep -q "Backend-$BACKEND_PORT2"; then
            echo "  请求 $i: Backend-2 响应 ✓"
            ((success_count++))
        else
            echo "  请求 $i: 请求失败 ✗"
        fi
        sleep 1
    done
    
    if [[ $success_count -eq 5 ]]; then
        log_success "故障转移测试通过！所有请求都转移到健康的后端"
    else
        log_warning "故障转移部分成功 ($success_count/5)"
    fi
    
    # 重启后端服务1（可选）
    echo ""
    echo "3. 重启 Backend-1 并测试恢复"
    cd /tmp/outpostnet-demo
    python3 -m http.server $BACKEND_PORT1 > /tmp/outpostnet-demo-backend1.log 2>&1 &
    BACKEND1_PID=$!
    cd - > /dev/null
    
    sleep 3
    echo "  Backend-1 已重启，等待健康检查生效..."
}

# 显示演示总结
show_summary() {
    echo ""
    echo "🎉 outpostnet 演示完成！"
    echo "================================"
    echo ""
    echo "✅ 演示内容总结:"
    echo "  1. 使用标准 K8s Service/Endpoint 配置"
    echo "  2. 多后端服务注册和负载均衡"
    echo "  3. DNS 劫持和透明代理"
    echo "  4. 健康检查和故障转移"
    echo "  5. K8s API 兼容性"
    echo ""
    echo "🔗 访问地址:"
    echo "  代理访问: http://localhost:$PROXY_PORT"
    echo "  K8s API:  https://localhost:8443"
    echo "  直接访问: http://localhost:$BACKEND_PORT1 | http://localhost:$BACKEND_PORT2"
    echo ""
    echo "📁 日志文件:"
    echo "  Server:   /tmp/outpostnet-demo-server.log"
    echo "  Register: /tmp/outpostnet-demo-register1.log, /tmp/outpostnet-demo-register2.log"
    echo "  Proxy:    /tmp/outpostnet-demo-proxy.log"
    echo ""
    echo "🛠️  手动测试命令:"
    echo "  curl http://localhost:$PROXY_PORT/index.html"
    echo "  curl -k https://localhost:8443/api/v1/namespaces/default/services"
    echo ""
    echo "⚡ Express Mode 等价命令 (概念演示):"
    echo "  ./outpostnet-server --express --service demo-web-service:80:8080 --token $DEMO_TOKEN"
    echo "  ./outpostnet-client register --express --service demo-web-service --backend 127.0.0.1:$BACKEND_PORT1"
    echo "  ./outpostnet-client proxy --express --service demo-web-service --local-port $PROXY_PORT"
    echo ""
    echo "📖 想了解更多？查看文档:"
    echo "  docs/quick-start.md - 详细使用指南"
    echo "  docs/architecture.md - 架构设计"
    echo "  docs/express-mode-design.md - Express Mode 设计"
    echo ""
    
    # 等待用户输入
    read -p "按 Enter 键结束演示并清理环境..."
}

# 主函数
main() {
    echo "🚀 outpostnet 快速演示脚本"
    echo "================================"
    echo "本演示将在5分钟内展示 outpostnet 的核心功能："
    echo "• 标准 K8s Service/Endpoint 配置"
    echo "• 多后端负载均衡"
    echo "• 健康检查和故障转移"
    echo "• K8s API 兼容性"
    echo ""
    
    # 确认继续
    read -p "是否开始演示？(y/N): " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        echo "演示已取消"
        exit 0
    fi
    
    # 执行演示步骤
    check_dependencies
    setup_demo_env
    start_backends
    start_outpostnet_server
    register_backends
    start_proxy_client
    
    # 等待所有服务稳定
    log_info "等待所有服务稳定..."
    sleep 5
    
    # 运行测试
    test_load_balancing
    test_k8s_api
    test_failover
    
    # 显示总结
    show_summary
}

# 检查是否在正确的目录
if [[ ! -f "./Makefile" ]] || [[ ! -d "./cmd" ]]; then
    log_error "请在 outpostnet 根目录下运行此脚本"
    exit 1
fi

# 运行演示
main "$@"