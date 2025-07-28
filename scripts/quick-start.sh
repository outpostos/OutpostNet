#!/bin/bash

# outpostnet 快速开始脚本
# 这个脚本会启动一个完整的 outpostnet 演示环境

set -e

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# 打印带颜色的消息
print_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# 检查依赖
check_dependencies() {
    print_info "检查依赖..."
    
    if ! command -v go &> /dev/null; then
        print_error "Go 未安装，请先安装 Go 1.23.0 或更高版本"
        exit 1
    fi
    
    if ! command -v curl &> /dev/null; then
        print_error "curl 未安装，请先安装 curl"
        exit 1
    fi
    
    print_success "依赖检查通过"
}

# 构建 outpostnet
build_outpostnet() {
    print_info "构建 outpostnet..."
    
    if [ ! -f "go.mod" ]; then
        print_error "请在 outpostnet 项目根目录运行此脚本"
        exit 1
    fi
    
    go build -o outpostnet-server ./cmd/outpostnet-server
    go build -o outpostnet-client ./cmd/outpostnet-client
    
    print_success "构建完成"
}

# 启动后端服务
start_backend_service() {
    print_info "启动后端服务 (Python HTTP Server)..."
    
    # 检查端口是否被占用
    if lsof -i :8080 &> /dev/null; then
        print_warning "端口 8080 已被占用，尝试终止占用进程..."
        lsof -ti :8080 | xargs kill -9 2>/dev/null || true
        sleep 2
    fi
    
    # 启动 Python HTTP 服务器作为后端服务
    cd /tmp
    echo "<html><body><h1>Hello from outpostnet backend!</h1><p>Time: $(date)</p></body></html>" > index.html
    python3 -m http.server 8080 > /dev/null 2>&1 &
    BACKEND_PID=$!
    cd - > /dev/null
    
    # 等待服务启动
    sleep 2
    if ! curl -s http://localhost:8080 > /dev/null; then
        print_error "后端服务启动失败"
        exit 1
    fi
    
    print_success "后端服务已启动 (PID: $BACKEND_PID)"
    echo $BACKEND_PID > /tmp/outpostnet-backend.pid
}

# 启动 outpostnet-server
start_outpostnet_server() {
    print_info "启动 outpostnet-server..."
    
    # 检查端口是否被占用
    if lsof -i :7000 &> /dev/null; then
        print_warning "端口 7000 已被占用，尝试终止占用进程..."
        lsof -ti :7000 | xargs kill -9 2>/dev/null || true
        sleep 2
    fi
    
    # 启动 outpostnet-server
    ./outpostnet-server -config examples/server-config.yaml > /tmp/outpostnet-server.log 2>&1 &
    SERVER_PID=$!
    
    # 等待服务启动
    sleep 3
    if ! ps -p $SERVER_PID > /dev/null; then
        print_error "outpostnet-server 启动失败，查看日志: /tmp/outpostnet-server.log"
        exit 1
    fi
    
    print_success "outpostnet-server 已启动 (PID: $SERVER_PID)"
    echo $SERVER_PID > /tmp/outpostnet-server.pid
}

# 启动注册客户端
start_register_client() {
    print_info "启动 outpostnet-client (register 模式)..."
    
    # 启动注册客户端
    ./outpostnet-client -config examples/client-register-config.yaml > /tmp/outpostnet-register.log 2>&1 &
    REGISTER_PID=$!
    
    # 等待服务启动
    sleep 2
    if ! ps -p $REGISTER_PID > /dev/null; then
        print_error "outpostnet-client (register) 启动失败，查看日志: /tmp/outpostnet-register.log"
        exit 1
    fi
    
    print_success "outpostnet-client (register) 已启动 (PID: $REGISTER_PID)"
    echo $REGISTER_PID > /tmp/outpostnet-register.pid
}

# 启动代理客户端
start_proxy_client() {
    print_info "启动 outpostnet-client (proxy 模式)..."
    
    # 检查端口是否被占用
    if lsof -i :9080 &> /dev/null; then
        print_warning "端口 9080 已被占用，尝试终止占用进程..."
        lsof -ti :9080 | xargs kill -9 2>/dev/null || true
        sleep 2
    fi
    
    # 创建临时的代理配置（使用不同的端口避免冲突）
    cat > /tmp/proxy-config.yaml << EOF
client:
  server_addr: "127.0.0.1:7000"
  auth_token: "test-client-token"
  mode: "proxy"

proxies:
  - name: "web-proxy"
    service_name: "web-service"
    service_namespace: "default"
    local_port: 9080
    proxy_mode: "tcp"
    selector:
      app: "web"
    load_balancer: "round_robin"
EOF
    
    # 启动代理客户端
    ./outpostnet-client -config /tmp/proxy-config.yaml > /tmp/outpostnet-proxy.log 2>&1 &
    PROXY_PID=$!
    
    # 等待服务启动
    sleep 2
    if ! ps -p $PROXY_PID > /dev/null; then
        print_error "outpostnet-client (proxy) 启动失败，查看日志: /tmp/outpostnet-proxy.log"
        exit 1
    fi
    
    print_success "outpostnet-client (proxy) 已启动 (PID: $PROXY_PID)"
    echo $PROXY_PID > /tmp/outpostnet-proxy.pid
}

# 测试连接
test_connection() {
    print_info "等待服务完全启动..."
    sleep 5
    
    print_info "测试连接..."
    
    # 测试直接访问后端服务
    print_info "测试直接访问后端服务 (http://localhost:8080)..."
    if curl -s http://localhost:8080 | grep -q "Hello from outpostnet backend"; then
        print_success "✓ 直接访问后端服务成功"
    else
        print_warning "! 直接访问后端服务失败"
    fi
    
    # 测试通过代理访问
    print_info "测试通过 outpostnet 代理访问 (http://localhost:9080)..."
    if curl -s http://localhost:9080 | grep -q "Hello from outpostnet backend"; then
        print_success "✓ 通过 outpostnet 代理访问成功"
        print_success "🎉 outpostnet 演示环境运行正常!"
    else
        print_warning "! 通过 outpostnet 代理访问失败"
        print_info "检查日志文件:"
        print_info "  Server: /tmp/outpostnet-server.log"
        print_info "  Register: /tmp/outpostnet-register.log"
        print_info "  Proxy: /tmp/outpostnet-proxy.log"
    fi
}

# 显示状态信息
show_status() {
    echo
    print_info "=== outpostnet 演示环境状态 ==="
    echo
    echo "服务访问地址:"
    echo "  • 后端服务 (直接):    http://localhost:8080"
    echo "  • 通过 outpostnet 代理:  http://localhost:9080"
    echo
    echo "日志文件:"
    echo "  • outpostnet-server:     /tmp/outpostnet-server.log"
    echo "  • outpostnet-register:   /tmp/outpostnet-register.log"
    echo "  • outpostnet-proxy:      /tmp/outpostnet-proxy.log"
    echo
    echo "进程 ID:"
    echo "  • 后端服务:           $(cat /tmp/outpostnet-backend.pid 2>/dev/null || echo 'Unknown')"
    echo "  • outpostnet-server:     $(cat /tmp/outpostnet-server.pid 2>/dev/null || echo 'Unknown')"
    echo "  • outpostnet-register:   $(cat /tmp/outpostnet-register.pid 2>/dev/null || echo 'Unknown')"
    echo "  • outpostnet-proxy:      $(cat /tmp/outpostnet-proxy.pid 2>/dev/null || echo 'Unknown')"
    echo
    print_info "测试命令:"
    echo "  curl http://localhost:8080   # 直接访问后端"
    echo "  curl http://localhost:9080   # 通过代理访问"
    echo
    print_info "停止演示环境:"
    echo "  ./scripts/quick-start.sh stop"
}

# 停止服务
stop_services() {
    print_info "停止 outpostnet 演示环境..."
    
    # 停止各个进程
    for pidfile in /tmp/outpostnet-backend.pid /tmp/outpostnet-server.pid /tmp/outpostnet-register.pid /tmp/outpostnet-proxy.pid; do
        if [ -f "$pidfile" ]; then
            pid=$(cat "$pidfile")
            if ps -p $pid > /dev/null 2>&1; then
                kill $pid
                print_success "已停止进程 $pid"
            fi
            rm -f "$pidfile"
        fi
    done
    
    # 清理临时文件
    rm -f /tmp/proxy-config.yaml
    rm -f /tmp/outpostnet-*.log
    rm -f /tmp/index.html
    
    print_success "outpostnet 演示环境已停止"
}

# 主函数
main() {
    case "${1:-start}" in
        "start")
            print_info "启动 outpostnet 演示环境..."
            check_dependencies
            build_outpostnet
            start_backend_service
            start_outpostnet_server
            start_register_client
            start_proxy_client
            test_connection
            show_status
            ;;
        "stop")
            stop_services
            ;;
        "status")
            show_status
            ;;
        "test")
            test_connection
            ;;
        *)
            echo "用法: $0 {start|stop|status|test}"
            echo
            echo "命令:"
            echo "  start   - 启动 outpostnet 演示环境 (默认)"
            echo "  stop    - 停止 outpostnet 演示环境"
            echo "  status  - 显示服务状态"
            echo "  test    - 测试连接"
            exit 1
            ;;
    esac
}

# 捕获 Ctrl+C 信号
trap 'echo; print_info "收到中断信号，停止服务..."; stop_services; exit 0' INT

# 运行主函数
main "$@"