#!/bin/bash

# OutpostNet TCP 代理示例 - 一键运行脚本
# 
# 此脚本会自动启动服务端、注册客户端、代理客户端和测试Web服务
# 然后测试代理功能

set -e

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

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

# 检查是否在正确的目录
if [ ! -f "../../build/outpostnet-server" ]; then
    log_error "找不到 outpostnet-server 可执行文件"
    log_error "请先运行 'make build' 构建项目"
    exit 1
fi

# 清理函数
cleanup() {
    log_info "正在清理进程..."
    pkill -f "outpostnet-server.*demo-server-config.yaml" 2>/dev/null || true
    pkill -f "outpostnet-client.*demo-register-config.yaml" 2>/dev/null || true  
    pkill -f "outpostnet-client.*demo-proxy-config.yaml" 2>/dev/null || true
    pkill -f "python.*8000" 2>/dev/null || true
    
    # 等待端口释放
    sleep 2
    log_success "清理完成"
}

# 注册信号处理
trap cleanup EXIT INT TERM

log_info "=== OutpostNet TCP 代理示例 ==="
log_info ""

# 检查端口占用
check_port() {
    local port=$1
    if netstat -tln 2>/dev/null | grep -q ":$port "; then
        log_warning "端口 $port 已被占用，正在尝试释放..."
        lsof -ti:$port | xargs kill -9 2>/dev/null || true
        sleep 1
    fi
}

log_info "检查端口占用..."
check_port 17000  # 服务端口
check_port 18000  # 后端服务端口  
check_port 19999  # 代理端口

# 启动简单的HTTP服务器作为后端服务
log_info "启动后端 Web 服务 (端口 18000)..."

python3 test-web-server.py &
WEBSERVER_PID=$!

sleep 2
log_success "后端 Web 服务已启动 (PID: $WEBSERVER_PID)"

# 启动 OutpostNet 服务端
log_info "启动 OutpostNet 服务端 (端口 17000)..."
../../build/outpostnet-server -config demo-server-config.yaml > /tmp/outpostnet-server.log 2>&1 &
SERVER_PID=$!
sleep 3

if ! kill -0 $SERVER_PID 2>/dev/null; then
    log_error "服务端启动失败，查看日志："
    cat /tmp/outpostnet-server.log
    exit 1
fi
log_success "OutpostNet 服务端已启动 (PID: $SERVER_PID)"

# 启动注册客户端
log_info "启动服务注册客户端..."
../../build/outpostnet-client -config demo-register-config.yaml > /tmp/outpostnet-register.log 2>&1 &
REGISTER_PID=$!
sleep 2

if ! kill -0 $REGISTER_PID 2>/dev/null; then
    log_error "注册客户端启动失败，查看日志："
    cat /tmp/outpostnet-register.log
    exit 1
fi
log_success "服务注册客户端已启动 (PID: $REGISTER_PID)"

# 启动代理客户端
log_info "启动代理客户端 (本地端口 19999)..."
../../build/outpostnet-client -config demo-proxy-config.yaml > /tmp/outpostnet-proxy.log 2>&1 &
PROXY_PID=$!
sleep 2

if ! kill -0 $PROXY_PID 2>/dev/null; then
    log_error "代理客户端启动失败，查看日志："
    cat /tmp/outpostnet-proxy.log
    exit 1
fi
log_success "代理客户端已启动 (PID: $PROXY_PID)"

# 等待服务就绪
log_info "等待服务就绪..."
sleep 3

# 测试连接
log_info "测试代理连接..."
log_info ""

# 测试1：基本连接测试
log_info "📡 测试1：基本连接"
if curl -s --connect-timeout 5 http://localhost:19999 > /dev/null; then
    log_success "✅ 基本连接测试通过"
else
    log_error "❌ 基本连接测试失败"
    log_info "查看日志排查问题："
    echo "  - 服务端日志: tail -f /tmp/outpostnet-server.log"
    echo "  - 注册日志: tail -f /tmp/outpostnet-register.log"  
    echo "  - 代理日志: tail -f /tmp/outpostnet-proxy.log"
    exit 1
fi

# 测试2：内容验证
log_info "📄 测试2：响应内容验证"
RESPONSE=$(curl -s http://localhost:19999)
if echo "$RESPONSE" | grep -q "OutpostNet TCP 代理测试成功"; then
    log_success "✅ 响应内容验证通过"
else
    log_warning "⚠️  响应内容验证失败，但连接正常"
fi

# 测试3：并发测试
log_info "🚀 测试3：并发连接测试"
CONCURRENT_SUCCESS=0
for i in {1..5}; do
    if curl -s --connect-timeout 3 http://localhost:19999 > /dev/null; then
        ((CONCURRENT_SUCCESS++))
    fi &
done
wait

if [ $CONCURRENT_SUCCESS -eq 5 ]; then
    log_success "✅ 并发连接测试通过 (5/5)"
else
    log_warning "⚠️  并发连接测试部分成功 ($CONCURRENT_SUCCESS/5)"
fi

log_info ""
log_success "🎉 OutpostNet TCP 代理示例运行成功！"
log_info ""
log_info "📋 服务信息："
log_info "  - 后端服务:   http://localhost:18000"
log_info "  - 代理地址:   http://localhost:19999  ⭐"
log_info "  - 服务端:     localhost:17000"
log_info ""
log_info "🧪 测试命令："
log_info "  curl http://localhost:19999"
log_info "  curl -i http://localhost:19999"
log_info ""
log_info "📊 查看实时响应："
echo "curl http://localhost:19999"
echo ""

# 显示响应内容
curl -s http://localhost:19999

log_info ""
log_info "按 Ctrl+C 停止所有服务"

# 保持运行
while true; do
    sleep 1
    # 检查进程是否还在运行
    if ! kill -0 $SERVER_PID 2>/dev/null || ! kill -0 $REGISTER_PID 2>/dev/null || ! kill -0 $PROXY_PID 2>/dev/null; then
        log_warning "检测到进程退出，正在重启..."
        break
    fi
done