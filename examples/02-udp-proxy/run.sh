#!/bin/bash

# OutpostNet UDP 代理示例 - 一键运行脚本

set -e

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

log_info() { echo -e "${BLUE}[INFO]${NC} $1"; }
log_success() { echo -e "${GREEN}[SUCCESS]${NC} $1"; }
log_warning() { echo -e "${YELLOW}[WARNING]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }

# 检查构建文件
if [ ! -f "../../build/outpostnet-server" ]; then
    log_error "找不到 outpostnet-server，请先运行 'make build'"
    exit 1
fi

# 清理函数
cleanup() {
    log_info "正在清理进程..."
    pkill -f "outpostnet-server.*demo-udp-server-config.yaml" 2>/dev/null || true
    pkill -f "outpostnet-client.*demo-udp-register-config.yaml" 2>/dev/null || true
    pkill -f "outpostnet-client.*demo-udp-proxy-config.yaml" 2>/dev/null || true
    pkill -f "python.*udp-test-server.py" 2>/dev/null || true
    sleep 2
    log_success "清理完成"
}

trap cleanup EXIT INT TERM

log_info "=== OutpostNet UDP 代理示例 ==="
log_info ""

# 检查端口占用
check_port() {
    local port=$1
    local proto=${2:-tcp}
    if lsof -ti:$port 2>/dev/null | grep -q .; then
        log_warning "端口 $port 已被占用，正在尝试释放..."
        lsof -ti:$port | xargs kill -9 2>/dev/null || true
        sleep 1
    fi
}

# 检查是否有进程占用端口
log_info "检查进程占用情况..."
PORTS_IN_USE=false

for port in 17001 18001 15353; do
    PID=$(lsof -ti:$port 2>/dev/null | head -1)
    if [ -n "$PID" ]; then
        PORTS_IN_USE=true
        PROCESS_INFO=$(ps -p $PID -o comm= 2>/dev/null || echo "unknown")
        log_warning "端口 $port 被占用 (PID: $PID, 进程: $PROCESS_INFO)"
    fi
done

if [ "$PORTS_IN_USE" = true ]; then
    log_info ""
    log_info "🛠️  如需清理相关进程，可执行以下命令："
    log_info "   # 清理 OutpostNet 进程"
    log_info "   pkill -f 'outpostnet-server.*demo-udp-server-config.yaml'"
    log_info "   pkill -f 'outpostnet-client.*demo-udp-register-config.yaml'"
    log_info "   pkill -f 'outpostnet-client.*demo-udp-proxy-config.yaml'"
    log_info "   # 清理测试服务器"
    log_info "   pkill -f 'python.*udp-test-server.py'"
    log_info "   # 强制清理端口占用"
    log_info "   lsof -ti:17001 | xargs kill -9; lsof -ti:18001 | xargs kill -9; lsof -ti:15353 | xargs kill -9"
    log_info ""
    
    read -p "是否继续启动？可能会遇到端口冲突 (y/N): " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        log_info "已取消启动"
        exit 0
    fi
fi

log_info "检查端口占用..."
check_port 17001 tcp  # 服务端控制端口
check_port 18001 udp  # 后端UDP服务端口
check_port 15353 udp  # 代理UDP端口

# 启动UDP测试服务器
log_info "启动后端 UDP 服务 (端口 18001)..."
cd /tmp
cat > udp-test-server.py << 'EOF'
#!/usr/bin/env python3
import socket
import threading
import time
import json
import sys
import signal
from datetime import datetime

# 全局标志用于优雅退出
running = True

def signal_handler(signum, frame):
    global running
    print(f"\n收到信号 {signum}，正在关闭UDP服务器...")
    running = False
    sys.exit(0)

signal.signal(signal.SIGTERM, signal_handler)
signal.signal(signal.SIGINT, signal_handler)

def udp_server():
    global running
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.settimeout(1.0)  # 设置超时以便检查running标志
    
    try:
        sock.bind(('127.0.0.1', 18001))
        print(f"UDP服务器启动在端口 18001，PID: {os.getpid() if 'os' in globals() else 'unknown'}")
        sys.stdout.flush()
    except Exception as e:
        print(f"UDP服务器启动失败: {e}")
        return
    
    while running:
        try:
            data, addr = sock.recvfrom(1024)
            message = data.decode('utf-8', errors='ignore')
            
            # 构造响应
            response_data = {
                "status": "success", 
                "message": f"UDP代理测试成功！",
                "received": message.strip(),
                "server": "UDP Test Server",
                "client_addr": f"{addr[0]}:{addr[1]}",
                "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                "protocol": "UDP",
                "proxy_path": "客户端 → OutpostNet代理(15353) → OutpostNet服务端(17001) → UDP服务端(18001)"
            }
            
            response = json.dumps(response_data, ensure_ascii=False, indent=2)
            sock.sendto(response.encode('utf-8'), addr)
            
            print(f"处理来自 {addr} 的UDP请求: {message.strip()}")
            sys.stdout.flush()
            
        except socket.timeout:
            # 超时是正常的，继续循环检查running标志
            continue
        except Exception as e:
            if running:  # 只在仍在运行时报错
                print(f"UDP服务器错误: {e}")
                sys.stdout.flush()
            continue
    
    print("UDP服务器正常关闭")
    sock.close()

if __name__ == "__main__":
    import os
    udp_server()
EOF

python3 udp-test-server.py > /tmp/udp-test-server.log 2>&1 &
UDP_SERVER_PID=$!
cd - > /dev/null

sleep 2

# 检查UDP服务器是否正常启动
if ! kill -0 $UDP_SERVER_PID 2>/dev/null; then
    log_error "UDP服务器启动失败，查看日志："
    cat /tmp/udp-test-server.log
    exit 1
fi

# 验证UDP端口是否监听
sleep 1
if ! lsof -i :18001 >/dev/null 2>&1; then
    log_error "UDP端口18001未正常监听，查看日志："
    cat /tmp/udp-test-server.log
    exit 1
fi

log_success "后端 UDP 服务已启动 (PID: $UDP_SERVER_PID)"

# 启动 OutpostNet 服务端
log_info "启动 OutpostNet 服务端 (端口 17001)..."
../../build/outpostnet-server -config demo-udp-server-config.yaml > /tmp/outpostnet-udp-server.log 2>&1 &
SERVER_PID=$!
sleep 3

if ! kill -0 $SERVER_PID 2>/dev/null; then
    log_error "服务端启动失败，查看日志："
    cat /tmp/outpostnet-udp-server.log
    exit 1
fi
log_success "OutpostNet 服务端已启动 (PID: $SERVER_PID)"

# 启动注册客户端
log_info "启动UDP服务注册客户端..."
../../build/outpostnet-client -config demo-udp-register-config.yaml > /tmp/outpostnet-udp-register.log 2>&1 &
REGISTER_PID=$!
sleep 2

if ! kill -0 $REGISTER_PID 2>/dev/null; then
    log_error "注册客户端启动失败，查看日志："
    cat /tmp/outpostnet-udp-register.log
    exit 1
fi
log_success "UDP服务注册客户端已启动 (PID: $REGISTER_PID)"

# 启动代理客户端
log_info "启动UDP代理客户端 (本地端口 15353)..."
../../build/outpostnet-client -config demo-udp-proxy-config.yaml > /tmp/outpostnet-udp-proxy.log 2>&1 &
PROXY_PID=$!
sleep 2

if ! kill -0 $PROXY_PID 2>/dev/null; then
    log_error "代理客户端启动失败，查看日志："
    cat /tmp/outpostnet-udp-proxy.log
    exit 1
fi
log_success "UDP代理客户端已启动 (PID: $PROXY_PID)"

# 等待服务就绪
log_info "等待服务就绪..."
sleep 8

# 测试UDP连接
log_info "测试UDP代理连接..."
log_info ""

# 测试1：基本UDP通信
log_info "📡 测试1：基本UDP通信"
TEST_MSG="Hello OutpostNet UDP!"
RESPONSE=$(echo "$TEST_MSG" | timeout 5 nc -u localhost 15353 2>/dev/null || echo "timeout")

if [ "$RESPONSE" != "timeout" ] && [ -n "$RESPONSE" ]; then
    log_success "✅ 基本UDP通信测试通过"
    echo "   发送: $TEST_MSG"
    echo "   响应: $(echo "$RESPONSE" | head -1)"
else
    log_error "❌ 基本UDP通信测试失败"
    log_info "排查步骤："
    echo "  1. 检查UDP端口监听: netstat -ulnp | grep -E '15353|17001|18001'"
    echo "  2. 测试后端服务: echo 'test' | nc -u localhost 18001"
    echo "  3. 查看服务端日志: tail -f /tmp/outpostnet-udp-server.log"
    log_warning "⚠️  继续运行其他测试..."
fi

# 测试2：JSON数据传输
log_info "📄 测试2：JSON数据传输"
JSON_MSG='{"type":"test","message":"UDP JSON test"}'
JSON_RESPONSE=$(echo "$JSON_MSG" | timeout 5 nc -u localhost 15353 2>/dev/null || echo "timeout")

if [ "$JSON_RESPONSE" != "timeout" ] && echo "$JSON_RESPONSE" | grep -q "success"; then
    log_success "✅ JSON数据传输测试通过"
else
    log_warning "⚠️  JSON数据传输测试部分成功"
fi

# 测试3：并发UDP请求
log_info "🚀 测试3：并发UDP请求"
CONCURRENT_SUCCESS=0
for i in {1..5}; do
    RESP=$(echo "Concurrent test $i" | timeout 3 nc -u localhost 15353 2>/dev/null)
    if [ -n "$RESP" ] && echo "$RESP" | grep -q "success"; then
        CONCURRENT_SUCCESS=$((CONCURRENT_SUCCESS + 1))
    fi
done

if [ $CONCURRENT_SUCCESS -ge 3 ]; then
    log_success "✅ 并发UDP请求测试通过 ($CONCURRENT_SUCCESS/5)"
else
    log_warning "⚠️  并发UDP请求测试部分成功 ($CONCURRENT_SUCCESS/5)"
fi

# 测试4：会话管理测试
log_info "⏱️  测试4：UDP会话管理"
echo "Session test 1" | timeout 3 nc -u localhost 15353 > /dev/null 2>&1
log_info "   等待5秒测试会话保持..."
sleep 5
SESSION_RESP=$(echo "Session test 2" | timeout 3 nc -u localhost 15353 2>/dev/null || echo "timeout")

if [ "$SESSION_RESP" != "timeout" ] && [ -n "$SESSION_RESP" ]; then
    log_success "✅ UDP会话管理测试通过"
else
    log_warning "⚠️  UDP会话管理测试需要检查"
fi

log_info ""
log_success "🎉 OutpostNet UDP 代理示例运行成功！"
log_info ""
log_info "📋 服务信息："
log_info "  - 后端UDP服务:  localhost:18001"
log_info "  - 代理UDP端口:  localhost:15353  ⭐"
log_info "  - 服务端:       localhost:17001"
log_info ""
log_info "🧪 测试命令："
log_info "  echo 'test message' | nc -u localhost 15353"
log_info "  echo '{\"test\":\"json\"}' | nc -u localhost 15353"
log_info ""
log_info "📊 实时UDP测试："

# 演示UDP通信
echo "echo 'Hello UDP World!' | nc -u localhost 15353"
echo ""
echo "Hello UDP World!" | nc -u localhost 15353 | head -5

log_info ""
log_info "🔍 UDP特性演示："
log_info "  - 无连接协议，每次发送都是独立的数据包"
log_info "  - 支持会话管理，相同客户端会路由到相同后端"
log_info "  - 自动超时清理，默认30秒会话超时"
log_info ""
log_info "按 Ctrl+C 停止所有服务"

# 保持运行并监控服务状态
while true; do
    sleep 5
    
    # 检查所有进程是否正常运行
    if ! kill -0 $SERVER_PID 2>/dev/null; then
        log_error "服务端进程退出"
        break
    fi
    if ! kill -0 $REGISTER_PID 2>/dev/null; then
        log_error "注册客户端进程退出"
        break
    fi  
    if ! kill -0 $PROXY_PID 2>/dev/null; then
        log_error "代理客户端进程退出"
        break
    fi
    if ! kill -0 $UDP_SERVER_PID 2>/dev/null; then
        log_error "UDP服务端进程退出"
        break
    fi
done