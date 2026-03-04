#!/bin/bash

# UDP 代理功能测试脚本
set -e

echo "🚀 Starting UDP proxy functionality test..."

# 清理函数
cleanup() {
    echo "🧹 Cleaning up processes..."
    pkill -f "outpostnet-server" || true
    pkill -f "outpostnet-client" || true
    pkill -f "test_udp_server" || true
    sleep 2
}

# 设置清理陷阱
trap cleanup EXIT

# 步骤 1: 启动 UDP 测试服务器 (18001 端口)
echo "📡 Starting UDP test server on port 18001..."
go run ../tools/udp-server/main.go &
UDP_SERVER_PID=$!
sleep 2

# 步骤 2: 启动 outpostnet-server
echo "🌐 Starting outpostnet-server..."
../../build/outpostnet-server -config ../configs/demo-udp-server-config.yaml &
KEELNET_SERVER_PID=$!
sleep 3

# 步骤 3: 启动 outpostnet-client (register 模式)
echo "📝 Starting outpostnet-client in register mode..."
../../build/outpostnet-client -config ../configs/demo-udp-register-config.yaml &
REGISTER_CLIENT_PID=$!
sleep 3

# 步骤 4: 启动 outpostnet-client (proxy 模式)  
echo "🔄 Starting outpostnet-client in proxy mode..."
../../build/outpostnet-client -config ../configs/demo-udp-proxy-config.yaml &
PROXY_CLIENT_PID=$!
sleep 3

# 步骤 5: 测试 UDP 代理功能
echo "🧪 Testing UDP proxy functionality..."
echo "📤 Testing direct connection to UDP server (port 18001)..."
go run ../tools/udp-client/main.go 18001

echo ""
echo "📤 Testing UDP proxy connection (port 19998 -> 18001)..."
go run ../tools/udp-client/main.go 19998

echo ""
echo "✅ UDP proxy test completed successfully!"

# 等待一下看结果
sleep 2