#!/bin/bash

# STCP 加密隧道代理功能测试脚本
set -e

echo "🔐 Starting STCP encrypted tunnel functionality test..."

# 清理函数
cleanup() {
    echo "🧹 Cleaning up processes..."
    pkill -f "outpostnet-server" || true
    pkill -f "outpostnet-client" || true
    pkill -f "test_stcp_server" || true
    sleep 2
}

# 设置清理陷阱
trap cleanup EXIT

# 步骤 1: 启动 STCP 测试服务器 (18002 端口)
echo "🌐 Starting STCP test server on port 18002..."
go run ../tools/stcp-server/main.go &
STCP_SERVER_PID=$!
sleep 2

# 步骤 2: 启动 outpostnet-server
echo "🚀 Starting outpostnet-server..."
../../build/outpostnet-server -config ../configs/demo-stcp-server-config.yaml &
KEELNET_SERVER_PID=$!
sleep 3

# 步骤 3: 启动 outpostnet-client (register 模式)
echo "📝 Starting outpostnet-client in register mode..."
../../build/outpostnet-client -config ../configs/demo-stcp-register-config.yaml &
REGISTER_CLIENT_PID=$!
sleep 3

# 步骤 4: 启动 outpostnet-client (proxy 模式)  
echo "🔄 Starting outpostnet-client in STCP proxy mode..."
../../build/outpostnet-client -config ../configs/demo-stcp-proxy-config.yaml &
PROXY_CLIENT_PID=$!
sleep 3

# 步骤 5: 测试 STCP 加密隧道功能
echo "🧪 Testing STCP encrypted tunnel functionality..."
echo "📤 Testing direct connection to STCP server (port 18002)..."
curl -s http://127.0.0.1:18002/ | jq . || curl -s http://127.0.0.1:18002/

echo ""
echo "🔐 Testing STCP encrypted tunnel connection (port 19997 -> 18002)..."
curl -s http://127.0.0.1:19997/ | jq . || curl -s http://127.0.0.1:19997/

echo ""
echo "✅ STCP encrypted tunnel test completed successfully!"

# 等待一下看结果
sleep 2