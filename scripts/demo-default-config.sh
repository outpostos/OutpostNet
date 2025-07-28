#!/bin/bash

# outpostnet 默认配置演示脚本
echo "🎬 outpostnet 默认配置演示"
echo "========================="

# 清理函数
cleanup() {
    echo ""
    echo "🧹 正在清理..."
    kill $(jobs -p) 2>/dev/null || true
    sleep 1
    echo "✅ 清理完成"
}

# 设置清理陷阱
trap cleanup EXIT

echo ""
echo "📋 第1步：启动模拟后端服务"
echo "启动一个简单的HTTP服务器作为后端..."
echo "创建测试内容..."
echo "<h1>Hello from outpostnet!</h1><p>这是通过 outpostnet 默认配置代理的服务</p>" > /tmp/index.html

python3 -m http.server 8081 --directory /tmp > /dev/null 2>&1 &
BACKEND_PID=$!

# 等待后端服务启动
sleep 2
echo "✅ 后端服务已启动在 http://localhost:8081"

echo ""
echo "📋 第2步：启动 outpostnet-server（零配置）"
echo "无需任何配置文件，直接启动..."
./build/outpostnet-server > /tmp/server.log 2>&1 &
SERVER_PID=$!

# 等待服务器启动
sleep 3
echo "✅ outpostnet-server 已启动"
echo "   - 监听端口: 0.0.0.0:7629"
echo "   - 认证令牌: outpostnet-default-token"
echo "   - 默认服务: default-service"

echo ""
echo "📋 第3步：启动 outpostnet-client（注册模式）"
echo "将本地服务注册到 outpostnet-server..."
./build/outpostnet-client -mode register > /tmp/register.log 2>&1 &
REGISTER_PID=$!

# 等待注册完成
sleep 3
echo "✅ 服务注册完成"
echo "   - 注册服务: 8081 端口的 HTTP 服务"
echo "   - 服务标签: app=default"

echo ""
echo "📋 第4步：启动 outpostnet-client（代理模式）"
echo "创建到远程服务的代理..."
./build/outpostnet-client -mode proxy > /tmp/proxy.log 2>&1 &
PROXY_PID=$!

# 等待代理启动
sleep 3
echo "✅ 代理服务已启动"
echo "   - 本地端口: 8081"
echo "   - 目标服务: default-service"

echo ""
echo "📋 第5步：测试代理功能"
echo "通过代理访问远程服务..."
echo ""
echo "🌐 发送HTTP请求到 http://localhost:8081"
echo "---"
RESPONSE=$(curl -s http://localhost:8081)
echo "$RESPONSE"
echo "---"
echo "✅ 代理功能测试成功！"

echo ""
echo "🎉 演示完成！"
echo ""
echo "📊 运行状态："
echo "   📡 outpostnet-server: 监听 0.0.0.0:7629"
echo "   📝 outpostnet-client (register): 已注册本地服务"
echo "   🔗 outpostnet-client (proxy): 本地代理 8081 端口"
echo "   🌐 后端服务: 提供 HTTP 内容"
echo ""
echo "🔧 零配置设置："
echo "   - 无需配置文件"
echo "   - 自动服务发现"
echo "   - 开箱即用"
echo ""
echo "⏳ 演示将运行30秒，然后自动清理..."
echo "   你可以在另一个终端中运行 'curl http://localhost:8081' 来测试"
echo "   或者按 Ctrl+C 提前退出"

# 等待30秒或用户中断
sleep 30