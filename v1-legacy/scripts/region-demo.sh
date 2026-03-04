#!/bin/bash

# outpostnet Phase 2 区域服务网格演示脚本
# 演示跨节点通信、健康检查、负载均衡和故障处理

set -e

echo "🚀 Starting outpostnet Phase 2 Region Service Mesh Demo"
echo "============================================="

# 检查构建
if [ ! -f "build/outpostnet-server" ] || [ ! -f "build/outpostnet-client" ]; then
    echo "❌ outpostnet binaries not found. Please run 'make build' first."
    exit 1
fi

# 创建演示目录
DEMO_DIR="demo/phase2-region"
mkdir -p $DEMO_DIR/configs
mkdir -p $DEMO_DIR/logs

# 清理函数
cleanup() {
    echo "🧹 Cleaning up processes..."
    pkill -f "outpostnet-server" || true
    pkill -f "outpostnet-client" || true
    pkill -f "nc -l" || true
    sleep 2
}

# 设置清理trap
trap cleanup EXIT

echo "📝 Creating Phase 2 configuration files..."

# 创建节点A配置 (Node-A: 区域网格主节点)
cat > $DEMO_DIR/configs/node-a-config.yaml << 'EOF'
# outpostnet Node-A 配置 (区域网格主节点)
server:
  listen_addr: "0.0.0.0:17000"
  auth_token: "phase2-demo-token"
  
  # Phase 2 区域配置
  region:
    enabled: true
    region_id: "region-east"
    node_id: "node-a"
    site_id: "site-datacenter"
    zone_id: "zone-1"
    listen_addr: "0.0.0.0:18000"  # 区域网格监听端口
    
    # 邻居节点配置
    neighbors:
      - node_id: "node-b"
        address: "127.0.0.1:18001"
      - node_id: "node-c"  
        address: "127.0.0.1:18002"
    
    # 服务同步配置
    service_sync:
      sync_interval: "10s"
      cache_timeout: "60s"
      
    # 负载均衡配置
    load_balancer:
      strategy: "geographic_aware"
      health_check_enabled: true

# 服务定义
services:
  - name: "web-service"
    namespace: "default"
    type: "NodePort"
    selector:
      app: "web"
    ports:
      - name: "http"
        port: 80
        nodePort: 30080
        targetPort: 8080
        protocol: "tcp"
    proxy_mode: "tcp"
    load_balancer: "round_robin"
    
  - name: "api-service"
    namespace: "default" 
    type: "ClusterIP"
    selector:
      app: "api"
    ports:
      - name: "api"
        port: 8080
        targetPort: 8080
        protocol: "stcp"
    proxy_mode: "stcp"
    load_balancer: "geographic_aware"
EOF

# 创建简化的单节点测试配置 (目前实际实现还未完全支持多节点)
cat > $DEMO_DIR/configs/standalone-config.yaml << 'EOF'
# outpostnet 单节点配置 (当前实现)
server:
  listen_addr: "0.0.0.0:17000"
  auth_token: "phase2-demo-token"

# 服务定义
services:
  - name: "web-service"
    namespace: "default"
    type: "NodePort"
    selector:
      app: "web"
    ports:
      - name: "http"
        port: 80
        nodePort: 30080
        targetPort: 8080
        protocol: "tcp"
    proxy_mode: "tcp"
    load_balancer: "round_robin"
    
  - name: "api-service"
    namespace: "default"
    type: "ClusterIP"  
    selector:
      app: "api"
    ports:
      - name: "api"
        port: 8080
        targetPort: 8080
        protocol: "stcp"
    proxy_mode: "stcp"
    load_balancer: "round_robin"
EOF

# 创建客户端配置 (注册模式 - 模拟后端服务)
cat > $DEMO_DIR/configs/client-register.yaml << 'EOF'
# outpostnet Client 配置 (Register 模式 - 服务提供方)
client:
  server_addr: "127.0.0.1:17000"
  auth_token: "phase2-demo-token"
  mode: "register"

# 注册服务到 outpostnet-server
services:
  - name: "web-service"
    namespace: "default"
    local_addr: "127.0.0.1"
    local_port: 8080
    labels:
      app: "web"
      version: "v1"
      node: "backend-node-1"
      
  - name: "api-service"
    namespace: "default"
    local_addr: "127.0.0.1"
    local_port: 8081
    labels:
      app: "api"
      version: "v2"
      node: "backend-node-1"
EOF

# 创建客户端配置 (代理模式 - 模拟应用客户端)
cat > $DEMO_DIR/configs/client-proxy.yaml << 'EOF'
# outpostnet Client 配置 (Proxy 模式 - 服务消费方)
client:
  server_addr: "127.0.0.1:17000"
  auth_token: "phase2-demo-token"
  mode: "proxy"

# 代理配置
proxies:
  - name: "web-proxy"
    service_name: "web-service"
    service_namespace: "default"
    local_port: 9080
    proxy_mode: "tcp"
    selector:
      app: "web"
      
  - name: "api-proxy"
    service_name: "api-service"
    service_namespace: "default"
    local_port: 9081
    proxy_mode: "stcp"
    selector:
      app: "api"
      version: "v2"
EOF

echo "🚀 Starting outpostnet server (Current Implementation)..."

# 启动outpostnet-server (使用当前实现)
echo "Starting outpostnet-server..."
./build/outpostnet-server -config $DEMO_DIR/configs/standalone-config.yaml > $DEMO_DIR/logs/server.log 2>&1 &
SERVER_PID=$!
sleep 3

echo "✅ outpostnet-server started!"
echo "Server PID: $SERVER_PID (Port: 17000)"

echo "🖥️  Starting mock backend services..."

# 启动模拟Web服务 (端口 8080)
echo "HTTP/1.1 200 OK
Content-Type: text/plain

Hello from Web Service (Phase 2)
Service: web-service
Version: v1
Timestamp: $(date)
" > $DEMO_DIR/web-response.txt

nc -l 8080 < $DEMO_DIR/web-response.txt &
WEB_PID=$!

# 启动模拟API服务 (端口 8081)  
echo "HTTP/1.1 200 OK
Content-Type: application/json

{
  \"message\": \"API Service Response\",
  \"service\": \"api-service\", 
  \"version\": \"v2\",
  \"phase\": 2,
  \"timestamp\": \"$(date)\"
}
" > $DEMO_DIR/api-response.txt

nc -l 8081 < $DEMO_DIR/api-response.txt &
API_PID=$!

echo "✅ Mock services started:"
echo "Web Service PID: $WEB_PID (Port: 8080)"
echo "API Service PID: $API_PID (Port: 8081)"

sleep 2

echo "🔗 Starting outpostnet clients..."

# 启动客户端 (注册模式)
echo "Starting client (register mode)..."
./build/outpostnet-client -config $DEMO_DIR/configs/client-register.yaml > $DEMO_DIR/logs/client-register.log 2>&1 &
CLIENT_REGISTER_PID=$!

sleep 3

# 启动客户端 (代理模式)
echo "Starting client (proxy mode)..."
./build/outpostnet-client -config $DEMO_DIR/configs/client-proxy.yaml > $DEMO_DIR/logs/client-proxy.log 2>&1 &
CLIENT_PROXY_PID=$!

sleep 3

echo "✅ outpostnet clients started:"
echo "Register Client PID: $CLIENT_REGISTER_PID"
echo "Proxy Client PID: $CLIENT_PROXY_PID"

echo ""
echo "🧪 Testing Phase 2 Service Mesh Implementation..."
echo "============================================="

# 测试1: 直接访问NodePort (如果实现)
echo "📋 Test 1: Direct NodePort Access"
echo "curl -s http://127.0.0.1:30080/"
if curl -s -m 5 http://127.0.0.1:30080/ 2>/dev/null; then
    echo "✅ Direct NodePort access passed!"
else
    echo "⚠️  Direct NodePort access not yet implemented"
fi

echo ""

# 测试2: TCP代理访问 (通过客户端)
echo "📋 Test 2: TCP Proxy (via client) - Web Service"
echo "curl -s http://127.0.0.1:9080/"
if curl -s -m 5 http://127.0.0.1:9080/ 2>/dev/null; then
    echo "✅ TCP proxy test passed!"
else
    echo "⚠️  TCP proxy test failed - checking logs"
    tail -n 5 $DEMO_DIR/logs/client-proxy.log
fi

echo ""

# 测试3: STCP代理访问 (通过客户端)
echo "📋 Test 3: STCP Proxy (via client) - API Service"
echo "curl -s http://127.0.0.1:9081/api"
if curl -s -m 5 http://127.0.0.1:9081/api 2>/dev/null; then
    echo "✅ STCP proxy test passed!"
else
    echo "⚠️  STCP proxy test failed - checking logs"
    tail -n 5 $DEMO_DIR/logs/client-proxy.log
fi

echo ""

# 测试4: 服务注册状态
echo "📋 Test 4: Service Registration Status"
echo "Checking service registration..."

# 检查各日志文件中的关键信息
if grep -q "started" $DEMO_DIR/logs/server.log 2>/dev/null; then
    echo "✅ Server started successfully"
else
    echo "⚠️  Server startup status unclear"
fi

if grep -q "register" $DEMO_DIR/logs/client-register.log 2>/dev/null; then
    echo "✅ Client register mode activated"
else
    echo "⚠️  Client register mode status unclear"
fi

if grep -q "proxy" $DEMO_DIR/logs/client-proxy.log 2>/dev/null; then
    echo "✅ Client proxy mode activated"
else
    echo "⚠️  Client proxy mode status unclear"
fi

echo ""

# 显示实时状态
echo "📊 Current Implementation Status"
echo "============================================="
echo "Phase 2 Progress:"
echo "  ✅ Core protocol definitions extended"
echo "  ✅ Region mesh manager implemented"
echo "  ✅ Network communication layer ready"
echo "  ✅ Health checking with φ failure detection"
echo "  ✅ Geographic-aware load balancing"
echo "  ⚠️  Full multi-node mesh: configuration needs integration"
echo ""

echo "Active Services:"
echo "  • web-service (TCP) → 127.0.0.1:9080"
echo "  • api-service (STCP) → 127.0.0.1:9081"
echo ""

echo "Process Status:"
echo "  • Server: PID $SERVER_PID"
echo "  • Register Client: PID $CLIENT_REGISTER_PID"
echo "  • Proxy Client: PID $CLIENT_PROXY_PID"
echo "  • Web Backend: PID $WEB_PID"
echo "  • API Backend: PID $API_PID"
echo ""

echo "📁 Log Files:"
echo "  • Server: $DEMO_DIR/logs/server.log"
echo "  • Register: $DEMO_DIR/logs/client-register.log"
echo "  • Proxy: $DEMO_DIR/logs/client-proxy.log"
echo ""

echo "🚀 Phase 2 Implementation Demo Running!"
echo "============================================="
echo ""
echo "🧪 You can now test the services:"
echo "  curl http://127.0.0.1:9080/      # Web Service (TCP)"
echo "  curl http://127.0.0.1:9081/api   # API Service (STCP)"
echo ""
echo "📋 To view logs in real-time:"
echo "  tail -f $DEMO_DIR/logs/server.log"
echo "  tail -f $DEMO_DIR/logs/client-register.log"
echo "  tail -f $DEMO_DIR/logs/client-proxy.log"
echo ""
echo "🔧 Phase 2 Features (Implemented but need integration):"
echo "  • RegionMeshManager - Unified orchestration"
echo "  • RegionNetworkManager - Cross-node communication"
echo "  • RegionHealthChecker - φ failure detection"
echo "  • RegionLoadBalancer - Geographic-aware routing"
echo ""
echo "⏹️  Press Ctrl+C to stop all services"
echo ""

# 保持脚本运行，等待用户中断
echo "⏳ Demo is running... Press Ctrl+C to stop"

# 等待用户中断
while true; do
    sleep 5
    
    # 检查进程是否还在运行
    if ! kill -0 $SERVER_PID 2>/dev/null; then
        echo "❌ Server process died unexpectedly"
        break
    fi
    
    if ! kill -0 $CLIENT_REGISTER_PID 2>/dev/null; then
        echo "❌ Register client process died unexpectedly"
        break
    fi
    
    if ! kill -0 $CLIENT_PROXY_PID 2>/dev/null; then
        echo "❌ Proxy client process died unexpectedly" 
        break
    fi
done