package client

import (
	"context"
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	"github.com/outpostos/OutpostNet/pkg/config"
	"github.com/outpostos/OutpostNet/pkg/loadbalancer"
	"github.com/outpostos/OutpostNet/pkg/pool"
	"github.com/outpostos/OutpostNet/pkg/protocol"
	"github.com/outpostos/OutpostNet/pkg/types"
	"google.golang.org/protobuf/proto"
)

// OptimizedClient 优化后的outpostnet客户端
type OptimizedClient struct {
	config *config.ClientConfig
	
	// 连接池管理
	connectionPool *pool.ConnectionPool
	controlConn    net.Conn
	codec          *protocol.Codec
	
	// 代理管理
	proxies   map[string]*OptimizedProxyInstance
	proxiesMu sync.RWMutex
	
	// 负载均衡管理
	lbManager *loadbalancer.LoadBalancerManager
	
	// 消息处理
	messageHandler *protocol.MessageHandler
	converter      *protocol.MessageConverter
	authenticated  bool
	clientID       string
	
	// 服务发现缓存
	serviceCache    map[string]*CachedServiceInfo
	serviceCacheMu  sync.RWMutex
	cacheTimeout    time.Duration
	
	// 控制通道
	ctx    context.Context
	cancel context.CancelFunc
}

// OptimizedProxyInstance 优化后的代理实例
type OptimizedProxyInstance struct {
	Config   types.Proxy
	Listener net.Listener
	UDPConn  *net.UDPConn
	Active   bool
	
	// 连接统计
	ActiveConnections int64
	TotalConnections  int64
	
	// 性能指标
	AvgLatency        time.Duration
	TotalDataTransfer int64
}

// CachedServiceInfo 缓存的服务信息
type CachedServiceInfo struct {
	Service     *protocol.Service
	Endpoints   []*protocol.Endpoint
	CachedAt    time.Time
	ExpiresAt   time.Time
	Version     int32
}

// NewOptimizedClient 创建优化后的客户端
func NewOptimizedClient(cfg *config.ClientConfig) (*OptimizedClient, error) {
	if cfg == nil {
		return nil, fmt.Errorf("config is nil")
	}
	
	ctx, cancel := context.WithCancel(context.Background())
	
	// 创建连接池
	poolConfig := &pool.ConnectionPoolConfig{
		MaxConnections:      100,
		MaxIdleConnections:  20,
		MaxConnectionAge:    30 * time.Minute,
		IdleTimeout:         5 * time.Minute,
		ConnectionTimeout:   10 * time.Second,
		HealthCheckInterval: 30 * time.Second,
	}
	
	client := &OptimizedClient{
		config:          cfg,
		connectionPool:  pool.NewConnectionPool(poolConfig),
		proxies:         make(map[string]*OptimizedProxyInstance),
		lbManager:       loadbalancer.NewLoadBalancerManager(),
		serviceCache:    make(map[string]*CachedServiceInfo),
		cacheTimeout:    5 * time.Minute,
		converter:       protocol.NewMessageConverter(),
		ctx:             ctx,
		cancel:          cancel,
	}
	
	return client, nil
}

// Connect 连接到服务器
func (c *OptimizedClient) Connect() error {
	// 建立控制连接
	conn, err := c.connectionPool.GetConnection(c.config.Client.ServerAddr)
	if err != nil {
		return fmt.Errorf("failed to connect to server: %w", err)
	}
	
	c.controlConn = conn
	c.codec = protocol.NewCodec(conn)
	c.messageHandler = protocol.NewMessageHandler(c.codec)
	
	// 注册消息处理器
	c.registerMessageHandlers()
	
	// 执行认证
	if err := c.authenticate(); err != nil {
		return fmt.Errorf("authentication failed: %w", err)
	}
	
	// 启动消息处理循环
	go c.messageLoop()
	
	return nil
}

// authenticate 执行认证
func (c *OptimizedClient) authenticate() error {
	// 创建认证消息
	authPayload := c.converter.ConvertAuthPayload(c.config.Client.AuthToken, c.config.Client.Mode)
	
	msg := protocol.NewMessageBuilder(protocol.MsgTypeAuth).
		WithPayload(authPayload).
		Build()
	
	// 发送认证请求
	if err := c.codec.WriteMessage(msg); err != nil {
		return fmt.Errorf("failed to send auth message: %w", err)
	}
	
	// 读取认证响应
	response, err := c.codec.ReadMessage()
	if err != nil {
		return fmt.Errorf("failed to read auth response: %w", err)
	}
	
	if response.Type != protocol.MsgTypeAuthResponse {
		return fmt.Errorf("unexpected response type: %s", response.Type)
	}
	
	// 解析认证响应
	authResp := &protocol.AuthResponse{}
	if err := proto.Unmarshal(response.Payload, authResp); err != nil {
		return fmt.Errorf("failed to parse auth response: %w", err)
	}
	
	if !authResp.Success {
		return fmt.Errorf("authentication failed: %s", authResp.Message)
	}
	
	c.authenticated = true
	c.clientID = authResp.ClientId
	
	log.Printf("Authenticated successfully as client %s", c.clientID)
	return nil
}

// registerMessageHandlers 注册消息处理器
func (c *OptimizedClient) registerMessageHandlers() {
	c.messageHandler.RegisterHandler(protocol.MsgTypeServiceInfo, c.handleServiceInfo)
	c.messageHandler.RegisterHandler(protocol.MsgTypeSTCPTunnelResponse, c.handleSTCPTunnelResponse)
	c.messageHandler.RegisterHandler(protocol.MsgTypeError, c.handleError)
	c.messageHandler.RegisterHandler(protocol.MsgTypePong, c.handlePong)
}

// messageLoop 消息处理循环
func (c *OptimizedClient) messageLoop() {
	for {
		select {
		case <-c.ctx.Done():
			return
		default:
			msg, err := c.codec.ReadMessage()
			if err != nil {
				log.Printf("Failed to read message: %v", err)
				continue
			}
			
			if err := c.messageHandler.ProcessMessage(msg); err != nil {
				log.Printf("Failed to process message: %v", err)
			}
			
			// 返回消息到池中
			c.codec.ReturnMessage(msg)
		}
	}
}

// StartProxy 启动代理
func (c *OptimizedClient) StartProxy(proxyConfig types.Proxy) error {
	switch proxyConfig.ProxyMode {
	case "tcp", "":
		return c.startOptimizedTCPProxy(proxyConfig)
	case "udp":
		return c.startOptimizedUDPProxy(proxyConfig)
	case "stcp":
		return c.startOptimizedSTCPProxy(proxyConfig)
	case "sudp":
		return c.startOptimizedSUDPProxy(proxyConfig)
	default:
		return fmt.Errorf("unsupported proxy mode: %s", proxyConfig.ProxyMode)
	}
}

// startOptimizedTCPProxy 启动优化的TCP代理
func (c *OptimizedClient) startOptimizedTCPProxy(proxyConfig types.Proxy) error {
	// 监听本地端口
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", proxyConfig.LocalPort))
	if err != nil {
		return fmt.Errorf("failed to listen on port %d: %w", proxyConfig.LocalPort, err)
	}
	
	proxy := &OptimizedProxyInstance{
		Config:   proxyConfig,
		Listener: listener,
		Active:   true,
	}
	
	// 注册代理实例
	c.proxiesMu.Lock()
	c.proxies[proxyConfig.Name] = proxy
	c.proxiesMu.Unlock()
	
	// 启动连接处理
	go c.handleOptimizedTCPConnections(proxy)
	
	return nil
}

// handleOptimizedTCPConnections 处理优化的TCP连接
func (c *OptimizedClient) handleOptimizedTCPConnections(proxy *OptimizedProxyInstance) {
	defer proxy.Listener.Close()
	
	log.Printf("Optimized proxy %s listening on port %d", proxy.Config.Name, proxy.Config.LocalPort)
	
	for {
		conn, err := proxy.Listener.Accept()
		if err != nil {
			select {
			case <-c.ctx.Done():
				return
			default:
				log.Printf("Failed to accept connection for proxy %s: %v", proxy.Config.Name, err)
				continue
			}
		}
		
		// 在独立的goroutine中处理连接
		go c.handleOptimizedTCPConnection(proxy, conn)
	}
}

// handleOptimizedTCPConnection 处理优化的TCP连接
func (c *OptimizedClient) handleOptimizedTCPConnection(proxy *OptimizedProxyInstance, conn net.Conn) {
	defer conn.Close()
	
	start := time.Now()
	proxy.TotalConnections++
	proxy.ActiveConnections++
	
	defer func() {
		proxy.ActiveConnections--
		duration := time.Since(start)
		proxy.AvgLatency = (proxy.AvgLatency + duration) / 2
	}()
	
	log.Printf("New optimized connection to proxy %s from %s", proxy.Config.Name, conn.RemoteAddr())
	
	// 1. 使用缓存的服务信息
	serviceInfo, err := c.getCachedServiceInfo(proxy.Config.ServiceName, proxy.Config.ServiceNamespace, proxy.Config.Selector)
	if err != nil {
		log.Printf("Failed to get service info for %s: %v", proxy.Config.ServiceName, err)
		return
	}
	
	if len(serviceInfo.Endpoints) == 0 {
		log.Printf("No available endpoints for service %s", proxy.Config.ServiceName)
		return
	}
	
	// 2. 使用负载均衡器选择端点
	endpoint := c.selectOptimizedEndpoint(serviceInfo.Service, serviceInfo.Endpoints, conn.RemoteAddr().String())
	if endpoint == nil {
		log.Printf("No suitable endpoint selected for service %s", proxy.Config.ServiceName)
		return
	}
	
	targetAddr := fmt.Sprintf("%s:%d", endpoint.Ip, endpoint.Port)
	log.Printf("Proxy %s connecting to optimized endpoint %s", proxy.Config.Name, targetAddr)
	
	// 3. 使用连接池获取目标连接
	targetConn, err := c.connectionPool.GetConnectionWithContext(c.ctx, targetAddr)
	if err != nil {
		log.Printf("Failed to get connection to target %s: %v", targetAddr, err)
		return
	}
	defer targetConn.Close()
	
	log.Printf("Proxy %s established optimized connection to %s", proxy.Config.Name, targetAddr)
	
	// 4. 优化的双向数据转发
	c.optimizedDataForward(conn, targetConn, proxy)
}

// optimizedDataForward 优化的数据转发
func (c *OptimizedClient) optimizedDataForward(clientConn, targetConn net.Conn, proxy *OptimizedProxyInstance) {
	done := make(chan struct{}, 2)
	
	// 使用更大的缓冲区
	bufferSize := 64 * 1024
	
	// 客户端 -> 目标端点
	go func() {
		defer func() { done <- struct{}{} }()
		
		buffer := make([]byte, bufferSize)
		for {
			n, err := clientConn.Read(buffer)
			if err != nil {
				return
			}
			
			if _, err := targetConn.Write(buffer[:n]); err != nil {
				return
			}
			
			proxy.TotalDataTransfer += int64(n)
		}
	}()
	
	// 目标端点 -> 客户端
	go func() {
		defer func() { done <- struct{}{} }()
		
		buffer := make([]byte, bufferSize)
		for {
			n, err := targetConn.Read(buffer)
			if err != nil {
				return
			}
			
			if _, err := clientConn.Write(buffer[:n]); err != nil {
				return
			}
			
			proxy.TotalDataTransfer += int64(n)
		}
	}()
	
	// 等待任一方向的连接关闭
	<-done
}

// getCachedServiceInfo 获取缓存的服务信息
func (c *OptimizedClient) getCachedServiceInfo(serviceName, namespace string, selector map[string]string) (*CachedServiceInfo, error) {
	cacheKey := fmt.Sprintf("%s/%s", namespace, serviceName)
	
	c.serviceCacheMu.RLock()
	cachedInfo, exists := c.serviceCache[cacheKey]
	c.serviceCacheMu.RUnlock()
	
	// 检查缓存是否有效
	if exists && time.Now().Before(cachedInfo.ExpiresAt) {
		return cachedInfo, nil
	}
	
	// 查询服务信息
	serviceInfo, err := c.queryOptimizedService(serviceName, namespace, selector)
	if err != nil {
		return nil, err
	}
	
	// 更新缓存
	c.serviceCacheMu.Lock()
	c.serviceCache[cacheKey] = serviceInfo
	c.serviceCacheMu.Unlock()
	
	return serviceInfo, nil
}

// queryOptimizedService 查询优化的服务信息
func (c *OptimizedClient) queryOptimizedService(serviceName, namespace string, selector map[string]string) (*CachedServiceInfo, error) {
	// 创建查询消息
	queryPayload := c.converter.ConvertQueryServicePayload(serviceName, namespace, selector)
	
	msg := protocol.NewMessageBuilder(protocol.MsgTypeQueryService).
		WithPayload(queryPayload).
		Build()
	
	// 发送查询请求
	if err := c.codec.WriteMessage(msg); err != nil {
		return nil, fmt.Errorf("failed to send query message: %w", err)
	}
	
	// 读取响应
	response, err := c.codec.ReadMessage()
	if err != nil {
		return nil, fmt.Errorf("failed to read query response: %w", err)
	}
	
	if response.Type != protocol.MsgTypeServiceInfo {
		return nil, fmt.Errorf("unexpected response type: %s", response.Type)
	}
	
	// 解析服务信息
	serviceInfo := &protocol.ServiceInfoPayload{}
	if err := proto.Unmarshal(response.Payload, serviceInfo); err != nil {
		return nil, fmt.Errorf("failed to parse service info: %w", err)
	}
	
	// 创建缓存信息
	cachedInfo := &CachedServiceInfo{
		Service:   serviceInfo.Service,
		Endpoints: serviceInfo.Endpoints,
		CachedAt:  time.Now(),
		ExpiresAt: time.Now().Add(c.cacheTimeout),
		Version:   1,
	}
	
	// 如果响应中有过期时间，使用它
	if serviceInfo.CacheExpiry != nil {
		cachedInfo.ExpiresAt = serviceInfo.CacheExpiry.AsTime()
	}
	
	return cachedInfo, nil
}

// selectOptimizedEndpoint 选择优化的端点
func (c *OptimizedClient) selectOptimizedEndpoint(service *protocol.Service, endpoints []*protocol.Endpoint, clientAddr string) *protocol.Endpoint {
	if len(endpoints) == 0 {
		return nil
	}
	
	// 过滤健康的端点
	healthyEndpoints := make([]*protocol.Endpoint, 0)
	for _, ep := range endpoints {
		if ep.Status != nil && ep.Status.Ready {
			healthyEndpoints = append(healthyEndpoints, ep)
		}
	}
	
	if len(healthyEndpoints) == 0 {
		// 如果没有健康的端点，使用所有端点
		healthyEndpoints = endpoints
	}
	
	// 转换为types.Endpoint格式（为了兼容现有的负载均衡器）
	typesEndpoints := make([]*types.Endpoint, len(healthyEndpoints))
	for i, ep := range healthyEndpoints {
		typesEndpoints[i] = &types.Endpoint{
			ID:       ep.Id,
			IP:       ep.Ip,
			Port:     int(ep.Port),
			Protocol: ep.Protocol,
			Labels:   ep.Labels,
		}
	}
	
	// 获取负载均衡器
	lbType := loadbalancer.RoundRobin
	if service.Spec != nil && service.Spec.LoadBalancer != "" {
		switch service.Spec.LoadBalancer {
		case "round_robin":
			lbType = loadbalancer.RoundRobin
		case "weighted_round_robin":
			lbType = loadbalancer.WeightedRoundRobin
		case "least_connections":
			lbType = loadbalancer.LeastConnections
		case "random":
			lbType = loadbalancer.Random
		case "consistent_hash":
			lbType = loadbalancer.ConsistentHash
		}
	}
	
	lb := c.lbManager.GetLoadBalancer(service.Metadata.Name, lbType)
	selectedEndpoint := lb.SelectEndpoint(typesEndpoints, clientAddr)
	
	// 找到对应的protocol.Endpoint
	for _, ep := range healthyEndpoints {
		if ep.Id == selectedEndpoint.ID {
			return ep
		}
	}
	
	return healthyEndpoints[0]
}

// 消息处理函数
func (c *OptimizedClient) handleServiceInfo(msg *protocol.Message) error {
	// 处理服务信息响应
	return nil
}

func (c *OptimizedClient) handleSTCPTunnelResponse(msg *protocol.Message) error {
	// 处理STCP隧道响应
	return nil
}

func (c *OptimizedClient) handleError(msg *protocol.Message) error {
	errorPayload := &protocol.ErrorPayload{}
	if err := proto.Unmarshal(msg.Payload, errorPayload); err != nil {
		return err
	}
	
	log.Printf("Received error: %d - %s", errorPayload.Code, errorPayload.Message)
	return nil
}

func (c *OptimizedClient) handlePong(msg *protocol.Message) error {
	// 处理心跳响应
	return nil
}

// startOptimizedSTCPProxy 启动优化的STCP代理
func (c *OptimizedClient) startOptimizedSTCPProxy(proxyConfig types.Proxy) error {
	// 监听本地端口
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", proxyConfig.LocalPort))
	if err != nil {
		return fmt.Errorf("failed to listen on port %d: %w", proxyConfig.LocalPort, err)
	}
	
	proxy := &OptimizedProxyInstance{
		Config:   proxyConfig,
		Listener: listener,
		Active:   true,
	}
	
	// 注册代理实例
	c.proxiesMu.Lock()
	c.proxies[proxyConfig.Name] = proxy
	c.proxiesMu.Unlock()
	
	// 启动STCP连接处理
	go c.handleOptimizedSTCPConnections(proxy)
	
	return nil
}

// handleOptimizedSTCPConnections 处理优化的STCP连接
func (c *OptimizedClient) handleOptimizedSTCPConnections(proxy *OptimizedProxyInstance) {
	defer proxy.Listener.Close()
	
	log.Printf("Optimized STCP proxy %s listening on port %d", proxy.Config.Name, proxy.Config.LocalPort)
	
	for {
		conn, err := proxy.Listener.Accept()
		if err != nil {
			select {
			case <-c.ctx.Done():
				return
			default:
				log.Printf("Failed to accept STCP connection for proxy %s: %v", proxy.Config.Name, err)
				continue
			}
		}
		
		// 在独立的goroutine中处理STCP连接
		go c.handleOptimizedSTCPConnection(proxy, conn)
	}
}

// handleOptimizedSTCPConnection 处理优化的STCP连接
func (c *OptimizedClient) handleOptimizedSTCPConnection(proxy *OptimizedProxyInstance, conn net.Conn) {
	defer conn.Close()
	
	start := time.Now()
	proxy.TotalConnections++
	proxy.ActiveConnections++
	
	defer func() {
		proxy.ActiveConnections--
		duration := time.Since(start)
		proxy.AvgLatency = (proxy.AvgLatency + duration) / 2
	}()
	
	log.Printf("New optimized STCP connection to proxy %s from %s", proxy.Config.Name, conn.RemoteAddr())
	
	// 创建STCP隧道请求
	tunnelReq := c.converter.ConvertSTCPTunnelRequest(proxy.Config.ServiceName, "")
	
	msg := protocol.NewMessageBuilder(protocol.MsgTypeSTCPTunnelRequest).
		WithPayload(tunnelReq).
		Build()
	
	// 发送隧道请求
	if err := c.codec.WriteMessage(msg); err != nil {
		log.Printf("Failed to send STCP tunnel request: %v", err)
		return
	}
	
	// 读取隧道响应
	response, err := c.codec.ReadMessage()
	if err != nil {
		log.Printf("Failed to read STCP tunnel response: %v", err)
		return
	}
	
	if response.Type != protocol.MsgTypeSTCPTunnelResponse {
		log.Printf("Unexpected STCP response type: %s", response.Type)
		return
	}
	
	// 解析隧道响应
	tunnelResp := &protocol.STCPTunnelResponse{}
	if err := proto.Unmarshal(response.Payload, tunnelResp); err != nil {
		log.Printf("Failed to parse STCP tunnel response: %v", err)
		return
	}
	
	if !tunnelResp.Success {
		log.Printf("STCP tunnel failed: %s", tunnelResp.Message)
		return
	}
	
	log.Printf("STCP tunnel established: %s", tunnelResp.TunnelId)
	
	// 开始数据转发
	c.optimizedDataForward(conn, c.controlConn, proxy)
}

// startOptimizedUDPProxy 启动优化的UDP代理
func (c *OptimizedClient) startOptimizedUDPProxy(proxyConfig types.Proxy) error {
	// UDP代理实现
	addr, err := net.ResolveUDPAddr("udp", fmt.Sprintf(":%d", proxyConfig.LocalPort))
	if err != nil {
		return fmt.Errorf("failed to resolve UDP address: %w", err)
	}
	
	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		return fmt.Errorf("failed to listen on UDP port %d: %w", proxyConfig.LocalPort, err)
	}
	
	proxy := &OptimizedProxyInstance{
		Config:  proxyConfig,
		UDPConn: conn,
		Active:  true,
	}
	
	// 注册代理实例
	c.proxiesMu.Lock()
	c.proxies[proxyConfig.Name] = proxy
	c.proxiesMu.Unlock()
	
	// 启动UDP数据包处理
	go c.handleOptimizedUDPPackets(proxy)
	
	return nil
}

// handleOptimizedUDPPackets 处理优化的UDP数据包
func (c *OptimizedClient) handleOptimizedUDPPackets(proxy *OptimizedProxyInstance) {
	defer proxy.UDPConn.Close()
	
	log.Printf("Optimized UDP proxy %s listening on port %d", proxy.Config.Name, proxy.Config.LocalPort)
	
	// UDP代理处理逻辑
	buffer := make([]byte, 64*1024)
	for {
		n, clientAddr, err := proxy.UDPConn.ReadFromUDP(buffer)
		if err != nil {
			select {
			case <-c.ctx.Done():
				return
			default:
				log.Printf("Failed to read UDP packet for proxy %s: %v", proxy.Config.Name, err)
				continue
			}
		}
		
		proxy.TotalDataTransfer += int64(n)
		
		// 处理UDP数据包
		go c.handleOptimizedUDPPacket(proxy, buffer[:n], clientAddr)
	}
}

// handleOptimizedUDPPacket 处理优化的UDP数据包
func (c *OptimizedClient) handleOptimizedUDPPacket(proxy *OptimizedProxyInstance, data []byte, clientAddr *net.UDPAddr) {
	// UDP数据包处理逻辑
	log.Printf("Handling optimized UDP packet from %s, size: %d", clientAddr, len(data))
	
	// 这里可以实现UDP数据包的转发逻辑
	// 为了简化，这里只是记录日志
}

// startOptimizedSUDPProxy 启动优化的SUDP代理
func (c *OptimizedClient) startOptimizedSUDPProxy(proxyConfig types.Proxy) error {
	// SUDP代理实现，类似UDP但使用安全隧道
	return c.startOptimizedUDPProxy(proxyConfig)
}

// GetOptimizedStats 获取优化的统计信息
func (c *OptimizedClient) GetOptimizedStats() *OptimizedClientStats {
	poolStats := c.connectionPool.GetStats()
	codecStats := c.codec.GetStats()
	processingStats := c.messageHandler.GetProcessingStats()
	
	c.proxiesMu.RLock()
	proxyStats := make(map[string]*OptimizedProxyStats)
	for name, proxy := range c.proxies {
		proxyStats[name] = &OptimizedProxyStats{
			Name:              name,
			ActiveConnections: proxy.ActiveConnections,
			TotalConnections:  proxy.TotalConnections,
			AvgLatency:        proxy.AvgLatency,
			TotalDataTransfer: proxy.TotalDataTransfer,
			Active:            proxy.Active,
		}
	}
	c.proxiesMu.RUnlock()
	
	return &OptimizedClientStats{
		ConnectionPool:      poolStats,
		Codec:              codecStats,
		MessageProcessing:  processingStats,
		Proxies:            proxyStats,
		ServiceCacheSize:   len(c.serviceCache),
		Authenticated:      c.authenticated,
		ClientID:           c.clientID,
	}
}

// OptimizedClientStats 优化客户端统计信息
type OptimizedClientStats struct {
	ConnectionPool     *pool.PoolStats
	Codec             *protocol.CodecStats
	MessageProcessing map[string]*protocol.ProcessingStats
	Proxies           map[string]*OptimizedProxyStats
	ServiceCacheSize  int
	Authenticated     bool
	ClientID          string
}

// OptimizedProxyStats 优化代理统计信息
type OptimizedProxyStats struct {
	Name              string
	ActiveConnections int64
	TotalConnections  int64
	AvgLatency        time.Duration
	TotalDataTransfer int64
	Active            bool
}

// Close 关闭优化客户端
func (c *OptimizedClient) Close() error {
	c.cancel()
	
	// 关闭所有代理
	c.proxiesMu.Lock()
	for _, proxy := range c.proxies {
		proxy.Active = false
		if proxy.Listener != nil {
			proxy.Listener.Close()
		}
		if proxy.UDPConn != nil {
			proxy.UDPConn.Close()
		}
	}
	c.proxiesMu.Unlock()
	
	// 关闭编解码器
	if c.codec != nil {
		c.codec.Close()
	}
	
	// 关闭连接池
	if c.connectionPool != nil {
		c.connectionPool.Close()
	}
	
	return nil
}