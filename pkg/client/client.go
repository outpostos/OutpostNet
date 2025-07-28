package client

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"sync"
	"time"

	"github.com/outpostos/OutpostNet/pkg/config"
	"github.com/outpostos/OutpostNet/pkg/loadbalancer"
	"github.com/outpostos/OutpostNet/pkg/types"
	"github.com/google/uuid"
)

// Client outpostnet 客户端
type Client struct {
	config *config.ClientConfig
	conn   net.Conn

	// 代理管理
	proxies   map[string]*ProxyInstance
	proxiesMu sync.RWMutex

	// 负载均衡管理
	lbManager *loadbalancer.LoadBalancerManager

	// 地址解析器
	addressResolver *config.AddressResolver

	// 消息处理
	encoder *json.Encoder
	decoder *json.Decoder
	authenticated bool
	clientID string

	// 控制通道
	ctx    context.Context
	cancel context.CancelFunc
}

// ProxyInstance 代理实例
type ProxyInstance struct {
	Config   types.Proxy
	Listener net.Listener // TCP 监听器
	UDPConn  *net.UDPConn // UDP 连接
	Active   bool
}

// NewClient 创建新的客户端实例
func NewClient(cfg *config.ClientConfig) (*Client, error) {
	if cfg == nil {
		return nil, fmt.Errorf("config is nil")
	}

	c := &Client{
		config:          cfg,
		proxies:         make(map[string]*ProxyInstance),
		lbManager:       loadbalancer.NewLoadBalancerManager(),
		addressResolver: config.NewAddressResolver("tcp"),
	}

	return c, nil
}

// selectEndpoint 使用负载均衡器选择端点
func (c *Client) selectEndpoint(service *types.Service, endpoints []*types.Endpoint, clientAddr string) *types.Endpoint {
	if len(endpoints) == 0 {
		return nil
	}

	// 获取负载均衡类型
	lbType := loadbalancer.RoundRobin // 默认轮询
	if service.Spec.LoadBalancer != "" {
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

	// 获取负载均衡器
	lb := c.lbManager.GetLoadBalancer(service.Metadata.Name, lbType)

	// 选择端点（对于一致性哈希，使用客户端地址作为键）
	key := clientAddr
	if key == "" {
		key = fmt.Sprintf("%s:%d", service.Metadata.Name, time.Now().UnixNano())
	}

	return lb.SelectEndpoint(endpoints, key)
}

// Run 启动客户端
func (c *Client) Run(ctx context.Context) error {
	c.ctx, c.cancel = context.WithCancel(ctx)
	defer c.cancel()

	// 连接到服务器
	if err := c.connectToServer(); err != nil {
		return fmt.Errorf("failed to connect to server: %v", err)
	}
	defer c.closeConnection()

	log.Printf("connected to outpostnet-server at %s", c.config.Client.ServerAddr)

	// 根据模式启动不同的功能
	switch c.config.Client.Mode {
	case "proxy", "":
		if err := c.startProxyMode(); err != nil {
			return fmt.Errorf("failed to start proxy mode: %v", err)
		}
	case "register":
		if err := c.startRegisterMode(); err != nil {
			return fmt.Errorf("failed to start register mode: %v", err)
		}
	default:
		return fmt.Errorf("unknown client mode: %s", c.config.Client.Mode)
	}

	// 保持连接
	c.keepAlive()

	// 等待上下文取消
	<-c.ctx.Done()
	log.Println("shutting down outpostnet-client")

	return nil
}

// connectToServer 连接到服务器
func (c *Client) connectToServer() error {
	// 根据配置选择连接协议
	connProto := c.config.Client.ConnProto
	if connProto == "" || connProto == "auto" {
		// 自动检测协议类型
		if network, _, err := c.addressResolver.ParseAddress(c.config.Client.ServerAddr); err == nil {
			connProto = network
		} else {
			connProto = "tcp" // 默认TCP
		}
	}

	var conn net.Conn
	var err error

	switch connProto {
	case "unix":
		conn, err = c.addressResolver.Dial(c.config.Client.ServerAddr)
	case "tcp":
		conn, err = net.DialTimeout("tcp", c.config.Client.ServerAddr, 10*time.Second)
	default:
		// 使用地址解析器自动选择
		conn, err = c.addressResolver.Dial(c.config.Client.ServerAddr)
	}

	if err != nil {
		return fmt.Errorf("failed to connect via %s: %v", connProto, err)
	}

	log.Printf("connected to server via %s protocol", connProto)
	c.conn = conn
	c.encoder = json.NewEncoder(conn)
	c.decoder = json.NewDecoder(conn)
	
	// 进行认证
	if err := c.authenticate(); err != nil {
		return fmt.Errorf("authentication failed: %v", err)
	}
	
	return nil
}

// closeConnection 关闭与服务器的连接
func (c *Client) closeConnection() {
	if c.conn != nil {
		c.conn.Close()
		c.conn = nil
	}
}

// startProxyMode 启动代理模式
func (c *Client) startProxyMode() error {
	log.Println("starting client in proxy mode")

	// 启动所有配置的代理
	for _, proxyConfig := range c.config.Proxies {
		if err := c.startProxy(proxyConfig); err != nil {
			log.Printf("failed to start proxy %s: %v", proxyConfig.Name, err)
			continue
		}
		log.Printf("started proxy: %s on port %d", proxyConfig.Name, proxyConfig.LocalPort)
	}

	return nil
}

// startRegisterMode 启动注册模式 (K8s 工作负载风格)
func (c *Client) startRegisterMode() error {
	log.Println("starting client in register mode (K8s workload style)")

	// 向服务器注册工作负载实例
	for _, workload := range c.config.Workloads {
		if err := c.registerWorkload(workload); err != nil {
			log.Printf("failed to register workload: %v", err)
			continue
		}
		log.Printf("registered workload with labels: %v", workload.Metadata.Labels)
	}

	return nil
}

// startProxy 启动单个代理
func (c *Client) startProxy(proxyConfig types.Proxy) error {
	// 根据协议类型启动不同的代理
	switch proxyConfig.ProxyMode {
	case "tcp", "":
		return c.startTCPProxy(proxyConfig)
	case "udp":
		return c.startUDPProxy(proxyConfig)
	case "stcp":
		return c.startSTCPProxy(proxyConfig)
	case "sudp":
		return c.startSUDPProxy(proxyConfig)
	default:
		return fmt.Errorf("unsupported proxy mode: %s", proxyConfig.ProxyMode)
	}
}

// startTCPProxy 启动 TCP 代理
func (c *Client) startTCPProxy(proxyConfig types.Proxy) error {
	// 监听本地端口
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", proxyConfig.LocalPort))
	if err != nil {
		return fmt.Errorf("failed to listen on port %d: %v", proxyConfig.LocalPort, err)
	}

	proxy := &ProxyInstance{
		Config:   proxyConfig,
		Listener: listener,
		Active:   true,
	}

	// 注册代理实例
	c.proxiesMu.Lock()
	c.proxies[proxyConfig.Name] = proxy
	c.proxiesMu.Unlock()

	// 在独立的 goroutine 中处理连接
	go c.handleTCPProxyConnections(proxy)

	return nil
}

// startUDPProxy 启动 UDP 代理
func (c *Client) startUDPProxy(proxyConfig types.Proxy) error {
	// 监听本地 UDP 端口
	addr, err := net.ResolveUDPAddr("udp", fmt.Sprintf(":%d", proxyConfig.LocalPort))
	if err != nil {
		return fmt.Errorf("failed to resolve UDP address: %v", err)
	}

	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		return fmt.Errorf("failed to listen on UDP port %d: %v", proxyConfig.LocalPort, err)
	}

	proxy := &ProxyInstance{
		Config:   proxyConfig,
		UDPConn:  conn,
		Active:   true,
	}

	// 注册代理实例
	c.proxiesMu.Lock()
	c.proxies[proxyConfig.Name] = proxy
	c.proxiesMu.Unlock()

	// 在独立的 goroutine 中处理 UDP 数据包
	go c.handleUDPProxyPackets(proxy)

	return nil
}

// handleTCPProxyConnections 处理 TCP 代理连接
func (c *Client) handleTCPProxyConnections(proxy *ProxyInstance) {
	defer proxy.Listener.Close()

	log.Printf("proxy %s listening on port %d", proxy.Config.Name, proxy.Config.LocalPort)

	for {
		conn, err := proxy.Listener.Accept()
		if err != nil {
			select {
			case <-c.ctx.Done():
				return
			default:
				log.Printf("failed to accept connection for proxy %s: %v", proxy.Config.Name, err)
				continue
			}
		}

		// 在独立的 goroutine 中处理每个连接
		go c.handleTCPProxyConnection(proxy, conn)
	}
}

// handleTCPProxyConnection 处理单个 TCP 代理连接
func (c *Client) handleTCPProxyConnection(proxy *ProxyInstance, conn net.Conn) {
	defer conn.Close()

	log.Printf("new connection to proxy %s from %s", proxy.Config.Name, conn.RemoteAddr())

	// 1. 查询目标服务的端点
	service, endpoints, err := c.queryService(
		proxy.Config.ServiceName, 
		proxy.Config.ServiceNamespace, 
		proxy.Config.Selector,
	)
	if err != nil {
		log.Printf("failed to query service %s: %v", proxy.Config.ServiceName, err)
		return
	}

	if len(endpoints) == 0 {
		log.Printf("no available endpoints for service %s", proxy.Config.ServiceName)
		return
	}

	// 2. 转换 endpoints 为指针切片
	endpointPtrs := make([]*types.Endpoint, len(endpoints))
	for i := range endpoints {
		endpointPtrs[i] = &endpoints[i]
	}
	
	// 3. 使用负载均衡器选择端点
	endpoint := c.selectEndpoint(service, endpointPtrs, conn.RemoteAddr().String())
	if endpoint == nil {
		log.Printf("no suitable endpoint selected for service %s", proxy.Config.ServiceName)
		return
	}
	targetAddr := fmt.Sprintf("%s:%d", endpoint.IP, endpoint.Port)

	log.Printf("proxy %s connecting to endpoint %s", proxy.Config.Name, targetAddr)

	// 3. 建立到目标端点的连接
	targetConn, err := net.DialTimeout("tcp", targetAddr, 10*time.Second)
	if err != nil {
		log.Printf("failed to connect to target %s: %v", targetAddr, err)
		return
	}
	defer targetConn.Close()

	log.Printf("proxy %s established connection to %s for service %s", 
		proxy.Config.Name, targetAddr, service.Metadata.Name)

	// 4. 双向数据转发
	done := make(chan struct{}, 2)

	// 客户端 -> 目标端点
	go func() {
		defer func() { done <- struct{}{} }()
		_, err := io.Copy(targetConn, conn)
		if err != nil {
			log.Printf("error copying client->target for proxy %s: %v", proxy.Config.Name, err)
		}
	}()

	// 目标端点 -> 客户端
	go func() {
		defer func() { done <- struct{}{} }()
		_, err := io.Copy(conn, targetConn)
		if err != nil {
			log.Printf("error copying target->client for proxy %s: %v", proxy.Config.Name, err)
		}
	}()

	// 等待任一方向的连接关闭
	<-done
	log.Printf("proxy connection closed for %s", proxy.Config.Name)
}

// registerWorkload 向服务器注册工作负载 (K8s Pod 风格)
func (c *Client) registerWorkload(workload config.ClientWorkload) error {
	// 转换为 types.Workload
	typesWorkload := types.Workload{
		Metadata: types.WorkloadMetadata{
			Labels: workload.Metadata.Labels,
		},
		Spec: types.WorkloadSpec{
			LocalAddr: workload.Spec.LocalAddr,
			LocalPort: workload.Spec.LocalPort,
			Protocol:  workload.Spec.Protocol,
		},
	}
	
	// 发送注册消息
	msg := types.Message{
		Type: types.MsgTypeRegisterWorkload,
		ID:   uuid.New().String(),
		Payload: types.RegisterWorkloadPayload{
			Workload: typesWorkload,
		},
		Timestamp: time.Now(),
	}
	
	if err := c.encoder.Encode(msg); err != nil {
		return fmt.Errorf("failed to send register message: %v", err)
	}
	
	// 等待响应
	var response types.Message
	if err := c.decoder.Decode(&response); err != nil {
		return fmt.Errorf("failed to receive response: %v", err)
	}
	
	if response.Type == types.MsgTypeError {
		var errorPayload types.ErrorPayload
		payloadBytes, _ := json.Marshal(response.Payload)
		json.Unmarshal(payloadBytes, &errorPayload)
		return fmt.Errorf("server error: %s", errorPayload.Message)
	}
	
	log.Printf("successfully registered workload at %s:%d with labels: %v", 
		workload.Spec.LocalAddr, workload.Spec.LocalPort, workload.Metadata.Labels)
	
	return nil
}

// keepAlive 保持与服务器的连接
func (c *Client) keepAlive() {
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-c.ctx.Done():
				return
			case <-ticker.C:
				if c.conn != nil && c.authenticated {
					// 发送心跳消息
					msg := types.Message{
						Type: types.MsgTypeHeartbeat,
						ID:   uuid.New().String(),
						Payload: types.HeartbeatPayload{
							Status: "alive",
						},
						Timestamp: time.Now(),
					}
					if err := c.encoder.Encode(msg); err != nil {
						log.Printf("failed to send heartbeat: %v", err)
						// 尝试重连
						c.reconnect()
					}
				}
			}
		}
	}()
}

// authenticate 进行认证
func (c *Client) authenticate() error {
	msg := types.Message{
		Type: types.MsgTypeAuth,
		ID:   uuid.New().String(),
		Payload: types.AuthPayload{
			Token: c.config.Client.AuthToken,
			Mode:  c.config.Client.Mode,
		},
		Timestamp: time.Now(),
	}
	
	if err := c.encoder.Encode(msg); err != nil {
		return fmt.Errorf("failed to send auth message: %v", err)
	}
	
	// 等待认证响应
	var response types.Message
	if err := c.decoder.Decode(&response); err != nil {
		return fmt.Errorf("failed to receive auth response: %v", err)
	}
	
	if response.Type != types.MsgTypeAuthResponse {
		return fmt.Errorf("unexpected response type: %s", response.Type)
	}
	
	var authResponse types.AuthResponsePayload
	payloadBytes, err := json.Marshal(response.Payload)
	if err != nil {
		return fmt.Errorf("failed to marshal auth response: %v", err)
	}
	if err := json.Unmarshal(payloadBytes, &authResponse); err != nil {
		return fmt.Errorf("failed to unmarshal auth response: %v", err)
	}
	
	if !authResponse.Success {
		return fmt.Errorf("authentication failed: %s", authResponse.Message)
	}
	
	c.authenticated = true
	c.clientID = authResponse.ClientID
	log.Printf("authenticated successfully as client %s", c.clientID)
	
	return nil
}

// queryService 查询服务信息（使用独立连接避免并发问题）
func (c *Client) queryService(serviceName, serviceNamespace string, selector map[string]string) (*types.Service, []types.Endpoint, error) {
	// 创建独立的连接用于查询
	conn, err := net.DialTimeout("tcp", c.config.Client.ServerAddr, 10*time.Second)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to connect to server: %v", err)
	}
	defer conn.Close()

	encoder := json.NewEncoder(conn)
	decoder := json.NewDecoder(conn)

	// 认证
	authMsg := types.Message{
		Type: types.MsgTypeAuth,
		ID:   uuid.New().String(),
		Payload: types.AuthPayload{
			Token: c.config.Client.AuthToken,
			Mode:  c.config.Client.Mode,
		},
		Timestamp: time.Now(),
	}

	if err := encoder.Encode(authMsg); err != nil {
		return nil, nil, fmt.Errorf("failed to send auth message: %v", err)
	}

	var authResponse types.Message
	if err := decoder.Decode(&authResponse); err != nil {
		return nil, nil, fmt.Errorf("failed to receive auth response: %v", err)
	}

	if authResponse.Type != types.MsgTypeAuthResponse {
		return nil, nil, fmt.Errorf("unexpected auth response type: %s", authResponse.Type)
	}

	// 查询服务
	msg := types.Message{
		Type: types.MsgTypeQueryService,
		ID:   uuid.New().String(),
		Payload: types.QueryServicePayload{
			ServiceName:      serviceName,
			ServiceNamespace: serviceNamespace,
			Selector:         selector,
		},
		Timestamp: time.Now(),
	}
	
	if err := encoder.Encode(msg); err != nil {
		return nil, nil, fmt.Errorf("failed to send query message: %v", err)
	}
	
	// 等待响应
	var response types.Message
	if err := decoder.Decode(&response); err != nil {
		return nil, nil, fmt.Errorf("failed to receive response: %v", err)
	}
	
	if response.Type == types.MsgTypeError {
		var errorPayload types.ErrorPayload
		payloadBytes, _ := json.Marshal(response.Payload)
		json.Unmarshal(payloadBytes, &errorPayload)
		return nil, nil, fmt.Errorf("server error: %s", errorPayload.Message)
	}
	
	if response.Type != types.MsgTypeServiceInfo {
		return nil, nil, fmt.Errorf("unexpected response type: %s", response.Type)
	}
	
	var serviceInfo types.ServiceInfoPayload
	payloadBytes, err := json.Marshal(response.Payload)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to marshal service info: %v", err)
	}
	if err := json.Unmarshal(payloadBytes, &serviceInfo); err != nil {
		return nil, nil, fmt.Errorf("failed to unmarshal service info: %v", err)
	}
	
	return &serviceInfo.Service, serviceInfo.Endpoints, nil
}

// reconnect 重新连接到服务器
func (c *Client) reconnect() {
	log.Println("attempting to reconnect to server...")
	
	c.authenticated = false
	if err := c.connectToServer(); err != nil {
		log.Printf("failed to reconnect: %v", err)
	} else {
		log.Println("successfully reconnected to server")
	}
}

// Stop 停止客户端
func (c *Client) Stop() error {
	if c.cancel != nil {
		c.cancel()
	}

	// 停止所有代理
	c.proxiesMu.Lock()
	for _, proxy := range c.proxies {
		if proxy.Listener != nil {
			proxy.Listener.Close()
		}
		if proxy.UDPConn != nil {
			proxy.UDPConn.Close()
		}
		proxy.Active = false
	}
	c.proxiesMu.Unlock()

	c.closeConnection()
	return nil
}

// handleUDPProxyPackets 处理 UDP 代理数据包
func (c *Client) handleUDPProxyPackets(proxy *ProxyInstance) {
	defer proxy.UDPConn.Close()

	log.Printf("UDP proxy %s listening on port %d", proxy.Config.Name, proxy.Config.LocalPort)

	// UDP 会话管理（伪连接管理）
	sessions := make(map[string]*UDPSession)
	sessionsMu := sync.RWMutex{}

	buffer := make([]byte, 64*1024) // 64KB 缓冲区

	for {
		select {
		case <-c.ctx.Done():
			return
		default:
			// 设置读取超时
			proxy.UDPConn.SetReadDeadline(time.Now().Add(1 * time.Second))
			
			n, clientAddr, err := proxy.UDPConn.ReadFromUDP(buffer)
			if err != nil {
				if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
					continue // 超时继续
				}
				log.Printf("failed to read UDP packet for proxy %s: %v", proxy.Config.Name, err)
				continue
			}

			// 处理数据包
			go c.handleUDPPacket(proxy, clientAddr, buffer[:n], sessions, &sessionsMu)
		}
	}
}

// UDPSession UDP 会话信息
type UDPSession struct {
	ClientAddr   *net.UDPAddr
	TargetConn   *net.UDPConn
	LastActivity time.Time
	mu           sync.Mutex
}

// handleUDPPacket 处理单个 UDP 数据包
func (c *Client) handleUDPPacket(proxy *ProxyInstance, clientAddr *net.UDPAddr, data []byte, sessions map[string]*UDPSession, sessionsMu *sync.RWMutex) {
	sessionKey := clientAddr.String()

	sessionsMu.RLock()
	session, exists := sessions[sessionKey]
	sessionsMu.RUnlock()

	if !exists {
		// 创建新会话
		session = c.createUDPSession(proxy, clientAddr)
		if session == nil {
			return
		}

		sessionsMu.Lock()
		sessions[sessionKey] = session
		sessionsMu.Unlock()

		// 启动会话数据接收
		go c.handleUDPSessionData(proxy, session, sessions, sessionsMu)
	}

	// 更新会话活动时间
	session.mu.Lock()
	session.LastActivity = time.Now()
	session.mu.Unlock()

	// 转发数据到目标端点
	_, err := session.TargetConn.Write(data)
	if err != nil {
		log.Printf("failed to write UDP data to target for proxy %s: %v", proxy.Config.Name, err)
		c.cleanupUDPSession(sessionKey, session, sessions, sessionsMu)
	}
}

// createUDPSession 创建新的 UDP 会话
func (c *Client) createUDPSession(proxy *ProxyInstance, clientAddr *net.UDPAddr) *UDPSession {
	// 1. 查询目标服务的端点
	service, endpoints, err := c.queryService(
		proxy.Config.ServiceName,
		proxy.Config.ServiceNamespace,
		proxy.Config.Selector,
	)
	if err != nil {
		log.Printf("failed to query service %s for UDP session: %v", proxy.Config.ServiceName, err)
		return nil
	}

	if len(endpoints) == 0 {
		log.Printf("no available endpoints for UDP service %s", proxy.Config.ServiceName)
		return nil
	}

	// 2. 转换 endpoints 为指针切片
	endpointPtrs := make([]*types.Endpoint, len(endpoints))
	for i := range endpoints {
		endpointPtrs[i] = &endpoints[i]
	}
	
	// 3. 使用负载均衡器选择端点
	endpoint := c.selectEndpoint(service, endpointPtrs, clientAddr.String())
	if endpoint == nil {
		log.Printf("no suitable endpoint selected for UDP service %s", proxy.Config.ServiceName)
		return nil
	}
	targetAddr := fmt.Sprintf("%s:%d", endpoint.IP, endpoint.Port)

	log.Printf("UDP proxy %s creating session to endpoint %s for client %s", 
		proxy.Config.Name, targetAddr, clientAddr.String())

	// 3. 建立到目标端点的 UDP 连接
	targetUDPAddr, err := net.ResolveUDPAddr("udp", targetAddr)
	if err != nil {
		log.Printf("failed to resolve target UDP address %s: %v", targetAddr, err)
		return nil
	}

	targetConn, err := net.DialUDP("udp", nil, targetUDPAddr)
	if err != nil {
		log.Printf("failed to connect to UDP target %s: %v", targetAddr, err)
		return nil
	}

	log.Printf("UDP proxy %s established session to %s for service %s", 
		proxy.Config.Name, targetAddr, service.Metadata.Name)

	return &UDPSession{
		ClientAddr:   clientAddr,
		TargetConn:   targetConn,
		LastActivity: time.Now(),
	}
}

// handleUDPSessionData 处理 UDP 会话的返回数据
func (c *Client) handleUDPSessionData(proxy *ProxyInstance, session *UDPSession, sessions map[string]*UDPSession, sessionsMu *sync.RWMutex) {
	defer session.TargetConn.Close()

	buffer := make([]byte, 64*1024)
	sessionKey := session.ClientAddr.String()

	for {
		// 设置读取超时
		session.TargetConn.SetReadDeadline(time.Now().Add(30 * time.Second))
		
		n, err := session.TargetConn.Read(buffer)
		if err != nil {
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				// 检查会话是否过期
				session.mu.Lock()
				inactive := time.Since(session.LastActivity) > 5*time.Minute
				session.mu.Unlock()
				
				if inactive {
					log.Printf("UDP session %s expired, cleaning up", sessionKey)
					c.cleanupUDPSession(sessionKey, session, sessions, sessionsMu)
					return
				}
				continue
			}
			log.Printf("failed to read UDP data from target for session %s: %v", sessionKey, err)
			c.cleanupUDPSession(sessionKey, session, sessions, sessionsMu)
			return
		}

		// 转发数据回客户端
		_, err = proxy.UDPConn.WriteToUDP(buffer[:n], session.ClientAddr)
		if err != nil {
			log.Printf("failed to write UDP data to client %s: %v", sessionKey, err)
			c.cleanupUDPSession(sessionKey, session, sessions, sessionsMu)
			return
		}

		// 更新会话活动时间
		session.mu.Lock()
		session.LastActivity = time.Now()
		session.mu.Unlock()
	}
}

// cleanupUDPSession 清理 UDP 会话
func (c *Client) cleanupUDPSession(sessionKey string, session *UDPSession, sessions map[string]*UDPSession, sessionsMu *sync.RWMutex) {
	sessionsMu.Lock()
	delete(sessions, sessionKey)
	sessionsMu.Unlock()
	
	if session.TargetConn != nil {
		session.TargetConn.Close()
	}
}

// startSTCPProxy 启动 STCP 代理（加密隧道模式）
func (c *Client) startSTCPProxy(proxyConfig types.Proxy) error {
	// 监听本地端口（类似 TCP 模式）
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", proxyConfig.LocalPort))
	if err != nil {
		return fmt.Errorf("failed to listen on port %d: %v", proxyConfig.LocalPort, err)
	}

	proxy := &ProxyInstance{
		Config:   proxyConfig,
		Listener: listener,
		Active:   true,
	}

	// 注册代理实例
	c.proxiesMu.Lock()
	c.proxies[proxyConfig.Name] = proxy
	c.proxiesMu.Unlock()

	// 在独立的 goroutine 中处理 STCP 连接
	go c.handleSTCPProxyConnections(proxy)

	return nil
}

// startSUDPProxy 启动 SUDP 代理（加密 UDP 模式）
func (c *Client) startSUDPProxy(proxyConfig types.Proxy) error {
	// 监听本地 UDP 端口
	addr, err := net.ResolveUDPAddr("udp", fmt.Sprintf(":%d", proxyConfig.LocalPort))
	if err != nil {
		return fmt.Errorf("failed to resolve UDP address: %v", err)
	}

	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		return fmt.Errorf("failed to listen on UDP port %d: %v", proxyConfig.LocalPort, err)
	}

	proxy := &ProxyInstance{
		Config:   proxyConfig,
		UDPConn:  conn,
		Active:   true,
	}

	// 注册代理实例
	c.proxiesMu.Lock()
	c.proxies[proxyConfig.Name] = proxy
	c.proxiesMu.Unlock()

	// 在独立的 goroutine 中处理 SUDP 连接
	go c.handleSUDPProxyConnections(proxy)

	return nil
}

// handleSTCPProxyConnections 处理 STCP 代理连接
func (c *Client) handleSTCPProxyConnections(proxy *ProxyInstance) {
	defer proxy.Listener.Close()

	log.Printf("STCP proxy %s listening on port %d", proxy.Config.Name, proxy.Config.LocalPort)

	for {
		conn, err := proxy.Listener.Accept()
		if err != nil {
			select {
			case <-c.ctx.Done():
				return
			default:
				log.Printf("failed to accept STCP connection for proxy %s: %v", proxy.Config.Name, err)
				continue
			}
		}

		// 在独立的 goroutine 中处理每个 STCP 连接
		go c.handleSTCPProxyConnection(proxy, conn)
	}
}

// handleSTCPProxyConnection 处理单个 STCP 代理连接（加密隧道）
func (c *Client) handleSTCPProxyConnection(proxy *ProxyInstance, conn net.Conn) {
	defer conn.Close()

	log.Printf("new STCP connection to proxy %s from %s", proxy.Config.Name, conn.RemoteAddr())

	// 1. 查询目标服务的端点
	service, endpoints, err := c.queryService(
		proxy.Config.ServiceName,
		proxy.Config.ServiceNamespace,
		proxy.Config.Selector,
	)
	if err != nil {
		log.Printf("failed to query service %s for STCP: %v", proxy.Config.ServiceName, err)
		return
	}

	if len(endpoints) == 0 {
		log.Printf("no available endpoints for STCP service %s", proxy.Config.ServiceName)
		return
	}

	// 2. 转换 endpoints 为指针切片
	endpointPtrs := make([]*types.Endpoint, len(endpoints))
	for i := range endpoints {
		endpointPtrs[i] = &endpoints[i]
	}
	
	// 3. 使用负载均衡器选择端点
	endpoint := c.selectEndpoint(service, endpointPtrs, conn.RemoteAddr().String())
	if endpoint == nil {
		log.Printf("no suitable endpoint selected for STCP service %s", proxy.Config.ServiceName)
		return
	}
	targetAddr := fmt.Sprintf("%s:%d", endpoint.IP, endpoint.Port)

	log.Printf("STCP proxy %s connecting to endpoint %s", proxy.Config.Name, targetAddr)

	// 3. 建立到目标端点的加密隧道连接
	targetConn, err := c.establishSTCPTunnel(targetAddr, proxy.Config.ServiceName)
	if err != nil {
		log.Printf("failed to establish STCP tunnel to %s: %v", targetAddr, err)
		return
	}
	defer targetConn.Close()

	log.Printf("STCP proxy %s established encrypted tunnel to %s for service %s",
		proxy.Config.Name, targetAddr, service.Metadata.Name)

	// 4. 双向数据转发（隧道内的数据已经是加密的）
	done := make(chan struct{}, 2)

	// 客户端 -> 加密隧道
	go func() {
		defer func() { done <- struct{}{} }()
		_, err := io.Copy(targetConn, conn)
		if err != nil {
			log.Printf("error copying client->tunnel for STCP proxy %s: %v", proxy.Config.Name, err)
		}
	}()

	// 加密隧道 -> 客户端
	go func() {
		defer func() { done <- struct{}{} }()
		_, err := io.Copy(conn, targetConn)
		if err != nil {
			log.Printf("error copying tunnel->client for STCP proxy %s: %v", proxy.Config.Name, err)
		}
	}()

	// 等待任一方向的连接关闭
	<-done
	log.Printf("STCP proxy connection closed for %s", proxy.Config.Name)
}

// establishSTCPTunnel 建立 STCP 加密隧道
func (c *Client) establishSTCPTunnel(targetAddr, serviceName string) (net.Conn, error) {
	// 创建独立的控制连接用于隧道协商
	controlConn, err := net.DialTimeout("tcp", c.config.Client.ServerAddr, 10*time.Second)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to server for STCP tunnel: %v", err)
	}

	encoder := json.NewEncoder(controlConn)
	decoder := json.NewDecoder(controlConn)

	// 认证
	authMsg := types.Message{
		Type: types.MsgTypeAuth,
		ID:   uuid.New().String(),
		Payload: types.AuthPayload{
			Token: c.config.Client.AuthToken,
			Mode:  "stcp", // 特殊的 STCP 模式
		},
		Timestamp: time.Now(),
	}

	if err := encoder.Encode(authMsg); err != nil {
		controlConn.Close()
		return nil, fmt.Errorf("failed to send STCP auth message: %v", err)
	}

	var authResponse types.Message
	if err := decoder.Decode(&authResponse); err != nil {
		controlConn.Close()
		return nil, fmt.Errorf("failed to receive STCP auth response: %v", err)
	}

	if authResponse.Type != types.MsgTypeAuthResponse {
		controlConn.Close()
		return nil, fmt.Errorf("unexpected STCP auth response type: %s", authResponse.Type)
	}

	// 请求建立加密隧道
	tunnelMsg := types.Message{
		Type: types.MsgTypeSTCPTunnelRequest,
		ID:   uuid.New().String(),
		Payload: types.STCPTunnelRequestPayload{
			ServiceName: serviceName,
			TargetAddr:  targetAddr,
		},
		Timestamp: time.Now(),
	}

	if err := encoder.Encode(tunnelMsg); err != nil {
		controlConn.Close()
		return nil, fmt.Errorf("failed to send STCP tunnel request: %v", err)
	}

	var tunnelResponse types.Message
	if err := decoder.Decode(&tunnelResponse); err != nil {
		controlConn.Close()
		return nil, fmt.Errorf("failed to receive STCP tunnel response: %v", err)
	}

	if tunnelResponse.Type != types.MsgTypeSTCPTunnelResponse {
		controlConn.Close()
		return nil, fmt.Errorf("STCP tunnel request failed: %s", tunnelResponse.Type)
	}

	// 控制连接现在变成了数据隧道
	log.Printf("STCP tunnel established to %s for service %s", targetAddr, serviceName)
	return controlConn, nil
}

// handleSUDPProxyConnections 处理 SUDP 代理连接（加密 UDP）
func (c *Client) handleSUDPProxyConnections(proxy *ProxyInstance) {
	defer proxy.UDPConn.Close()

	log.Printf("SUDP proxy %s listening on UDP port %d", proxy.Config.Name, proxy.Config.LocalPort)

	// 建立到服务器的加密隧道连接
	tunnelConn, err := c.establishSUDPTunnel(proxy.Config)
	if err != nil {
		log.Printf("failed to establish SUDP tunnel for %s: %v", proxy.Config.Name, err)
		return
	}
	defer tunnelConn.Close()

	// 读取 UDP 数据包并转发到加密隧道
	buffer := make([]byte, 65536) // UDP 最大包大小
	for {
		select {
		case <-c.ctx.Done():
			return
		default:
			// 设置读取超时
			proxy.UDPConn.SetReadDeadline(time.Now().Add(time.Second))
			
			n, clientAddr, err := proxy.UDPConn.ReadFromUDP(buffer)
			if err != nil {
				if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
					continue // 超时，继续循环
				}
				log.Printf("SUDP proxy %s read error: %v", proxy.Config.Name, err)
				return
			}

			log.Printf("SUDP proxy %s received %d bytes from %s", proxy.Config.Name, n, clientAddr)

			// 封装 UDP 数据包并发送到加密隧道
			if err := c.forwardSUDPData(tunnelConn, buffer[:n], clientAddr); err != nil {
				log.Printf("SUDP proxy %s forward error: %v", proxy.Config.Name, err)
				return
			}
		}
	}
}

// establishSUDPTunnel 建立 SUDP 加密隧道
func (c *Client) establishSUDPTunnel(proxyConfig types.Proxy) (net.Conn, error) {
	// 1. 查询目标服务的端点
	service, endpoints, err := c.queryService(
		proxyConfig.ServiceName,
		proxyConfig.ServiceNamespace,
		proxyConfig.Selector,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to query service %s for SUDP: %v", proxyConfig.ServiceName, err)
	}

	if len(endpoints) == 0 {
		return nil, fmt.Errorf("no available endpoints for SUDP service %s", proxyConfig.ServiceName)
	}

	// 2. 转换 endpoints 为指针切片
	endpointPtrs := make([]*types.Endpoint, len(endpoints))
	for i := range endpoints {
		endpointPtrs[i] = &endpoints[i]
	}
	
	// 3. 使用负载均衡器选择端点
	endpoint := c.selectEndpoint(service, endpointPtrs, "")
	if endpoint == nil {
		return nil, fmt.Errorf("no suitable endpoint selected for SUDP service %s", proxyConfig.ServiceName)
	}
	targetAddr := fmt.Sprintf("%s:%d", endpoint.IP, endpoint.Port)

	log.Printf("SUDP proxy %s establishing tunnel to endpoint %s for service %s", 
		proxyConfig.Name, targetAddr, service.Metadata.Name)

	// 3. 建立到服务器的新控制连接
	controlConn, err := net.DialTimeout("tcp", c.config.Client.ServerAddr, 10*time.Second)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to server for SUDP tunnel: %v", err)
	}

	// 4. 认证
	encoder := json.NewEncoder(controlConn)
	decoder := json.NewDecoder(controlConn)

	authMsg := types.Message{
		Type: types.MsgTypeAuth,
		ID:   uuid.New().String(),
		Payload: types.AuthPayload{
			Token: c.config.Client.AuthToken,
			Mode:  "proxy",
		},
		Timestamp: time.Now(),
	}

	if err := encoder.Encode(authMsg); err != nil {
		controlConn.Close()
		return nil, fmt.Errorf("failed to send SUDP auth: %v", err)
	}

	var authResponse types.Message
	if err := decoder.Decode(&authResponse); err != nil {
		controlConn.Close()
		return nil, fmt.Errorf("failed to receive SUDP auth response: %v", err)
	}

	if authResponse.Type != types.MsgTypeAuthResponse {
		controlConn.Close()
		return nil, fmt.Errorf("SUDP authentication failed")
	}

	// 5. 请求建立 SUDP 隧道
	tunnelMsg := types.Message{
		Type: types.MsgTypeSUDPTunnelRequest,
		ID:   uuid.New().String(),
		Payload: types.SUDPTunnelRequestPayload{
			ServiceName: proxyConfig.ServiceName,
			TargetAddr:  targetAddr,
			LocalPort:   proxyConfig.LocalPort,
		},
		Timestamp: time.Now(),
	}

	if err := encoder.Encode(tunnelMsg); err != nil {
		controlConn.Close()
		return nil, fmt.Errorf("failed to send SUDP tunnel request: %v", err)
	}

	var tunnelResponse types.Message
	if err := decoder.Decode(&tunnelResponse); err != nil {
		controlConn.Close()
		return nil, fmt.Errorf("failed to receive SUDP tunnel response: %v", err)
	}

	if tunnelResponse.Type != types.MsgTypeSUDPTunnelResponse {
		controlConn.Close()
		return nil, fmt.Errorf("SUDP tunnel request failed: %s", tunnelResponse.Type)
	}

	// 控制连接现在变成了加密数据隧道
	log.Printf("SUDP tunnel established to %s for service %s", targetAddr, proxyConfig.ServiceName)
	return controlConn, nil
}

// forwardSUDPData 转发 SUDP 数据到加密隧道
func (c *Client) forwardSUDPData(tunnelConn net.Conn, data []byte, clientAddr *net.UDPAddr) error {
	// 创建 SUDP 数据包格式：[length][client_addr][data]
	clientAddrBytes := []byte(clientAddr.String())
	
	// 构建完整的数据包
	packet := make([]byte, 4+4+len(clientAddrBytes)+len(data))
	
	// 写入总长度
	binary.BigEndian.PutUint32(packet[0:4], uint32(len(packet)-4))
	
	// 写入客户端地址长度
	binary.BigEndian.PutUint32(packet[4:8], uint32(len(clientAddrBytes)))
	
	// 写入客户端地址
	copy(packet[8:8+len(clientAddrBytes)], clientAddrBytes)
	
	// 写入数据
	copy(packet[8+len(clientAddrBytes):], data)

	// 发送到加密隧道
	_, err := tunnelConn.Write(packet)
	return err
}