package server

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/outpostos/OutpostNet/pkg/config"
	"github.com/outpostos/OutpostNet/pkg/healthcheck"
	"github.com/outpostos/OutpostNet/pkg/listener"
	"github.com/outpostos/OutpostNet/pkg/loadbalancer"
	"github.com/outpostos/OutpostNet/pkg/registry"
	"github.com/outpostos/OutpostNet/pkg/types"
)

// Server outpostnet 服务器
type Server struct {
	config        *config.ServerConfig
	multiListener *listener.MultiProtocolListener // 多协议监听器
	registry      types.ServiceRegistry

	// 客户端连接管理
	clients   map[string]*ClientConn
	clientsMu sync.RWMutex

	// 负载均衡管理
	lbManager *loadbalancer.LoadBalancerManager

	// 健康检查管理
	healthChecker *healthcheck.HealthCheckManager

	// 连接跟踪器
	connTracker *listener.ConnectionTracker

	// 工作连接管理
	workConnManager *WorkConnManager
	
	// Register 工作连接管理
	registerWorkConnManager *RegisterWorkConnManager

	// 控制通道
	ctx    context.Context
	cancel context.CancelFunc
}

// ClientConn 表示一个客户端连接
type ClientConn struct {
	ID            string
	Conn          net.Conn
	Mode          string // proxy 或 register
	Authenticated bool
	Workloads     []string // register 模式下注册的工作负载 ID
	LastSeen      time.Time
}

// WorkConn 表示一个工作连接
type WorkConn struct {
	ID               string
	Conn             net.Conn
	ServiceName      string
	ServiceNamespace string
	TargetEndpoint   *types.Endpoint
	CreatedAt        time.Time
}

// WorkConnManager 管理工作连接 (服务器中转模式)
type WorkConnManager struct {
	// 按客户端ID组织的工作连接池
	clientPools map[string]*ClientWorkConnPool
	mu          sync.RWMutex
}

// ClientWorkConnPool 单个客户端的工作连接池
type ClientWorkConnPool struct {
	ClientID    string
	workConnCh  chan net.Conn      // 工作连接队列
	poolCount   int               // 池大小
	mu          sync.RWMutex
}

// RegisterWorkConnManager 管理来自 Register Client 的工作连接
type RegisterWorkConnManager struct {
	// 按服务分组的工作连接池
	serviceConnections map[string][]*RegisterWorkConn
	// 所有工作连接的索引
	allConnections map[string]*RegisterWorkConn
	mu            sync.RWMutex
}

// RegisterWorkConn 表示来自 Register Client 的工作连接
type RegisterWorkConn struct {
	ID           string
	Conn         net.Conn
	ClientID     string
	Workloads    []types.WorkloadInfo // 这个连接可以处理的工作负载
	LastActivity time.Time
	Available    bool // 是否可用于新的代理请求
}

// WorkConnRequest 工作连接请求
type WorkConnRequest struct {
	ClientID    string
	ProxyName   string
	SrcAddr     string
	DstAddr     string
	Protocol    string
	WorkloadID  string
}


// NewRegisterWorkConnManager 创建 Register 工作连接管理器
func NewRegisterWorkConnManager() *RegisterWorkConnManager {
	return &RegisterWorkConnManager{
		serviceConnections: make(map[string][]*RegisterWorkConn),
		allConnections:     make(map[string]*RegisterWorkConn),
	}
}

// AddWorkConn 添加工作连接
func (rm *RegisterWorkConnManager) AddWorkConn(workConn *RegisterWorkConn) {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	
	// 添加到全局连接索引
	rm.allConnections[workConn.ID] = workConn
	
	// 按工作负载分组添加到服务连接池
	for _, workload := range workConn.Workloads {
		serviceKey := fmt.Sprintf("%s-%s", workload.Labels["app"], workload.Protocol)
		if _, exists := rm.serviceConnections[serviceKey]; !exists {
			rm.serviceConnections[serviceKey] = make([]*RegisterWorkConn, 0)
		}
		rm.serviceConnections[serviceKey] = append(rm.serviceConnections[serviceKey], workConn)
	}
	
	log.Printf("added register work connection %s for %d workloads", workConn.ID, len(workConn.Workloads))
}

// GetAvailableConnection 获取可用的工作连接
func (rm *RegisterWorkConnManager) GetAvailableConnection(serviceName, protocol string) *RegisterWorkConn {
	rm.mu.RLock()
	defer rm.mu.RUnlock()
	
	// 基于服务名推断 app 标签
	appLabel := strings.TrimSuffix(serviceName, "-service")
	serviceKey := fmt.Sprintf("%s-%s", appLabel, protocol)
	
	connections, exists := rm.serviceConnections[serviceKey]
	if !exists || len(connections) == 0 {
		return nil
	}
	
	// 找到第一个可用的连接
	for _, conn := range connections {
		if conn.Available {
			return conn
		}
	}
	
	return nil
}

// MarkConnectionBusy 标记连接为忙碌状态
func (rm *RegisterWorkConnManager) MarkConnectionBusy(connectionID string) {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	
	if conn, exists := rm.allConnections[connectionID]; exists {
		conn.Available = false
		conn.LastActivity = time.Now()
	}
}

// MarkConnectionAvailable 标记连接为可用状态
func (rm *RegisterWorkConnManager) MarkConnectionAvailable(connectionID string) {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	
	if conn, exists := rm.allConnections[connectionID]; exists {
		conn.Available = true
		conn.LastActivity = time.Now()
	}
}

// RemoveConnection 移除工作连接
func (rm *RegisterWorkConnManager) RemoveConnection(connectionID string) {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	
	conn, exists := rm.allConnections[connectionID]
	if !exists {
		return
	}
	
	// 从全局索引中移除
	delete(rm.allConnections, connectionID)
	
	// 从服务连接池中移除
	for _, workload := range conn.Workloads {
		serviceKey := fmt.Sprintf("%s-%s", workload.Labels["app"], workload.Protocol)
		connections := rm.serviceConnections[serviceKey]
		
		// 找到并移除连接
		for i, c := range connections {
			if c.ID == connectionID {
				rm.serviceConnections[serviceKey] = append(connections[:i], connections[i+1:]...)
				break
			}
		}
		
		// 如果服务没有连接了，清理空切片
		if len(rm.serviceConnections[serviceKey]) == 0 {
			delete(rm.serviceConnections, serviceKey)
		}
	}
	
	log.Printf("removed register work connection %s", connectionID)
}

// handleWorkConnReady 处理来自 Register Client 的工作连接就绪消息
func (s *Server) handleWorkConnReady(client *ClientConn, msg *types.Message, encoder *json.Encoder) error {
	var payload types.WorkConnReadyPayload
	payloadBytes, err := json.Marshal(msg.Payload)
	if err != nil {
		return fmt.Errorf("invalid work connection ready payload: %v", err)
	}
	if err := json.Unmarshal(payloadBytes, &payload); err != nil {
		return fmt.Errorf("failed to parse work connection ready payload: %v", err)
	}

	log.Printf("register client %s work connection %s ready with %d workloads", 
		client.ID, payload.ConnectionID, len(payload.Workloads))

	// 创建新的工作连接记录
	registerConn := &RegisterWorkConn{
		ID:           payload.ConnectionID,
		Conn:         client.Conn,
		ClientID:     client.ID,
		Workloads:    payload.Workloads,
		LastActivity: time.Now(),
		Available:    true, // 初始状态为可用
	}

	// 添加到工作连接管理器
	s.registerWorkConnManager.AddWorkConn(registerConn)

	// 发送确认响应
	response := map[string]interface{}{
		"success":       true,
		"message":       "Work connection registered successfully",
		"connection_id": payload.ConnectionID,
	}

	return s.sendGenericResponse(encoder, msg.ID, types.MsgTypeWorkConnResponse, response)
}

// NewWorkConnManager 创建工作连接管理器 (服务器中转模式)
func NewWorkConnManager() *WorkConnManager {
	return &WorkConnManager{
		clientPools: make(map[string]*ClientWorkConnPool),
	}
}

// GetOrCreateClientPool 获取或创建客户端工作连接池
func (wm *WorkConnManager) GetOrCreateClientPool(clientID string, poolSize int) *ClientWorkConnPool {
	wm.mu.Lock()
	defer wm.mu.Unlock()
	
	pool, exists := wm.clientPools[clientID]
	if !exists {
		pool = &ClientWorkConnPool{
			ClientID:   clientID,
			workConnCh: make(chan net.Conn, poolSize+10),
			poolCount:  poolSize,
		}
		wm.clientPools[clientID] = pool
		log.Printf("created work connection pool for client %s with size %d", clientID, poolSize)
	}
	return pool
}

// AddWorkConn 添加工作连接 (兼容性方法，服务器中转模式不使用)
func (wm *WorkConnManager) AddWorkConn(workConn *WorkConn) {
	log.Printf("AddWorkConn called but not used in server relay mode: %s", workConn.ID)
}

// GetWorkConn 获取工作连接 (兼容性方法，服务器中转模式不使用)
func (wm *WorkConnManager) GetWorkConn(id string) (*WorkConn, bool) {
	log.Printf("GetWorkConn called but not used in server relay mode: %s", id)
	return nil, false
}

// RemoveWorkConn 移除工作连接 (兼容性方法，服务器中转模式不使用)
func (wm *WorkConnManager) RemoveWorkConn(id string) {
	log.Printf("RemoveWorkConn called but not used in server relay mode: %s", id)
}

// GetWorkConn 获取工作连接 (服务器中转模式)
func (pool *ClientWorkConnPool) GetWorkConn() (net.Conn, error) {
	pool.mu.Lock()
	defer pool.mu.Unlock()
	
	log.Printf("attempting to get work connection from pool for client %s (current count: %d)", pool.ClientID, pool.poolCount)
	
	select {
	case workConn := <-pool.workConnCh:
		pool.poolCount--
		log.Printf("successfully got work connection from pool for client %s (remaining count: %d)", pool.ClientID, pool.poolCount)
		return workConn, nil
	default:
		log.Printf("no available work connection in pool for client %s (count: %d)", pool.ClientID, pool.poolCount)
		return nil, fmt.Errorf("no available work connection in pool for client %s", pool.ClientID)
	}
}

// AddWorkConn 添加工作连接到池
func (pool *ClientWorkConnPool) AddWorkConn(conn net.Conn) error {
	pool.mu.Lock()
	defer pool.mu.Unlock()
	
	log.Printf("attempting to add work connection to pool for client %s (current count: %d, capacity: %d)", 
		pool.ClientID, pool.poolCount, cap(pool.workConnCh))
	
	select {
	case pool.workConnCh <- conn:
		pool.poolCount++
		log.Printf("successfully added work connection to pool for client %s (new count: %d)", pool.ClientID, pool.poolCount)
		return nil
	default:
		log.Printf("work connection pool full for client %s (capacity: %d), discarding connection", pool.ClientID, cap(pool.workConnCh))
		conn.Close()
		return fmt.Errorf("work connection pool is full")
	}
}

// NewServer 创建新的服务器实例
func NewServer(cfg *config.ServerConfig) (*Server, error) {
	if cfg == nil {
		return nil, fmt.Errorf("config is nil")
	}

	// 创建 K8s 风格服务注册表
	registry := registry.NewK8sMemoryRegistry()

	// 注册配置中的服务
	for _, service := range cfg.Services {
		if err := registry.RegisterService(&service); err != nil {
			return nil, fmt.Errorf("failed to register service %s: %v", service.Metadata.Name, err)
		}
		log.Printf("registered service: %s/%s", service.Metadata.Namespace, service.Metadata.Name)
	}

	// 创建多协议监听器
	multiListener, err := listener.NewMultiProtocolListener(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create multi-protocol listener: %v", err)
	}

	s := &Server{
		config:                  cfg,
		multiListener:           multiListener,
		registry:                registry,
		clients:                 make(map[string]*ClientConn),
		lbManager:               loadbalancer.NewLoadBalancerManager(),
		healthChecker:           healthcheck.NewHealthCheckManager(),
		connTracker:             listener.NewConnectionTracker(),
		workConnManager:         NewWorkConnManager(),
		registerWorkConnManager: NewRegisterWorkConnManager(),
	}

	return s, nil
}

// getOrCreateService 获取服务，如果不存在且启用自动创建则创建服务
func (s *Server) getOrCreateService(serviceName, serviceNamespace string) (*types.Service, error) {
	// 首先尝试获取现有服务
	service, err := s.registry.GetService(serviceName, serviceNamespace)
	if err == nil {
		return service, nil
	}

	// 如果服务不存在且没有启用自动创建，返回错误
	if s.config.ServiceDiscovery == nil || !s.config.ServiceDiscovery.AutoCreateServices {
		return nil, fmt.Errorf("service not found: %s/%s", serviceNamespace, serviceName)
	}

	log.Printf("service %s/%s not found, attempting auto-creation", serviceNamespace, serviceName)

	// 查找匹配的工作负载来创建服务 - 暂时简化实现
	// TODO: 实现基于已注册工作负载的服务创建逻辑
	
	// 暂时使用默认配置创建服务
	expectedAppLabel := strings.TrimSuffix(serviceName, "-service")
	
	// 确定端口配置 - 暂时使用推断的默认值
	port := 80 // 默认端口
	if strings.Contains(serviceName, "udp") {
		port = 18001 // UDP 服务默认端口
	}

	// 确定协议
	protocol := "tcp" // 默认协议
	if strings.Contains(serviceName, "udp") {
		protocol = "udp"
	}

	// 创建新服务
	newService := &types.Service{
		Metadata: types.ServiceMetadata{
			Name:      serviceName,
			Namespace: serviceNamespace,
			Labels: map[string]string{
				"auto-created": "true",
				"app":          expectedAppLabel,
			},
		},
		Spec: types.ServiceSpec{
			Selector: map[string]string{
				"app": expectedAppLabel,
			},
			Ports: []types.ServicePort{
				{
					Name:       protocol,
					Port:       port,
					TargetPort: port,
					Protocol:   protocol,
				},
			},
			ProxyMode:    protocol,
			LoadBalancer: s.config.ServiceDiscovery.DefaultLoadBalancer,
		},
	}

	// 注册新服务
	if err := s.registry.RegisterService(newService); err != nil {
		return nil, fmt.Errorf("failed to auto-create service %s/%s: %v", serviceNamespace, serviceName, err)
	}

	log.Printf("auto-created service %s/%s with selector app=%s, port=%d, protocol=%s", 
		serviceNamespace, serviceName, expectedAppLabel, port, protocol)

	return newService, nil
}

// selectEndpoint 使用负载均衡器选择健康的端点
func (s *Server) selectEndpoint(service *types.Service, endpoints []*types.Endpoint, clientID string) *types.Endpoint {
	if len(endpoints) == 0 {
		return nil
	}

	// 先过滤出健康的端点
	healthyEndpoints := s.healthChecker.GetHealthyEndpoints(endpoints)
	if len(healthyEndpoints) == 0 {
		log.Printf("no healthy endpoints available for service %s, falling back to all endpoints", service.Metadata.Name)
		healthyEndpoints = endpoints // 如果没有健康的端点，使用所有端点作为后备
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
	lb := s.lbManager.GetLoadBalancer(service.Metadata.Name, lbType)

	// 选择端点（对于一致性哈希，使用客户端ID作为键）
	key := clientID
	if key == "" {
		key = fmt.Sprintf("%s:%d", service.Metadata.Name, time.Now().UnixNano())
	}

	return lb.SelectEndpoint(healthyEndpoints, key)
}

// Run 启动服务器
func (s *Server) Run(ctx context.Context) error {
	s.ctx, s.cancel = context.WithCancel(ctx)
	defer s.cancel()

	// 启动多协议监听器
	if err := s.multiListener.Start(); err != nil {
		return fmt.Errorf("failed to start multi-protocol listener: %v", err)
	}

	// 确保监听器在退出时关闭
	defer func() {
		if err := s.multiListener.Stop(); err != nil {
			log.Printf("error stopping multi-protocol listener: %v", err)
		}
	}()

	// 记录活跃监听器
	listeners := s.multiListener.GetActiveListeners()
	for _, info := range listeners {
		log.Printf("outpostnet-server listening on %s (%s)", info.Address, info.Protocol)
	}

	// 启动健康检查
	s.healthChecker.Start(s.ctx)
	log.Printf("health checker started")

	// 启动 K8s 兼容 API（如果启用）
	if s.config.K8sAPI.Enabled {
		k8sAPI := NewK8sAPI(s)
		go func() {
			log.Printf("starting K8s API on port %d", s.config.K8sAPI.Port)
			if err := k8sAPI.Start(s.config.K8sAPI.Port); err != nil {
				log.Printf("K8s API error: %v", err)
			}
		}()
	}

	// 启动监控服务器（如果启用）
	if s.config.Monitoring.Enabled {
		go func() {
			metricsServer := s.startMetricsServer()
			if err := metricsServer.Start(s.ctx); err != nil {
				log.Printf("Metrics server error: %v", err)
			}
		}()
	}

	// 启动连接处理
	go s.handleConnections()

	// 启动错误监控
	go s.handleErrors()

	// 等待上下文取消
	<-s.ctx.Done()
	log.Println("shutting down outpostnet-server")

	return nil
}

// handleConnections 处理新连接
func (s *Server) handleConnections() {
	log.Printf("started connection handler")

	for {
		select {
		case <-s.ctx.Done():
			log.Printf("connection handler shutting down")
			return
		case connInfo := <-s.multiListener.GetConnectionChannel():
			// 跟踪连接
			connID := s.connTracker.TrackConnection(connInfo)
			log.Printf("new connection %s from %s via %s", connID, connInfo.RemoteAddr, connInfo.Protocol)

			// 在独立的 goroutine 中处理连接
			go s.handleClientConnection(connInfo, connID)
		}
	}
}

// handleErrors 处理监听器错误
func (s *Server) handleErrors() {
	log.Printf("started error handler")

	for {
		select {
		case <-s.ctx.Done():
			log.Printf("error handler shutting down")
			return
		case err := <-s.multiListener.GetErrorChannel():
			log.Printf("listener error: %v", err)
		}
	}
}

// handleClientConnection 处理客户端连接
func (s *Server) handleClientConnection(connInfo listener.ConnectionInfo, connID string) {
	connectionTakenOver := false
	defer func() {
		if !connectionTakenOver {
			connInfo.Conn.Close()
		}
	}()
	defer s.connTracker.UntrackConnection(connID)

	clientID := connInfo.RemoteAddr
	log.Printf("handling client connection %s from %s via %s", connID, clientID, connInfo.Protocol)

	// 创建客户端连接对象
	client := &ClientConn{
		ID:   clientID,
		Conn: connInfo.Conn,
	}

	// 注册客户端
	s.clientsMu.Lock()
	s.clients[clientID] = client
	s.clientsMu.Unlock()

	// 清理客户端连接
	defer func() {
		s.clientsMu.Lock()
		// 清理客户端注册的工作负载
		for _, workloadID := range client.Workloads {
			// 从健康检查中移除相关端点
			endpoints, err := s.registry.GetEndpoints("", "", nil)
			if err == nil {
				for _, endpoint := range endpoints {
					if endpoint.Labels != nil {
						if workloadIDLabel, exists := endpoint.Labels["workload_id"]; exists && workloadIDLabel == workloadID {
							s.healthChecker.RemoveEndpoint(endpoint.ID)
							log.Printf("removed endpoint %s from health checker", endpoint.ID)
						}
					}
				}
			}

			if err := s.registry.UnregisterWorkload(workloadID); err != nil {
				log.Printf("failed to unregister workload %s: %v", workloadID, err)
			}
		}
		delete(s.clients, clientID)
		s.clientsMu.Unlock()
		log.Printf("client connection closed: %s (connection ID: %s)", clientID, connID)
	}()

	// 处理客户端消息
	connectionTakenOver = s.handleClientMessages(client)
}

// handleClientMessages 处理客户端消息
func (s *Server) handleClientMessages(client *ClientConn) bool {
	decoder := json.NewDecoder(client.Conn)
	encoder := json.NewEncoder(client.Conn)

	for {
		select {
		case <-s.ctx.Done():
			return false
		default:
			var msg types.Message
			if err := decoder.Decode(&msg); err != nil {
				log.Printf("failed to decode message from client %s: %v", client.ID, err)
				return false
			}

			client.LastSeen = time.Now()

			// 处理不同类型的消息
			if err := s.handleMessage(client, &msg, encoder); err != nil {
				// 检查是否是连接被接管
				if strings.Contains(err.Error(), "connection taken over by") {
					log.Printf("client %s connection taken over, ending message loop", client.ID)
					return true // 返回true表示连接被接管，防止自动关闭
				}
				log.Printf("failed to handle message from client %s: %v", client.ID, err)
				// 发送错误响应
				s.sendError(encoder, msg.ID, 500, err.Error())
			}
		}
	}
}

// Stop 停止服务器
func (s *Server) Stop() error {
	if s.cancel != nil {
		s.cancel()
	}

	if s.multiListener != nil {
		if err := s.multiListener.Stop(); err != nil {
			log.Printf("error stopping multi-protocol listener: %v", err)
			return err
		}
	}

	return nil
}

// GetRegistry 获取服务注册表
func (s *Server) GetRegistry() types.ServiceRegistry {
	return s.registry
}

// GetListenerStats 获取监听器统计信息
func (s *Server) GetListenerStats() *listener.ListenerStats {
	if s.multiListener != nil {
		return s.multiListener.GetStats()
	}
	return nil
}

// GetActiveListeners 获取活跃监听器信息
func (s *Server) GetActiveListeners() []listener.ListenerInfo {
	if s.multiListener != nil {
		return s.multiListener.GetActiveListeners()
	}
	return nil
}

// GetConnectionStats 获取连接统计信息
func (s *Server) GetConnectionStats() map[string]*listener.TrackedConnection {
	if s.connTracker != nil {
		return s.connTracker.GetConnections()
	}
	return nil
}

// GetConnectionsByProtocol 按协议获取连接信息
func (s *Server) GetConnectionsByProtocol(protocol string) []*listener.TrackedConnection {
	if s.connTracker != nil {
		return s.connTracker.GetConnectionsByProtocol(protocol)
	}
	return nil
}

// handleMessage 处理具体的消息类型
func (s *Server) handleMessage(client *ClientConn, msg *types.Message, encoder *json.Encoder) error {
	switch msg.Type {
	case types.MsgTypeAuth:
		return s.handleAuth(client, msg, encoder)
	case types.MsgTypeRegisterWorkload:
		if !client.Authenticated {
			return fmt.Errorf("client not authenticated")
		}
		return s.handleRegisterWorkload(client, msg, encoder)
	case types.MsgTypeQueryService:
		if !client.Authenticated {
			return fmt.Errorf("client not authenticated")
		}
		return s.handleQueryService(client, msg, encoder)
	case types.MsgTypeWorkConnRequest:
		if !client.Authenticated {
			return fmt.Errorf("client not authenticated")
		}
		return s.handleWorkConnRequest(client, msg, encoder)
	case types.MsgTypeWorkConnID:
		return s.handleWorkConnID(client, msg, encoder)
	case types.MsgTypeTunnelRequest:
		if !client.Authenticated {
			return fmt.Errorf("client not authenticated")
		}
		return s.handleTunnelRequest(client, msg, encoder)
	case types.MsgTypeUDPTunnelRequest:
		if !client.Authenticated {
			return fmt.Errorf("client not authenticated")
		}
		return s.handleUDPTunnelRequest(client, msg, encoder)
	case types.MsgTypeCreateService:
		if !client.Authenticated {
			return fmt.Errorf("client not authenticated")
		}
		return s.handleCreateService(client, msg, encoder)
	case types.MsgTypeSTCPTunnelRequest:
		if !client.Authenticated {
			return fmt.Errorf("client not authenticated")
		}
		return s.handleSTCPTunnelRequest(client, msg, encoder)
	case types.MsgTypeSUDPTunnelRequest:
		if !client.Authenticated {
			return fmt.Errorf("client not authenticated")
		}
		return s.handleSUDPTunnelRequest(client, msg, encoder)
	case types.MsgTypeHeartbeat:
		return s.handleHeartbeat(client, msg, encoder)
	case types.MsgTypeWorkConnReady:
		if !client.Authenticated {
			return fmt.Errorf("client not authenticated")
		}
		return s.handleWorkConnReady(client, msg, encoder)
	case types.MsgTypeNewWorkConn:
		// 工作连接不需要认证，它们是纯数据流管道 (服务器中转模式)
		return s.handleNewWorkConn(client, msg, encoder)
	case types.MsgTypeReqWorkConn:
		// 请求工作连接消息 (服务器中转模式)
		if !client.Authenticated {
			return fmt.Errorf("client not authenticated")
		}
		return s.handleReqWorkConn(client, msg, encoder)
	default:
		return fmt.Errorf("unknown message type: %s", msg.Type)
	}
}

// handleAuth 处理认证消息
func (s *Server) handleAuth(client *ClientConn, msg *types.Message, encoder *json.Encoder) error {
	var payload types.AuthPayload
	payloadBytes, err := json.Marshal(msg.Payload)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(payloadBytes, &payload); err != nil {
		return err
	}

	// 验证 token
	success := payload.Token == s.config.Server.AuthToken
	var message string
	if success {
		client.Authenticated = true
		client.Mode = payload.Mode
		message = "authentication successful"
		log.Printf("client %s authenticated in %s mode", client.ID, payload.Mode)
	} else {
		message = "invalid token"
		log.Printf("client %s authentication failed", client.ID)
	}

	// 发送认证响应
	return s.sendAuthResponse(encoder, msg.ID, success, message, client.ID)
}

// handleRegisterWorkload 处理工作负载注册
func (s *Server) handleRegisterWorkload(client *ClientConn, msg *types.Message, encoder *json.Encoder) error {
	var payload types.RegisterWorkloadPayload
	payloadBytes, err := json.Marshal(msg.Payload)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(payloadBytes, &payload); err != nil {
		return err
	}

	// 设置工作负载的注册 IP
	clientIP, _, _ := net.SplitHostPort(client.Conn.RemoteAddr().String())
	payload.Workload.Status.RegisterIP = clientIP

	// 注册工作负载到服务注册表
	if err := s.registry.RegisterWorkload(&payload.Workload); err != nil {
		return err
	}

	// 记录客户端注册的工作负载
	client.Workloads = append(client.Workloads, payload.Workload.Metadata.ID)

	// 将工作负载对应的端点添加到健康检查
	endpoints, err := s.registry.GetEndpoints("", "", payload.Workload.Metadata.Labels)
	if err == nil {
		for _, endpoint := range endpoints {
			if endpoint.IP == clientIP && endpoint.Port == payload.Workload.Spec.LocalPort {
				s.healthChecker.AddEndpoint(endpoint)
				log.Printf("added endpoint %s to health checker", endpoint.ID)
				break
			}
		}
	}

	log.Printf("registered workload %s from client %s with labels: %v",
		payload.Workload.Metadata.ID, client.ID, payload.Workload.Metadata.Labels)

	// 发送成功响应
	return s.sendGenericResponse(encoder, msg.ID, types.MsgTypeWorkloadRegistered, map[string]interface{}{
		"workload_id": payload.Workload.Metadata.ID,
		"status":      "success",
	})
}

// handleQueryService 处理服务查询
func (s *Server) handleQueryService(client *ClientConn, msg *types.Message, encoder *json.Encoder) error {
	var payload types.QueryServicePayload
	payloadBytes, err := json.Marshal(msg.Payload)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(payloadBytes, &payload); err != nil {
		return err
	}

	// 查询服务
	service, err := s.registry.GetService(payload.ServiceName, payload.ServiceNamespace)
	if err != nil {
		return err
	}

	// 获取服务的端点（基于 K8s Controller 自动发现）
	endpoints, err := s.registry.GetEndpoints(payload.ServiceName, payload.ServiceNamespace, payload.Selector)
	if err != nil {
		return err
	}

	log.Printf("client %s queried service %s/%s, found %d endpoints",
		client.ID, payload.ServiceNamespace, payload.ServiceName, len(endpoints))

	// 转换 endpoints 类型
	endpointValues := make([]types.Endpoint, len(endpoints))
	for i, ep := range endpoints {
		endpointValues[i] = *ep
	}

	// 发送服务信息响应
	return s.sendServiceInfo(encoder, msg.ID, *service, endpointValues)
}

// handleHeartbeat 处理心跳消息
func (s *Server) handleHeartbeat(client *ClientConn, msg *types.Message, encoder *json.Encoder) error {
	return s.sendGenericResponse(encoder, msg.ID, types.MsgTypePong, map[string]interface{}{
		"status":    "alive",
		"timestamp": time.Now(),
	})
}

// sendAuthResponse 发送认证响应
func (s *Server) sendAuthResponse(encoder *json.Encoder, msgID string, success bool, message string, clientID string) error {
	response := types.Message{
		Type: types.MsgTypeAuthResponse,
		ID:   msgID,
		Payload: types.AuthResponsePayload{
			Success:  success,
			Message:  message,
			ClientID: clientID,
		},
		Timestamp: time.Now(),
	}
	return encoder.Encode(response)
}

// handleNewWorkConn 处理来自Register Client的新工作连接 (服务器中转模式)
func (s *Server) handleNewWorkConn(client *ClientConn, msg *types.Message, encoder *json.Encoder) error {
	var payload types.NewWorkConnPayload
	payloadBytes, err := json.Marshal(msg.Payload)
	if err != nil {
		return fmt.Errorf("failed to marshal new work connection payload: %v", err)
	}
	if err := json.Unmarshal(payloadBytes, &payload); err != nil {
		return fmt.Errorf("failed to unmarshal new work connection payload: %v", err)
	}

	log.Printf("work connection from client %s with payload RunID: %s, WorkConnID: %s", client.ID, payload.RunID, payload.WorkConnID)

	// 工作连接需要关联到对应的注册客户端而不是自己的连接ID
	// 从 payload.RunID 获取注册客户端的ID
	registerClientID := payload.RunID
	log.Printf("associating work connection %s with register client %s (work conn client ID: %s)", payload.WorkConnID, registerClientID, client.ID)
	
	// 获取对应的工作连接池
	pool := s.workConnManager.GetOrCreateClientPool(registerClientID, 5)
	
	// 将这个连接添加到工作连接池中
	// 注意：这里的client.Conn就是新创建的工作连接
	pool.AddWorkConn(client.Conn)
	
	log.Printf("work connection %s added to pool for register client %s", payload.WorkConnID, registerClientID)
	
	// 这个连接现在切换为纯数据流模式，不再处理控制消息
	// 返回连接被接管的错误，让控制循环知道不要继续处理这个连接
	return fmt.Errorf("connection taken over by work connection pool")
}

// handleReqWorkConn 处理请求工作连接消息 (服务器中转模式)
func (s *Server) handleReqWorkConn(client *ClientConn, msg *types.Message, encoder *json.Encoder) error {
	// 解析请求负载
	var payload types.ReqWorkConnPayload
	payloadBytes, err := json.Marshal(msg.Payload)
	if err != nil {
		return fmt.Errorf("failed to marshal req work connection payload: %v", err)
	}
	if err := json.Unmarshal(payloadBytes, &payload); err != nil {
		return fmt.Errorf("failed to unmarshal req work connection payload: %v", err)
	}

	log.Printf("client %s requesting %d work connections", client.ID, payload.Count)

	// 发送请求工作连接消息到对应的注册客户端
	// 这里是服务网格的核心：代理客户端请求工作连接，服务器转发给注册客户端
	reqWorkConnMsg := types.Message{
		Type: types.MsgTypeReqWorkConn,
		ID:   uuid.New().String(),
		Payload: types.ReqWorkConnPayload{
			Count:     payload.Count,
			RequestID: msg.ID, // 保留原请求ID用于关联
		},
		Timestamp: time.Now(),
	}

	// TODO: 这里需要根据请求的服务找到对应的注册客户端
	// 目前简化实现，发送到所有注册客户端
	s.broadcastToRegisterClients(reqWorkConnMsg)

	// 发送响应表示请求已接收
	return s.sendGenericResponse(encoder, msg.ID, types.MsgTypeReqWorkConnResponse, map[string]interface{}{
		"status":  "accepted",
		"message": fmt.Sprintf("requesting %d work connections", payload.Count),
	})
}

// broadcastToRegisterClients 向所有注册客户端广播消息
func (s *Server) broadcastToRegisterClients(msg types.Message) {
	s.clientsMu.RLock()
	defer s.clientsMu.RUnlock()

	for _, client := range s.clients {
		if client.Authenticated && client.Mode == "register" {
			encoder := json.NewEncoder(client.Conn)
			if err := encoder.Encode(msg); err != nil {
				log.Printf("failed to send message to register client %s: %v", client.ID, err)
			}
		}
	}
}

// sendServiceInfo 发送服务信息响应
func (s *Server) sendServiceInfo(encoder *json.Encoder, msgID string, service types.Service, endpoints []types.Endpoint) error {
	response := types.Message{
		Type: types.MsgTypeServiceInfo,
		ID:   msgID,
		Payload: types.ServiceInfoPayload{
			Service:   service,
			Endpoints: endpoints,
		},
		Timestamp: time.Now(),
	}
	return encoder.Encode(response)
}

// sendError 发送错误响应
func (s *Server) sendError(encoder *json.Encoder, msgID string, code int, message string) error {
	response := types.Message{
		Type: types.MsgTypeError,
		ID:   msgID,
		Payload: types.ErrorPayload{
			Code:    code,
			Message: message,
		},
		Timestamp: time.Now(),
	}
	return encoder.Encode(response)
}

// sendGenericResponse 发送通用响应
func (s *Server) sendGenericResponse(encoder *json.Encoder, msgID string, msgType types.MessageType, payload interface{}) error {
	response := types.Message{
		Type:      msgType,
		ID:        msgID,
		Payload:   payload,
		Timestamp: time.Now(),
	}
	return encoder.Encode(response)
}

// handleSTCPTunnelRequest 处理 STCP 隧道请求
func (s *Server) handleSTCPTunnelRequest(client *ClientConn, msg *types.Message, encoder *json.Encoder) error {
	// 解析隧道请求
	var payload types.STCPTunnelRequestPayload
	payloadBytes, err := json.Marshal(msg.Payload)
	if err != nil {
		return s.sendError(encoder, msg.ID, 400, "Invalid tunnel request payload")
	}
	if err := json.Unmarshal(payloadBytes, &payload); err != nil {
		return s.sendError(encoder, msg.ID, 400, "Failed to parse tunnel request")
	}

	log.Printf("STCP tunnel request from client %s for service %s to %s",
		client.ID, payload.ServiceName, payload.TargetAddr)

	// 解析目标地址：必须是 namespace/serviceName 格式，进行服务发现和负载均衡
	if !strings.Contains(payload.TargetAddr, "/") {
		return s.sendError(encoder, msg.ID, 400, "Invalid target address format, STCP requires namespace/serviceName format")
	}

	// 格式：namespace/serviceName
	parts := strings.SplitN(payload.TargetAddr, "/", 2)
	if len(parts) != 2 {
		return s.sendError(encoder, msg.ID, 400, "Invalid target address format, expected namespace/serviceName")
	}
	serviceNamespace, serviceName := parts[0], parts[1]

	// 1. 获取或创建服务信息
	service, err := s.getOrCreateService(serviceName, serviceNamespace)
	if err != nil {
		return s.sendError(encoder, msg.ID, 404, fmt.Sprintf("Service not found: %s/%s", serviceNamespace, serviceName))
	}

	// 2. 获取目标端点
	endpoints, err := s.registry.GetEndpoints(serviceName, serviceNamespace, nil)
	if err != nil || len(endpoints) == 0 {
		return s.sendError(encoder, msg.ID, 503, fmt.Sprintf("No available endpoints for service: %s/%s", serviceNamespace, serviceName))
	}

	// 3. 使用负载均衡器选择目标端点（在 server 端执行负载均衡）
	targetEndpoint := s.selectEndpoint(service, endpoints, client.ID)
	if targetEndpoint == nil {
		return s.sendError(encoder, msg.ID, 503, fmt.Sprintf("No suitable endpoint available for service: %s/%s", serviceNamespace, serviceName))
	}
	realTargetAddr := fmt.Sprintf("%s:%d", targetEndpoint.IP, targetEndpoint.Port)

	log.Printf("STCP tunnel target resolved: %s/%s -> %s", serviceNamespace, serviceName, realTargetAddr)

	// 4. 建立到真实目标的连接
	targetConn, err := net.DialTimeout("tcp", realTargetAddr, 10*time.Second)
	if err != nil {
		return s.sendError(encoder, msg.ID, 502, fmt.Sprintf("Failed to connect to target: %v", err))
	}

	// 5. 发送成功响应
	tunnelID := fmt.Sprintf("tunnel-%d", time.Now().UnixNano())
	response := types.STCPTunnelResponsePayload{
		Success:  true,
		Message:  "STCP tunnel established",
		TunnelID: tunnelID,
	}

	if err := s.sendGenericResponse(encoder, msg.ID, types.MsgTypeSTCPTunnelResponse, response); err != nil {
		targetConn.Close()
		return err
	}

	log.Printf("STCP tunnel %s established: client %s <-> server <-> %s",
		tunnelID, client.ID, realTargetAddr)

	// 6. 启动数据转发（客户端连接现在变成隧道）
	// 注意：这会阻塞当前 goroutine，因为连接现在专用于隧道
	s.forwardSTCPTunnel(client.Conn, targetConn, tunnelID)

	return nil
}

// forwardSTCPTunnel 转发 STCP 隧道数据
func (s *Server) forwardSTCPTunnel(clientConn, targetConn net.Conn, tunnelID string) {
	defer targetConn.Close()
	// 注意：不要关闭 clientConn，因为它会在 handleClient 中关闭

	done := make(chan struct{}, 2)

	// 客户端 -> 目标
	go func() {
		defer func() { done <- struct{}{} }()
		_, err := io.Copy(targetConn, clientConn)
		if err != nil {
			log.Printf("STCP tunnel %s: error copying client->target: %v", tunnelID, err)
		}
	}()

	// 目标 -> 客户端
	go func() {
		defer func() { done <- struct{}{} }()
		_, err := io.Copy(clientConn, targetConn)
		if err != nil {
			log.Printf("STCP tunnel %s: error copying target->client: %v", tunnelID, err)
		}
	}()

	// 等待任一方向关闭
	<-done
	log.Printf("STCP tunnel %s closed", tunnelID)
}

// handleSUDPTunnelRequest 处理 SUDP 隧道请求
func (s *Server) handleSUDPTunnelRequest(client *ClientConn, msg *types.Message, encoder *json.Encoder) error {
	// 解析隧道请求
	var payload types.SUDPTunnelRequestPayload
	payloadBytes, err := json.Marshal(msg.Payload)
	if err != nil {
		return s.sendError(encoder, msg.ID, 400, "Invalid SUDP tunnel request payload")
	}
	if err := json.Unmarshal(payloadBytes, &payload); err != nil {
		return s.sendError(encoder, msg.ID, 400, "Failed to parse SUDP tunnel request")
	}

	log.Printf("SUDP tunnel request from client %s for service %s to %s",
		client.ID, payload.ServiceName, payload.TargetAddr)

	// 1. 获取或创建服务信息
	service, err := s.getOrCreateService(payload.ServiceName, "default")
	if err != nil {
		return s.sendError(encoder, msg.ID, 404, fmt.Sprintf("Service not found: %s", payload.ServiceName))
	}

	// 2. 获取目标端点
	endpoints, err := s.registry.GetEndpoints(payload.ServiceName, "default", nil)
	if err != nil || len(endpoints) == 0 {
		return s.sendError(encoder, msg.ID, 503, fmt.Sprintf("No available endpoints for service: %s", payload.ServiceName))
	}

	// 3. 使用负载均衡器选择目标端点
	targetEndpoint := s.selectEndpoint(service, endpoints, client.ID)
	if targetEndpoint == nil {
		return s.sendError(encoder, msg.ID, 503, fmt.Sprintf("No suitable endpoint available for service: %s", payload.ServiceName))
	}
	realTargetAddr := fmt.Sprintf("%s:%d", targetEndpoint.IP, targetEndpoint.Port)

	log.Printf("SUDP tunnel target resolved: %s -> %s", payload.TargetAddr, realTargetAddr)

	// 4. 建立到真实目标的 UDP 连接
	targetAddr, err := net.ResolveUDPAddr("udp", realTargetAddr)
	if err != nil {
		return s.sendError(encoder, msg.ID, 502, fmt.Sprintf("Failed to resolve target address: %v", err))
	}

	targetConn, err := net.DialUDP("udp", nil, targetAddr)
	if err != nil {
		return s.sendError(encoder, msg.ID, 502, fmt.Sprintf("Failed to connect to target: %v", err))
	}

	// 5. 发送成功响应
	tunnelID := fmt.Sprintf("sudp-tunnel-%d", time.Now().UnixNano())
	response := types.SUDPTunnelResponsePayload{
		Success:  true,
		Message:  "SUDP tunnel established",
		TunnelID: tunnelID,
	}

	if err := s.sendGenericResponse(encoder, msg.ID, types.MsgTypeSUDPTunnelResponse, response); err != nil {
		targetConn.Close()
		return err
	}

	log.Printf("SUDP tunnel %s established: client %s <-> server <-> %s",
		tunnelID, client.ID, realTargetAddr)

	// 6. 启动 SUDP 数据转发（客户端连接现在变成隧道）
	// 注意：这会阻塞当前 goroutine，因为连接现在专用于隧道
	s.forwardSUDPTunnel(client.Conn, targetConn, tunnelID)

	return nil
}

// forwardSUDPTunnel 转发 SUDP 隧道数据
func (s *Server) forwardSUDPTunnel(clientConn net.Conn, targetConn *net.UDPConn, tunnelID string) {
	defer targetConn.Close()
	// 注意：不要关闭 clientConn，因为它会在 handleClient 中关闭

	// 建立客户端地址映射
	clientMap := make(map[string]*net.UDPAddr)
	var clientMapMu sync.RWMutex

	done := make(chan struct{}, 2)

	// 客户端隧道 -> 目标 UDP 服务
	go func() {
		defer func() { done <- struct{}{} }()

		buffer := make([]byte, 65536)
		for {
			// 读取来自客户端隧道的数据包
			n, err := clientConn.Read(buffer)
			if err != nil {
				log.Printf("SUDP tunnel %s: error reading from client: %v", tunnelID, err)
				return
			}

			// 解析 SUDP 数据包格式：[length][client_addr_len][client_addr][data]
			if n < 8 {
				log.Printf("SUDP tunnel %s: invalid packet size %d", tunnelID, n)
				continue
			}

			// 解析数据包长度（已知，但可验证）
			expectedLen := binary.BigEndian.Uint32(buffer[0:4])
			if uint32(n-4) != expectedLen {
				log.Printf("SUDP tunnel %s: packet length mismatch", tunnelID)
				continue
			}

			// 解析客户端地址长度
			clientAddrLen := binary.BigEndian.Uint32(buffer[4:8])
			if n < int(8+clientAddrLen) {
				log.Printf("SUDP tunnel %s: invalid client address length", tunnelID)
				continue
			}

			// 解析客户端地址
			clientAddrStr := string(buffer[8 : 8+clientAddrLen])
			clientAddr, err := net.ResolveUDPAddr("udp", clientAddrStr)
			if err != nil {
				log.Printf("SUDP tunnel %s: invalid client address %s: %v", tunnelID, clientAddrStr, err)
				continue
			}

			// 缓存客户端地址映射
			clientMapMu.Lock()
			clientMap[clientAddrStr] = clientAddr
			clientMapMu.Unlock()

			// 提取实际数据
			data := buffer[8+clientAddrLen : n]

			// 转发到目标 UDP 服务
			_, err = targetConn.Write(data)
			if err != nil {
				log.Printf("SUDP tunnel %s: error writing to target: %v", tunnelID, err)
				return
			}

			log.Printf("SUDP tunnel %s: forwarded %d bytes from %s to target",
				tunnelID, len(data), clientAddrStr)
		}
	}()

	// 目标 UDP 服务 -> 客户端隧道
	go func() {
		defer func() { done <- struct{}{} }()

		buffer := make([]byte, 65536)
		for {
			// 设置读取超时
			targetConn.SetReadDeadline(time.Now().Add(30 * time.Second))

			n, err := targetConn.Read(buffer)
			if err != nil {
				if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
					continue // 超时，继续
				}
				log.Printf("SUDP tunnel %s: error reading from target: %v", tunnelID, err)
				return
			}

			// 对于 UDP 响应，我们需要发送回客户端
			// 这里简化处理，发送给最后一个客户端
			// 实际实现可能需要更复杂的客户端映射逻辑
			clientMapMu.RLock()
			var lastClientAddr string
			for addr := range clientMap {
				lastClientAddr = addr
			}
			clientMapMu.RUnlock()

			if lastClientAddr == "" {
				continue // 没有客户端映射
			}

			// 构建响应数据包格式：[length][client_addr_len][client_addr][data]
			clientAddrBytes := []byte(lastClientAddr)
			responsePacket := make([]byte, 4+4+len(clientAddrBytes)+n)

			// 写入总长度
			binary.BigEndian.PutUint32(responsePacket[0:4], uint32(len(responsePacket)-4))

			// 写入客户端地址长度
			binary.BigEndian.PutUint32(responsePacket[4:8], uint32(len(clientAddrBytes)))

			// 写入客户端地址
			copy(responsePacket[8:8+len(clientAddrBytes)], clientAddrBytes)

			// 写入数据
			copy(responsePacket[8+len(clientAddrBytes):], buffer[:n])

			// 发送回客户端隧道
			_, err = clientConn.Write(responsePacket)
			if err != nil {
				log.Printf("SUDP tunnel %s: error writing to client: %v", tunnelID, err)
				return
			}

			log.Printf("SUDP tunnel %s: forwarded %d bytes from target to %s",
				tunnelID, n, lastClientAddr)
		}
	}()

	// 等待任一方向关闭
	<-done
	log.Printf("SUDP tunnel %s closed", tunnelID)
}

// handleTunnelRequest 处理隧道请求 (新架构 - 通过 Register Client 中转)
func (s *Server) handleTunnelRequest(client *ClientConn, msg *types.Message, encoder *json.Encoder) error {
	// 解析隧道请求
	var payload types.TunnelRequestPayload
	payloadBytes, err := json.Marshal(msg.Payload)
	if err != nil {
		return s.sendError(encoder, msg.ID, 400, "Invalid tunnel request payload")
	}
	if err := json.Unmarshal(payloadBytes, &payload); err != nil {
		return s.sendError(encoder, msg.ID, 400, "Failed to parse tunnel request")
	}

	log.Printf("TCP tunnel request from proxy client %s for service %s/%s",
		client.ID, payload.ServiceNamespace, payload.ServiceName)

	// 1. 获取或创建服务信息
	service, err := s.getOrCreateService(payload.ServiceName, payload.ServiceNamespace)
	if err != nil {
		return s.sendError(encoder, msg.ID, 404, fmt.Sprintf("Service not found: %s/%s", payload.ServiceNamespace, payload.ServiceName))
	}

	// 2. 查找可用的 Register Client 工作连接
	serviceName := service.Spec.Selector["app"] // 基于 selector 获取服务名
	if serviceName == "" {
		serviceName = service.Metadata.Name // 如果 selector 中没有 app，使用服务名
	}
	registerConn := s.registerWorkConnManager.GetAvailableConnection(serviceName, "tcp")
	if registerConn == nil {
		return s.sendError(encoder, msg.ID, 503, fmt.Sprintf("No available register client for service: %s/%s", payload.ServiceNamespace, payload.ServiceName))
	}

	// 3. 找到匹配的工作负载
	var targetWorkload *types.WorkloadInfo
	for _, workload := range registerConn.Workloads {
		// 检查标签是否匹配服务的 selector
		match := true
		for key, value := range service.Spec.Selector {
			if workload.Labels[key] != value {
				match = false
				break
			}
		}
		if match && workload.Protocol == "tcp" {
			targetWorkload = &workload
			break
		}
	}

	if targetWorkload == nil {
		return s.sendError(encoder, msg.ID, 503, fmt.Sprintf("No matching workload found for service: %s/%s", payload.ServiceNamespace, payload.ServiceName))
	}

	log.Printf("Found register client %s with workload %s for service %s/%s", 
		registerConn.ClientID, targetWorkload.ID, payload.ServiceNamespace, payload.ServiceName)

	// 4. 生成会话ID
	sessionID := fmt.Sprintf("session-%d", time.Now().UnixNano())

	// 5. 获取register client的工作连接池
	pool := s.workConnManager.GetOrCreateClientPool(registerConn.ClientID, 3)
	
	// 6. 获取工作连接进行数据桥接 (服务器中转模式)
	workConn, err := pool.GetWorkConn()
	if err != nil {
		// 池中无连接，向register client的控制连接请求创建新的工作连接
		log.Printf("requesting new work connection from register client %s", registerConn.ClientID)
		
		// 向 register client 发送工作连接请求
		reqMsg := types.Message{
			Type: types.MsgTypeReqWorkConn,
			ID:   uuid.New().String(),
			Payload: types.ReqWorkConnPayload{},
			Timestamp: time.Now(),
		}
		
		// 创建编码器发送到register client的控制连接
		registerEncoder := json.NewEncoder(registerConn.Conn)
		if err := registerEncoder.Encode(reqMsg); err != nil {
			return s.sendError(encoder, msg.ID, 502, fmt.Sprintf("Failed to request work connection: %v", err))
		}
		
		// 等待工作连接创建（简化实现，实际应该有超时和异步机制）
		log.Printf("waiting for work connection to be created...")
		
		// 增加等待时间和重试机制
		for i := 0; i < 5; i++ {
			time.Sleep(500 * time.Millisecond)
			log.Printf("attempt %d: checking for work connections in pool for client %s", i+1, registerConn.ClientID)
			workConn, err = pool.GetWorkConn()
			if err == nil {
				log.Printf("work connection acquired on attempt %d", i+1)
				break
			}
			log.Printf("attempt %d failed: %v", i+1, err)
		}
		
		if err != nil {
			return s.sendError(encoder, msg.ID, 503, "No work connection available after request")
		}
	}

	// 8. 发送StartWorkConn消息给工作连接
	startMsg := types.Message{
		Type: types.MsgTypeStartWorkConn,
		ID:   uuid.New().String(),
		Payload: types.StartWorkConnPayload{
			ProxyName:  fmt.Sprintf("tcp-%s", targetWorkload.ID),
			SrcAddr:    client.Conn.RemoteAddr().String(),
			DstAddr:    fmt.Sprintf("%s:%d", targetWorkload.Address, targetWorkload.Port),
			Protocol:   "tcp",
			WorkloadID: targetWorkload.ID,
		},
		Timestamp: time.Now(),
	}
	
	workEncoder := json.NewEncoder(workConn)
	if err := workEncoder.Encode(startMsg); err != nil {
		registerConn.Available = true
		workConn.Close()
		return s.sendError(encoder, msg.ID, 502, fmt.Sprintf("Failed to start work connection: %v", err))
	}

	// 9. 发送隧道建立成功响应给 Proxy Client
	response := types.TunnelResponsePayload{
		Success:  true,
		Message:  "TCP tunnel established",
		TunnelID: sessionID,
	}

	if err := s.sendGenericResponse(encoder, msg.ID, types.MsgTypeTunnelResponse, response); err != nil {
		registerConn.Available = true
		workConn.Close()
		return err
	}

	log.Printf("TCP tunnel %s established: proxy client %s <-> work connection <-> register client %s <-> workload %s",
		sessionID, client.ID, registerConn.ClientID, targetWorkload.ID)

	// 10. 开始数据桥接 (服务器中转模式)
	go func() {
		defer func() {
			client.Conn.Close()
			workConn.Close()
			registerConn.Available = true
			log.Printf("TCP tunnel %s closed", sessionID)
		}()
		
		// 使用io.Copy进行双向数据桥接
		done := make(chan struct{}, 2)
		
		// proxy client -> work connection
		go func() {
			defer func() { done <- struct{}{} }()
			io.Copy(workConn, client.Conn)
		}()
		
		// work connection -> proxy client  
		go func() {
			defer func() { done <- struct{}{} }()
			io.Copy(client.Conn, workConn)
		}()
		
		// 等待任一方向完成
		<-done
	}()
	
	// 返回特殊错误，表示连接已被数据桥接接管
	return fmt.Errorf("connection taken over by data bridge %s", sessionID)
}

// handleProxyData 处理代理数据消息 (已废弃，使用服务器中转工作连接模式)
func (s *Server) handleProxyData(client *ClientConn, msg *types.Message, encoder *json.Encoder) error {
	return s.sendError(encoder, msg.ID, 501, "ProxyData messages not supported in work connection mode")
}

// handleUDPTunnelRequest 处理 UDP 隧道请求 (新架构 - 通过 Register Client 中转)
func (s *Server) handleUDPTunnelRequest(client *ClientConn, msg *types.Message, encoder *json.Encoder) error {
	// 解析 UDP 隧道请求
	var payload types.UDPTunnelRequestPayload
	payloadBytes, err := json.Marshal(msg.Payload)
	if err != nil {
		return s.sendError(encoder, msg.ID, 400, "Invalid UDP tunnel request payload")
	}
	if err := json.Unmarshal(payloadBytes, &payload); err != nil {
		return s.sendError(encoder, msg.ID, 400, "Failed to parse UDP tunnel request")
	}

	log.Printf("UDP tunnel request from proxy client %s for service %s/%s, client addr %s",
		client.ID, payload.ServiceNamespace, payload.ServiceName, payload.ClientAddr)

	// 1. 获取或创建服务信息
	service, err := s.getOrCreateService(payload.ServiceName, payload.ServiceNamespace)
	if err != nil {
		return s.sendError(encoder, msg.ID, 404, fmt.Sprintf("Service not found: %s/%s", payload.ServiceNamespace, payload.ServiceName))
	}

	// 2. 查找可用的 Register Client 工作连接 (支持 UDP 的)
	serviceName := service.Spec.Selector["app"]
	if serviceName == "" {
		serviceName = service.Metadata.Name
	}
	registerConn := s.registerWorkConnManager.GetAvailableConnection(serviceName, "udp")
	if registerConn == nil {
		return s.sendError(encoder, msg.ID, 503, fmt.Sprintf("No available UDP register client for service: %s/%s", payload.ServiceNamespace, payload.ServiceName))
	}

	// 3. 找到匹配的 UDP 工作负载
	var targetWorkload *types.WorkloadInfo
	for _, workload := range registerConn.Workloads {
		// 检查标签是否匹配服务的 selector 并且协议是 UDP
		match := true
		for key, value := range service.Spec.Selector {
			if workload.Labels[key] != value {
				match = false
				break
			}
		}
		if match && workload.Protocol == "udp" {
			targetWorkload = &workload
			break
		}
	}

	if targetWorkload == nil {
		return s.sendError(encoder, msg.ID, 503, fmt.Sprintf("No matching UDP workload found for service: %s/%s", payload.ServiceNamespace, payload.ServiceName))
	}

	log.Printf("Found register client %s with UDP workload %s for service %s/%s", 
		registerConn.ClientID, targetWorkload.ID, payload.ServiceNamespace, payload.ServiceName)

	// 4. UDP tunneling using work connections (similar to TCP but needs UDP-specific implementation)
	// For now, return error indicating UDP work connection support needs implementation
	return s.sendError(encoder, msg.ID, 501, "UDP tunneling with work connections not yet implemented")
}

// forwardTCPTunnelThroughRegisterClient 通过 Register Client 转发 TCP 隧道数据 (新架构)
func (s *Server) forwardTCPTunnelThroughRegisterClient(proxyClientConn, registerClientConn net.Conn, tunnelID string, registerConn *RegisterWorkConn) {
	defer func() {
		// 清理：恢复 Register Client 工作连接的可用状态
		registerConn.Available = true
		log.Printf("TCP tunnel %s closed, register client %s restored to available state", tunnelID, registerConn.ClientID)
	}()

	// 注意：不要关闭连接，因为它们会在各自的 handleClient 中关闭

	done := make(chan struct{}, 2)

	// Proxy Client -> Register Client (通过 Server 转发)
	go func() {
		defer func() { done <- struct{}{} }()
		_, err := io.Copy(registerClientConn, proxyClientConn)
		if err != nil {
			log.Printf("TCP tunnel %s: error copying proxy client->register client: %v", tunnelID, err)
		} else {
			log.Printf("TCP tunnel %s: proxy client->register client stream closed normally", tunnelID)
		}
	}()

	// Register Client -> Proxy Client (通过 Server 转发)
	go func() {
		defer func() { done <- struct{}{} }()
		_, err := io.Copy(proxyClientConn, registerClientConn)
		if err != nil {
			log.Printf("TCP tunnel %s: error copying register client->proxy client: %v", tunnelID, err)
		} else {
			log.Printf("TCP tunnel %s: register client->proxy client stream closed normally", tunnelID)
		}
	}()

	// 等待任一方向的数据传输结束
	<-done
	log.Printf("TCP tunnel %s closed (data relay through register client %s)", tunnelID, registerConn.ClientID)
}

// forwardTCPTunnel 转发 TCP 隧道数据
func (s *Server) forwardTCPTunnel(clientConn, targetConn net.Conn, tunnelID string) {
	defer targetConn.Close()
	// 注意：不要关闭 clientConn，因为它会在 handleClient 中关闭

	done := make(chan struct{}, 2)

	// 客户端 -> 目标
	go func() {
		defer func() { done <- struct{}{} }()
		_, err := io.Copy(targetConn, clientConn)
		if err != nil {
			log.Printf("TCP tunnel %s: error copying client->target: %v", tunnelID, err)
		}
	}()

	// 目标 -> 客户端
	go func() {
		defer func() { done <- struct{}{} }()
		_, err := io.Copy(clientConn, targetConn)
		if err != nil {
			log.Printf("TCP tunnel %s: error copying target->client: %v", tunnelID, err)
		}
	}()

	// 等待任一方向关闭
	<-done
	log.Printf("TCP tunnel %s closed", tunnelID)
}

// handleWorkConnRequest 处理工作连接请求
func (s *Server) handleWorkConnRequest(client *ClientConn, msg *types.Message, encoder *json.Encoder) error {
	var payload types.WorkConnRequestPayload
	payloadBytes, err := json.Marshal(msg.Payload)
	if err != nil {
		return s.sendError(encoder, msg.ID, 400, "Invalid work connection request payload")
	}
	if err := json.Unmarshal(payloadBytes, &payload); err != nil {
		return s.sendError(encoder, msg.ID, 400, "Failed to parse work connection request")
	}

	log.Printf("work connection request from client %s for service %s/%s",
		client.ID, payload.ServiceNamespace, payload.ServiceName)

	// 1. 获取或创建服务信息
	service, err := s.getOrCreateService(payload.ServiceName, payload.ServiceNamespace)
	if err != nil {
		return s.sendError(encoder, msg.ID, 404, fmt.Sprintf("Service not found: %s/%s", payload.ServiceNamespace, payload.ServiceName))
	}

	// 2. 获取目标端点
	endpoints, err := s.registry.GetEndpoints(payload.ServiceName, payload.ServiceNamespace, nil)
	if err != nil || len(endpoints) == 0 {
		return s.sendError(encoder, msg.ID, 503, fmt.Sprintf("No available endpoints for service: %s/%s", payload.ServiceNamespace, payload.ServiceName))
	}

	// 3. 使用负载均衡器选择目标端点（在 server 端执行负载均衡）
	targetEndpoint := s.selectEndpoint(service, endpoints, client.ID)
	if targetEndpoint == nil {
		return s.sendError(encoder, msg.ID, 503, fmt.Sprintf("No suitable endpoint available for service: %s/%s", payload.ServiceNamespace, payload.ServiceName))
	}

	// 4. 生成工作连接ID
	workConnID := fmt.Sprintf("work-%s-%d", client.ID, time.Now().UnixNano())

	// 5. 发送成功响应
	response := types.WorkConnResponsePayload{
		Success:    true,
		Message:    "Work connection request accepted",
		WorkConnID: workConnID,
	}

	if err := s.sendGenericResponse(encoder, msg.ID, types.MsgTypeWorkConnResponse, response); err != nil {
		return err
	}

	// 6. 预注册工作连接信息（等待实际连接）
	workConn := &WorkConn{
		ID:               workConnID,
		ServiceName:      payload.ServiceName,
		ServiceNamespace: payload.ServiceNamespace,
		TargetEndpoint:   targetEndpoint,
		CreatedAt:        time.Now(),
	}
	s.workConnManager.AddWorkConn(workConn)

	log.Printf("work connection %s prepared for service %s/%s -> %s:%d",
		workConnID, payload.ServiceNamespace, payload.ServiceName, targetEndpoint.IP, targetEndpoint.Port)

	return nil
}

// handleWorkConnID 处理工作连接标识
func (s *Server) handleWorkConnID(client *ClientConn, msg *types.Message, encoder *json.Encoder) error {
	var payload types.WorkConnIDPayload
	payloadBytes, err := json.Marshal(msg.Payload)
	if err != nil {
		return fmt.Errorf("invalid work connection ID payload: %v", err)
	}
	if err := json.Unmarshal(payloadBytes, &payload); err != nil {
		return fmt.Errorf("failed to parse work connection ID: %v", err)
	}

	// 获取预注册的工作连接信息
	workConn, exists := s.workConnManager.GetWorkConn(payload.WorkConnID)
	if !exists {
		return fmt.Errorf("work connection %s not found", payload.WorkConnID)
	}

	// 设置实际的连接
	workConn.Conn = client.Conn

	log.Printf("work connection %s established for service %s/%s",
		payload.WorkConnID, workConn.ServiceNamespace, workConn.ServiceName)

	// 建立到目标端点的连接
	targetAddr := fmt.Sprintf("%s:%d", workConn.TargetEndpoint.IP, workConn.TargetEndpoint.Port)
	targetConn, err := net.DialTimeout("tcp", targetAddr, 10*time.Second)
	if err != nil {
		s.workConnManager.RemoveWorkConn(payload.WorkConnID)
		return fmt.Errorf("failed to connect to target %s: %v", targetAddr, err)
	}

	log.Printf("work connection %s connected to target %s", payload.WorkConnID, targetAddr)

	// 启动数据转发（这会阻塞当前 goroutine）
	s.forwardWorkConnection(workConn.Conn, targetConn, payload.WorkConnID)

	// 清理工作连接
	s.workConnManager.RemoveWorkConn(payload.WorkConnID)
	return nil
}

// forwardWorkConnection 转发工作连接数据
func (s *Server) forwardWorkConnection(clientConn, targetConn net.Conn, workConnID string) {
	defer targetConn.Close()
	// 注意：不要关闭 clientConn，因为它会在上层处理

	done := make(chan struct{}, 2)

	// 客户端 -> 目标
	go func() {
		defer func() { done <- struct{}{} }()
		_, err := io.Copy(targetConn, clientConn)
		if err != nil {
			log.Printf("work connection %s: error copying client->target: %v", workConnID, err)
		}
	}()

	// 目标 -> 客户端
	go func() {
		defer func() { done <- struct{}{} }()
		_, err := io.Copy(clientConn, targetConn)
		if err != nil {
			log.Printf("work connection %s: error copying target->client: %v", workConnID, err)
		}
	}()

	// 等待任一方向关闭
	<-done
	log.Printf("work connection %s closed", workConnID)
}

// handleCreateService 处理创建服务请求
func (s *Server) handleCreateService(client *ClientConn, msg *types.Message, encoder *json.Encoder) error {
	var payload types.CreateServicePayload
	payloadBytes, err := json.Marshal(msg.Payload)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(payloadBytes, &payload); err != nil {
		return err
	}

	log.Printf("client %s requested to create service %s/%s",
		client.ID, payload.Service.Metadata.Namespace, payload.Service.Metadata.Name)

	// 检查服务是否已存在
	existingService, err := s.registry.GetService(payload.Service.Metadata.Name, payload.Service.Metadata.Namespace)
	if err == nil && existingService != nil {
		// 服务已存在，返回成功
		return s.sendGenericResponse(encoder, msg.ID, types.MsgTypeServiceCreated, map[string]interface{}{
			"service_name":      payload.Service.Metadata.Name,
			"service_namespace": payload.Service.Metadata.Namespace,
			"status":            "already_exists",
			"message":           "Service already exists",
		})
	}

	// 查找匹配selector的workloads，智能推断端口
	workloads, err := s.registry.ListWorkloads(payload.Service.Spec.Selector)
	if err == nil && len(workloads) > 0 {
		// 使用第一个匹配的workload的端口作为目标端口
		targetPort := workloads[0].Spec.LocalPort
		if len(payload.Service.Spec.Ports) > 0 {
			payload.Service.Spec.Ports[0].TargetPort = targetPort
		}
		log.Printf("inferred target port %d from existing workload %s", targetPort, workloads[0].Metadata.ID)
	}

	// 创建服务
	if err := s.registry.RegisterService(&payload.Service); err != nil {
		return s.sendError(encoder, msg.ID, 500, fmt.Sprintf("Failed to create service: %v", err))
	}

	log.Printf("created service %s/%s successfully",
		payload.Service.Metadata.Namespace, payload.Service.Metadata.Name)

	// 发送成功响应
	return s.sendGenericResponse(encoder, msg.ID, types.MsgTypeServiceCreated, map[string]interface{}{
		"service_name":      payload.Service.Metadata.Name,
		"service_namespace": payload.Service.Metadata.Namespace,
		"status":            "created",
		"message":           "Service created successfully",
	})
}

// handleProxyResponse 处理来自 Register Client 的代理响应
func (s *Server) handleProxyResponse(client *ClientConn, msg *types.Message, encoder *json.Encoder) error {
	log.Printf("received proxy response from register client %s: %s", client.ID, msg.ID)
	
	// 解析代理响应
	var payload types.ProxyResponsePayload
	payloadBytes, err := json.Marshal(msg.Payload)
	if err != nil {
		log.Printf("failed to marshal proxy response payload: %v", err)
		return nil // 不返回错误，避免关闭连接
	}
	if err := json.Unmarshal(payloadBytes, &payload); err != nil {
		log.Printf("failed to unmarshal proxy response payload: %v", err)
		return nil // 不返回错误，避免关闭连接
	}

	if payload.Success {
		log.Printf("register client %s successfully established connection for session %s", 
			client.ID, payload.SessionID)
	} else {
		log.Printf("register client %s failed to establish connection for session %s: %s", 
			client.ID, payload.SessionID, payload.Message)
	}

	// TODO: 这里应该通知等待的隧道建立过程
	// 当前的实现有架构问题：在 handleTunnelRequest 中同步等待响应
	// 而这里异步接收响应，需要重构为基于回调或通道的架构
	
	return nil
}
