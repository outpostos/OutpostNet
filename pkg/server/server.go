package server

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
	"github.com/outpostos/OutpostNet/pkg/healthcheck"
	"github.com/outpostos/OutpostNet/pkg/listener"
	"github.com/outpostos/OutpostNet/pkg/loadbalancer"
	"github.com/outpostos/OutpostNet/pkg/registry"
	"github.com/outpostos/OutpostNet/pkg/types"
)

// Server outpostnet 服务器
type Server struct {
	config   *config.ServerConfig
	multiListener *listener.MultiProtocolListener // 多协议监听器
	registry types.ServiceRegistry

	// 客户端连接管理
	clients   map[string]*ClientConn
	clientsMu sync.RWMutex

	// 负载均衡管理
	lbManager *loadbalancer.LoadBalancerManager

	// 健康检查管理
	healthChecker *healthcheck.HealthCheckManager

	// 连接跟踪器
	connTracker *listener.ConnectionTracker

	// 控制通道
	ctx    context.Context
	cancel context.CancelFunc
}

// ClientConn 表示一个客户端连接
type ClientConn struct {
	ID           string
	Conn         net.Conn
	Mode         string // proxy 或 register
	Authenticated bool
	Workloads    []string // register 模式下注册的工作负载 ID
	LastSeen     time.Time
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
		config:        cfg,
		multiListener: multiListener,
		registry:      registry,
		clients:       make(map[string]*ClientConn),
		lbManager:     loadbalancer.NewLoadBalancerManager(),
		healthChecker: healthcheck.NewHealthCheckManager(),
		connTracker:   listener.NewConnectionTracker(),
	}

	return s, nil
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
	defer connInfo.Conn.Close()
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
	s.handleClientMessages(client)
}

// handleClientMessages 处理客户端消息
func (s *Server) handleClientMessages(client *ClientConn) {
	decoder := json.NewDecoder(client.Conn)
	encoder := json.NewEncoder(client.Conn)
	
	for {
		select {
		case <-s.ctx.Done():
			return
		default:
			var msg types.Message
			if err := decoder.Decode(&msg); err != nil {
				log.Printf("failed to decode message from client %s: %v", client.ID, err)
				return
			}
			
			client.LastSeen = time.Now()
			
			// 处理不同类型的消息
			if err := s.handleMessage(client, &msg, encoder); err != nil {
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
	return s.sendGenericResponse(encoder, msg.ID, "workload_registered", map[string]interface{}{
		"workload_id": payload.Workload.Metadata.ID,
		"status": "success",
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
		"status": "alive",
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
func (s *Server) sendGenericResponse(encoder *json.Encoder, msgID string, msgType string, payload interface{}) error {
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

	// 1. 获取服务信息
	service, err := s.registry.GetService(payload.ServiceName, "default")
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

	log.Printf("STCP tunnel target resolved: %s -> %s", payload.TargetAddr, realTargetAddr)

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

	// 1. 获取服务信息
	service, err := s.registry.GetService(payload.ServiceName, "default")
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