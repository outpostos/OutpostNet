package client

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
	"github.com/outpostos/OutpostNet/pkg/types"
)

// Client outpostnet 客户端
type Client struct {
	config *config.ClientConfig
	conn   net.Conn

	// 代理管理
	proxies   map[string]*ProxyInstance
	proxiesMu sync.RWMutex



	// 地址解析器
	addressResolver *config.AddressResolver

	// 消息处理
	encoder       *json.Encoder
	decoder       *json.Decoder
	authenticated bool
	clientID      string
	currentSessionID string // 当前数据会话ID

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
		addressResolver: config.NewAddressResolver("tcp"),
	}

	return c, nil
}

// requestWorkConnection 向 server 请求工作连接
func (c *Client) requestWorkConnection(serviceName, serviceNamespace string) (net.Conn, error) {
	// 发送工作连接请求
	workConnMsg := types.Message{
		Type: types.MsgTypeWorkConnRequest,
		ID:   uuid.New().String(),
		Payload: types.WorkConnRequestPayload{
			ServiceName:      serviceName,
			ServiceNamespace: serviceNamespace,
		},
		Timestamp: time.Now(),
	}

	if err := c.encoder.Encode(workConnMsg); err != nil {
		return nil, fmt.Errorf("failed to send work connection request: %v", err)
	}

	var response types.Message
	if err := c.decoder.Decode(&response); err != nil {
		return nil, fmt.Errorf("failed to receive work connection response: %v", err)
	}

	if response.Type != types.MsgTypeWorkConnResponse {
		return nil, fmt.Errorf("work connection request failed: %s", response.Type)
	}

	var workConnResponse types.WorkConnResponsePayload
	payloadBytes, err := json.Marshal(response.Payload)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal work connection response: %v", err)
	}
	if err := json.Unmarshal(payloadBytes, &workConnResponse); err != nil {
		return nil, fmt.Errorf("failed to unmarshal work connection response: %v", err)
	}

	if !workConnResponse.Success {
		return nil, fmt.Errorf("work connection request failed: %s", workConnResponse.Message)
	}

	// 建立新的连接作为工作连接
	workConn, err := net.DialTimeout("tcp", c.config.Client.ServerAddr, 10*time.Second)
	if err != nil {
		return nil, fmt.Errorf("failed to establish work connection: %v", err)
	}

	// 发送工作连接标识
	workConnID := types.Message{
		Type: types.MsgTypeWorkConnID,
		ID:   workConnResponse.WorkConnID,
		Payload: types.WorkConnIDPayload{
			WorkConnID: workConnResponse.WorkConnID,
		},
		Timestamp: time.Now(),
	}

	encoder := json.NewEncoder(workConn)
	if err := encoder.Encode(workConnID); err != nil {
		workConn.Close()
		return nil, fmt.Errorf("failed to send work connection ID: %v", err)
	}

	log.Printf("work connection %s established for service %s/%s", workConnResponse.WorkConnID, serviceNamespace, serviceName)
	return workConn, nil
}

// requestTunnelConnection 向 server 请求隧道连接 (新架构)
func (c *Client) requestTunnelConnection(serviceName, serviceNamespace string) (net.Conn, error) {
	// 发送隧道请求
	tunnelMsg := types.Message{
		Type: types.MsgTypeTunnelRequest,
		ID:   uuid.New().String(),
		Payload: types.TunnelRequestPayload{
			ServiceName:      serviceName,
			ServiceNamespace: serviceNamespace,
		},
		Timestamp: time.Now(),
	}

	if err := c.encoder.Encode(tunnelMsg); err != nil {
		return nil, fmt.Errorf("failed to send tunnel request: %v", err)
	}

	var response types.Message
	if err := c.decoder.Decode(&response); err != nil {
		return nil, fmt.Errorf("failed to receive tunnel response: %v", err)
	}

	if response.Type == types.MsgTypeError {
		return nil, fmt.Errorf("tunnel request failed: %v", response.Payload)
	}

	if response.Type != types.MsgTypeTunnelResponse {
		return nil, fmt.Errorf("unexpected tunnel response type: %s", response.Type)
	}

	var tunnelResponse types.TunnelResponsePayload
	payloadBytes, err := json.Marshal(response.Payload)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal tunnel response: %v", err)
	}
	if err := json.Unmarshal(payloadBytes, &tunnelResponse); err != nil {
		return nil, fmt.Errorf("failed to unmarshal tunnel response: %v", err)
	}

	if !tunnelResponse.Success {
		return nil, fmt.Errorf("tunnel request failed: %s", tunnelResponse.Message)
	}

	log.Printf("data session %s established for service %s/%s", 
		tunnelResponse.TunnelID, serviceNamespace, serviceName)
	
	// 新架构：不再需要连接专用隧道端口
	// 但需要记录session ID用于数据消息
	c.currentSessionID = tunnelResponse.TunnelID
	log.Printf("using control connection for data transfer in session %s", tunnelResponse.TunnelID)
	return c.conn, nil
}

// requestUDPTunnel 向 server 请求 UDP 隧道连接 (新架构)
func (c *Client) requestUDPTunnel(serviceName, serviceNamespace, clientAddr string) (*net.UDPConn, error) {
	// 发送 UDP 隧道请求
	tunnelMsg := types.Message{
		Type: types.MsgTypeUDPTunnelRequest,
		ID:   uuid.New().String(),
		Payload: types.UDPTunnelRequestPayload{
			ServiceName:      serviceName,
			ServiceNamespace: serviceNamespace,
			ClientAddr:       clientAddr,
		},
		Timestamp: time.Now(),
	}

	if err := c.encoder.Encode(tunnelMsg); err != nil {
		return nil, fmt.Errorf("failed to send UDP tunnel request: %v", err)
	}

	var response types.Message
	if err := c.decoder.Decode(&response); err != nil {
		return nil, fmt.Errorf("failed to receive UDP tunnel response: %v", err)
	}

	if response.Type == types.MsgTypeError {
		return nil, fmt.Errorf("UDP tunnel request failed: %v", response.Payload)
	}

	if response.Type != types.MsgTypeUDPTunnelResponse {
		return nil, fmt.Errorf("unexpected UDP tunnel response type: %s", response.Type)
	}

	var tunnelResponse types.UDPTunnelResponsePayload
	payloadBytes, err := json.Marshal(response.Payload)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal UDP tunnel response: %v", err)
	}
	if err := json.Unmarshal(payloadBytes, &tunnelResponse); err != nil {
		return nil, fmt.Errorf("failed to unmarshal UDP tunnel response: %v", err)
	}

	if !tunnelResponse.Success {
		return nil, fmt.Errorf("UDP tunnel request failed: %s", tunnelResponse.Message)
	}

	log.Printf("UDP tunnel %s established for service %s/%s, client %s", 
		tunnelResponse.TunnelID, serviceNamespace, serviceName, clientAddr)
	
	// 对于 UDP 隧道，我们需要建立一个 UDP 连接用于数据传输
	// 但 UDP 隧道的数据传输需要通过特殊的协议进行包装
	// 为了简化实现，我们创建一个本地 UDP 连接用于数据传输
	udpConn, err := net.DialUDP("udp", nil, &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 0})
	if err != nil {
		return nil, fmt.Errorf("failed to create UDP connection for tunnel: %v", err)
	}
	
	// TODO: 实现通过控制连接转发 UDP 数据包的逻辑
	// 目前先返回本地 UDP 连接，后续需要实现数据包包装和转发
	return udpConn, nil
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

	// 注册完成后，建立工作连接池来处理来自 Server 的代理请求
	go c.startWorkConnectionPool()

	// 启动控制连接消息处理器，监听来自Server的工作连接请求 (服务器中转模式)
	go c.startControlMessageHandler()

	// 创建初始工作连接池 (服务器中转模式)
	go c.createInitialWorkConnections()

	return nil
}

// startWorkConnectionPool 启动工作连接池 (Register Client 专用)
// 这是修复架构的关键：Register Client 主动建立到 Server 的工作连接
func (c *Client) startWorkConnectionPool() {
	log.Println("starting work connection pool for register mode")
	
	// 初始工作连接数
	initialConnections := 3
	
	for i := 0; i < initialConnections; i++ {
		go c.createWorkConnection(i)
	}
	
	// 监控工作连接状态，必要时重新建立连接
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	
	for {
		select {
		case <-c.ctx.Done():
			return
		case <-ticker.C:
			// TODO: 实现工作连接健康检查和重连逻辑
			log.Println("work connection pool health check")
		}
	}
}

// createWorkConnection 创建单个工作连接
func (c *Client) createWorkConnection(id int) {
	for {
		select {
		case <-c.ctx.Done():
			return
		default:
			if err := c.handleWorkConnection(id); err != nil {
				log.Printf("work connection %d error: %v, retrying in 5s", id, err)
				time.Sleep(5 * time.Second)
			}
		}
	}
}

// handleWorkConnection 处理单个工作连接的生命周期 (服务器中转模式)
func (c *Client) handleWorkConnection(id int) error {
	// 工作连接池保持连接活跃，等待server请求工作连接
	for {
		select {
		case <-c.ctx.Done():
			return nil
		default:
			// 工作连接在控制连接中等待server的工作连接请求
			time.Sleep(100 * time.Millisecond)
		}
	}
}

// handleReqWorkConn 处理来自Server的工作连接请求 (服务器中转模式)
func (c *Client) handleReqWorkConn(msg types.Message) {
	// 解析ReqWorkConn消息
	var reqPayload types.ReqWorkConnPayload
	payloadBytes, err := json.Marshal(msg.Payload)
	if err != nil {
		log.Printf("failed to marshal req work connection payload: %v", err)
		return
	}
	if err := json.Unmarshal(payloadBytes, &reqPayload); err != nil {
		log.Printf("failed to unmarshal req work connection payload: %v", err)
		return
	}

	log.Printf("server requested %d work connections", reqPayload.Count)
	
	// Server请求创建指定数量的工作连接
	for i := 0; i < reqPayload.Count; i++ {
		go c.createNewWorkConn()
	}
}

// createNewWorkConn 创建新的工作连接 (服务器中转模式)
func (c *Client) createNewWorkConn() {
	// 建立到Server的新工作连接
	workConn, err := net.DialTimeout("tcp", c.config.Client.ServerAddr, 10*time.Second)
	if err != nil {
		log.Printf("failed to establish work connection: %v", err)
		return
	}

	log.Printf("work connection established to %s", c.config.Client.ServerAddr)

	// 发送NewWorkConn消息到Server
	encoder := json.NewEncoder(workConn)
	decoder := json.NewDecoder(workConn)

	// 发送工作连接标识
	workConnID := uuid.New().String()
	newWorkConnMsg := types.Message{
		Type: types.MsgTypeNewWorkConn,
		ID:   workConnID,
		Payload: types.NewWorkConnPayload{
			RunID:      c.clientID,
			WorkConnID: workConnID,
		},
		Timestamp: time.Now(),
	}

	if err := encoder.Encode(newWorkConnMsg); err != nil {
		log.Printf("failed to send new work connection message: %v", err)
		workConn.Close()
		return
	}

	log.Printf("work connection %s waiting for start signal", workConnID)

	// 等待Server的StartWorkConn指令
	var startMsg types.Message
	if err := decoder.Decode(&startMsg); err != nil {
		log.Printf("failed to receive start work connection message: %v", err)
		workConn.Close()
		return
	}

	if startMsg.Type != types.MsgTypeStartWorkConn {
		log.Printf("unexpected message type: %s, expected start_work_conn", startMsg.Type)
		workConn.Close()
		return
	}

	// 解析StartWorkConn消息
	var startPayload types.StartWorkConnPayload
	payloadBytes, err := json.Marshal(startMsg.Payload)
	if err != nil {
		log.Printf("failed to marshal start work connection payload: %v", err)
		workConn.Close()
		return
	}
	if err := json.Unmarshal(payloadBytes, &startPayload); err != nil {
		log.Printf("failed to unmarshal start work connection payload: %v", err)
		workConn.Close()
		return
	}

	log.Printf("work connection %s starting data bridge for workload %s", workConnID, startPayload.WorkloadID)

	// 根据WorkloadID找到对应的本地服务
	var matchedWorkload *config.ClientWorkload
	for i, workload := range c.config.Workloads {
		if workload.Metadata.ID == startPayload.WorkloadID {
			matchedWorkload = &c.config.Workloads[i]
			break
		}
	}

	if matchedWorkload == nil {
		log.Printf("no matching workload found for ID: %s", startPayload.WorkloadID)
		workConn.Close()
		return
	}

	// 建立到本地后端服务的连接
	backendAddr := fmt.Sprintf("%s:%d", matchedWorkload.Spec.LocalAddr, matchedWorkload.Spec.LocalPort)
	backendConn, err := net.DialTimeout(startPayload.Protocol, backendAddr, 10*time.Second)
	if err != nil {
		log.Printf("failed to connect to backend %s: %v", backendAddr, err)
		workConn.Close()
		return
	}

	log.Printf("work connection %s established backend connection to %s", workConnID, backendAddr)

	// 开始数据桥接 (服务器中转模式)
	defer func() {
		workConn.Close()
		backendConn.Close()
		log.Printf("work connection %s data bridge closed", workConnID)
	}()

	// 使用io.Copy进行双向数据桥接
	done := make(chan struct{}, 2)

	// work connection -> backend
	go func() {
		defer func() { done <- struct{}{} }()
		io.Copy(backendConn, workConn)
	}()

	// backend -> work connection
	go func() {
		defer func() { done <- struct{}{} }()
		io.Copy(workConn, backendConn)
	}()

	// 等待任一方向完成
	<-done
}

// createInitialWorkConnections 创建初始工作连接池 (服务器中转模式)
func (c *Client) createInitialWorkConnections() {
	// 等待一小段时间确保工作负载注册完成
	time.Sleep(1 * time.Second)
	
	// 发送工作连接就绪消息 (为了兼容现有的RegisterWorkConnManager)
	c.sendWorkConnReady()
	
	// 创建3个初始工作连接
	for i := 0; i < 3; i++ {
		go c.createNewWorkConn()
		time.Sleep(100 * time.Millisecond) // 避免同时创建太多连接
	}
	
	log.Printf("created initial work connections for register client %s", c.clientID)
}

// sendWorkConnReady 发送工作连接就绪消息 (兼容性)
func (c *Client) sendWorkConnReady() {
	readyMsg := types.Message{
		Type: types.MsgTypeWorkConnReady,
		ID:   uuid.New().String(),
		Payload: types.WorkConnReadyPayload{
			ConnectionID: fmt.Sprintf("work-conn-ready-%s", c.clientID),
			Workloads:    c.getRegisteredWorkloads(),
		},
		Timestamp: time.Now(),
	}

	if err := c.encoder.Encode(readyMsg); err != nil {
		log.Printf("failed to send work connection ready message: %v", err)
	} else {
		log.Printf("sent work connection ready message for client %s", c.clientID)
	}
}

// startControlMessageHandler 启动控制连接消息处理器 (服务器中转模式)
func (c *Client) startControlMessageHandler() {
	log.Println("starting control message handler for register mode")

	for {
		select {
		case <-c.ctx.Done():
			return
		default:
			var msg types.Message
			if err := c.decoder.Decode(&msg); err != nil {
				if err != io.EOF {
					log.Printf("error decoding control message: %v, reconnecting...", err)
					// 尝试重连
					time.Sleep(5 * time.Second)
					c.reconnect()
				}
				return
			}

			// 处理控制消息 (服务器中转模式)
			switch msg.Type {
			case types.MsgTypeReqWorkConn:
				// Server请求创建工作连接 (服务器中转模式)
				log.Printf("received work connection request from server")
				c.handleReqWorkConn(msg)
			case types.MsgTypeHeartbeat:
				// 响应心跳
				c.sendHeartbeatResponse(c.encoder, msg.ID)
			case types.MsgTypePong:
				// 心跳响应，忽略
				log.Printf("received pong from server")
			default:
				log.Printf("received unexpected control message type: %s", msg.Type)
			}
		}
	}
}

// getRegisteredWorkloads 获取注册的工作负载信息
func (c *Client) getRegisteredWorkloads() []types.WorkloadInfo {
	var workloads []types.WorkloadInfo
	for _, workload := range c.config.Workloads {
		workloads = append(workloads, types.WorkloadInfo{
			ID:       workload.Metadata.ID,
			Labels:   workload.Metadata.Labels,
			Protocol: workload.Spec.Protocol,
			Address:  workload.Spec.LocalAddr,
			Port:     workload.Spec.LocalPort,
		})
	}
	return workloads
}

// handleProxyRequestWithDecoder 处理来自 Server 的代理请求（已废弃，使用服务器中转工作连接模式）
func (c *Client) handleProxyRequestWithDecoder(workConn net.Conn, msg types.Message, encoder *json.Encoder, decoder *json.Decoder) {
	log.Printf("handleProxyRequestWithDecoder called but is deprecated in server relay mode")
}

// handleDataForwardingWithDecoder 处理数据转发 (已废弃，使用服务器中转工作连接模式)
func (c *Client) handleDataForwardingWithDecoder(workConn net.Conn, backendConn net.Conn, sessionID string, encoder *json.Encoder, decoder *json.Decoder) {
	log.Printf("handleDataForwardingWithDecoder called but is deprecated in server relay mode")
}

// sendHeartbeatResponse 发送心跳响应
func (c *Client) sendHeartbeatResponse(encoder *json.Encoder, requestID string) {
	response := types.Message{
		Type: types.MsgTypeHeartbeatResponse,
		ID:   requestID,
		Payload: types.HeartbeatPayload{
			Timestamp: time.Now(),
			ClientID:  c.clientID,
		},
		Timestamp: time.Now(),
	}

	if err := encoder.Encode(response); err != nil {
		log.Printf("failed to send heartbeat response: %v", err)
	}
}

// sendResponse 发送通用响应
func (c *Client) sendResponse(encoder *json.Encoder, requestID string, msgType types.MessageType, payload interface{}) error {
	response := types.Message{
		Type:      msgType,
		ID:        requestID,
		Payload:   payload,
		Timestamp: time.Now(),
	}
	return encoder.Encode(response)
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
	log.Printf("starting UDP proxy %s on port %d", proxyConfig.Name, proxyConfig.LocalPort)

	// 监听本地 UDP 端口
	addr, err := net.ResolveUDPAddr("udp", fmt.Sprintf(":%d", proxyConfig.LocalPort))
	if err != nil {
		return fmt.Errorf("failed to resolve UDP address: %v", err)
	}

	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		return fmt.Errorf("failed to listen on UDP port %d: %v", proxyConfig.LocalPort, err)
	}

	log.Printf("UDP proxy %s successfully bound to port %d", proxyConfig.Name, proxyConfig.LocalPort)

	proxy := &ProxyInstance{
		Config:  proxyConfig,
		UDPConn: conn,
		Active:  true,
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

	// 向 server 请求隧道连接来处理这个代理连接 (新架构)
	tunnelConn, err := c.requestTunnelConnection(proxy.Config.ServiceName, proxy.Config.ServiceNamespace)
	if err != nil {
		log.Printf("failed to request tunnel connection for service %s: %v", proxy.Config.ServiceName, err)
		return
	}
	defer tunnelConn.Close()

	log.Printf("proxy %s established tunnel connection for service %s", proxy.Config.Name, proxy.Config.ServiceName)

	// 服务器中转架构：直接进行数据转发，不需要JSON包装
	// tunnelConn 已经是一个工作连接，可以直接进行数据传输
	c.forwardTCPData(conn, tunnelConn, proxy.Config.Name)
	log.Printf("proxy connection closed for %s", proxy.Config.Name)
}

// forwardTCPData 进行TCP数据转发 (服务器中转模式)
func (c *Client) forwardTCPData(localConn, workConn net.Conn, proxyName string) {
	defer func() {
		localConn.Close()
		workConn.Close()
		log.Printf("TCP data forwarding closed for proxy %s", proxyName)
	}()

	log.Printf("starting TCP data forwarding for proxy %s", proxyName)

	// 使用io.Copy进行双向数据转发 (服务器中转模式)
	done := make(chan struct{}, 2)

	// local client -> work connection
	go func() {
		defer func() { done <- struct{}{} }()
		_, err := io.Copy(workConn, localConn)
		if err != nil {
			log.Printf("error copying local->work for proxy %s: %v", proxyName, err)
		}
	}()

	// work connection -> local client
	go func() {
		defer func() { done <- struct{}{} }()
		_, err := io.Copy(localConn, workConn)
		if err != nil {
			log.Printf("error copying work->local for proxy %s: %v", proxyName, err)
		}
	}()

	// 等待任一方向完成
	<-done
	log.Printf("TCP data forwarding completed for proxy %s", proxyName)
}

// handleProxyDataForwarding 处理代理数据转发 (已废弃，使用服务器中转工作连接模式)
func (c *Client) handleProxyDataForwarding(localConn, controlConn net.Conn, serviceName string) {
	log.Printf("handleProxyDataForwarding called but is deprecated in server relay mode")
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

		// 如果是服务未找到错误，尝试自动创建服务
		if strings.Contains(errorPayload.Message, "not found") {
			log.Printf("service %s/%s not found, attempting to create automatically", serviceNamespace, serviceName)

			// 尝试创建服务
			if err := c.createServiceIfNeeded(serviceName, serviceNamespace, selector, encoder, decoder); err != nil {
				log.Printf("failed to auto-create service %s/%s: %v", serviceNamespace, serviceName, err)
				return nil, nil, fmt.Errorf("server error: %s", errorPayload.Message)
			}

			// 重新查询服务
			msg2 := types.Message{
				Type: types.MsgTypeQueryService,
				ID:   uuid.New().String(),
				Payload: types.QueryServicePayload{
					ServiceName:      serviceName,
					ServiceNamespace: serviceNamespace,
					Selector:         selector,
				},
				Timestamp: time.Now(),
			}

			if err := encoder.Encode(msg2); err != nil {
				return nil, nil, fmt.Errorf("failed to send retry query message: %v", err)
			}

			// 等待第二次响应
			if err := decoder.Decode(&response); err != nil {
				return nil, nil, fmt.Errorf("failed to receive retry response: %v", err)
			}

			if response.Type == types.MsgTypeError {
				var retryErrorPayload types.ErrorPayload
				retryPayloadBytes, _ := json.Marshal(response.Payload)
				json.Unmarshal(retryPayloadBytes, &retryErrorPayload)
				return nil, nil, fmt.Errorf("server error after retry: %s", retryErrorPayload.Message)
			}
		} else {
			return nil, nil, fmt.Errorf("server error: %s", errorPayload.Message)
		}
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
			log.Printf("UDP proxy %s received %d bytes from %s", proxy.Config.Name, n, clientAddr.String())
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
	log.Printf("UDP proxy %s handling packet from %s", proxy.Config.Name, sessionKey)

	sessionsMu.RLock()
	session, exists := sessions[sessionKey]
	sessionsMu.RUnlock()

	if !exists {
		log.Printf("UDP proxy %s creating new session for %s", proxy.Config.Name, sessionKey)
		// 通过 server 建立 UDP 隧道会话
		session = c.createUDPTunnelSession(proxy, clientAddr)
		if session == nil {
			log.Printf("UDP proxy %s failed to create tunnel session for %s", proxy.Config.Name, sessionKey)
			return
		}

		sessionsMu.Lock()
		sessions[sessionKey] = session
		sessionsMu.Unlock()

		log.Printf("UDP proxy %s created tunnel session for %s", proxy.Config.Name, sessionKey)
		// 启动会话数据接收
		go c.handleUDPSessionData(proxy, session, sessions, sessionsMu)
	}

	// 更新会话活动时间
	session.mu.Lock()
	session.LastActivity = time.Now()
	session.mu.Unlock()

	// 转发数据到隧道连接
	_, err := session.TargetConn.Write(data)
	if err != nil {
		log.Printf("failed to write UDP data to tunnel for proxy %s: %v", proxy.Config.Name, err)
		c.cleanupUDPSession(sessionKey, session, sessions, sessionsMu)
	}
}

// createUDPTunnelSession 通过 server 创建新的 UDP 隧道会话 (新架构)
func (c *Client) createUDPTunnelSession(proxy *ProxyInstance, clientAddr *net.UDPAddr) *UDPSession {
	log.Printf("Creating UDP tunnel session for service %s, client %s", proxy.Config.ServiceName, clientAddr.String())

	// 向 server 请求 UDP 隧道
	tunnelConn, err := c.requestUDPTunnel(proxy.Config.ServiceName, proxy.Config.ServiceNamespace, clientAddr.String())
	if err != nil {
		log.Printf("failed to request UDP tunnel for service %s: %v", proxy.Config.ServiceName, err)
		return nil
	}

	log.Printf("UDP tunnel established for service %s, client %s", proxy.Config.ServiceName, clientAddr.String())

	// 创建 UDP 会话
	session := &UDPSession{
		ClientAddr:   clientAddr,
		TargetConn:   tunnelConn,
		LastActivity: time.Now(),
	}

	return session
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
		Config:  proxyConfig,
		UDPConn: conn,
		Active:  true,
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

	// TODO: 重新实现 STCP 隧道逻辑，使用工作连接机制
	log.Printf("STCP proxy not implemented with new work connection mechanism")
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

	// 请求建立加密隧道（现在 targetAddr 实际上是 namespace/serviceName 格式）
	tunnelMsg := types.Message{
		Type: types.MsgTypeSTCPTunnelRequest,
		ID:   uuid.New().String(),
		Payload: types.STCPTunnelRequestPayload{
			ServiceName: serviceName,
			TargetAddr:  targetAddr, // 格式：namespace/serviceName
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
	// TODO: 重新实现 SUDP 隧道逻辑，使用工作连接机制
	return nil, fmt.Errorf("SUDP tunnel not implemented with new work connection mechanism")
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

// createServiceIfNeeded 尝试根据selector自动创建服务
func (c *Client) createServiceIfNeeded(serviceName, serviceNamespace string, selector map[string]string, encoder *json.Encoder, decoder *json.Decoder) error {
	if selector == nil || len(selector) == 0 {
		return fmt.Errorf("selector is empty, cannot auto-create service")
	}

	// 构造服务定义
	service := types.Service{
		Metadata: types.ServiceMetadata{
			Name:      serviceName,
			Namespace: serviceNamespace,
			Labels: map[string]string{
				"outpostnet.io/auto-created": "true",
				"outpostnet.io/created-by":   "proxy-client",
			},
		},
		Spec: types.ServiceSpec{
			Selector: selector,
			Ports: []types.ServicePort{
				{
					Name:       "auto",
					Port:       80,   // 默认服务端口
					TargetPort: 8080, // 默认目标端口，实际会从workload推断
					Protocol:   "tcp",
				},
			},
			ProxyMode:    "tcp",
			LoadBalancer: "round_robin",
		},
	}

	// 发送创建服务消息
	msg := types.Message{
		Type: types.MsgTypeCreateService,
		ID:   uuid.New().String(),
		Payload: types.CreateServicePayload{
			Service: service,
		},
		Timestamp: time.Now(),
	}

	if err := encoder.Encode(msg); err != nil {
		return fmt.Errorf("failed to send create service message: %v", err)
	}

	// 等待创建响应
	var response types.Message
	if err := decoder.Decode(&response); err != nil {
		return fmt.Errorf("failed to receive create service response: %v", err)
	}

	if response.Type == types.MsgTypeError {
		var errorPayload types.ErrorPayload
		payloadBytes, _ := json.Marshal(response.Payload)
		json.Unmarshal(payloadBytes, &errorPayload)
		return fmt.Errorf("failed to create service: %s", errorPayload.Message)
	}

	if response.Type != types.MsgTypeServiceCreated {
		return fmt.Errorf("unexpected response type for create service: %s", response.Type)
	}

	log.Printf("successfully auto-created service %s/%s", serviceNamespace, serviceName)
	return nil
}
