package protocol

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/outpostos/OutpostNet/pkg/types"
)

// ProtocolHandler 协议处理器接口
type ProtocolHandler interface {
	HandleConnection(ctx context.Context, clientConn net.Conn, target *types.Endpoint) error
	GetProtocol() string
	SupportsEncryption() bool
	Close() error
}

// ProtocolManager 协议管理器
type ProtocolManager struct {
	handlers map[string]ProtocolHandler
	mu       sync.RWMutex
	
	// 统计信息
	connectionCounts map[string]int64
	errorCounts      map[string]int64
	totalBytesTransferred map[string]int64
}

// NewProtocolManager 创建协议管理器
func NewProtocolManager() *ProtocolManager {
	pm := &ProtocolManager{
		handlers:              make(map[string]ProtocolHandler),
		connectionCounts:      make(map[string]int64),
		errorCounts:           make(map[string]int64),
		totalBytesTransferred: make(map[string]int64),
	}
	
	// 注册内置协议处理器
	pm.RegisterHandler("tcp", NewTCPHandler())
	pm.RegisterHandler("udp", NewUDPHandler())
	pm.RegisterHandler("stcp", NewSTCPHandler())
	pm.RegisterHandler("sudp", NewSUDPHandler())
	
	return pm
}

// RegisterHandler 注册协议处理器
func (pm *ProtocolManager) RegisterHandler(protocol string, handler ProtocolHandler) {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	pm.handlers[protocol] = handler
}

// GetHandler 获取协议处理器
func (pm *ProtocolManager) GetHandler(protocol string) (ProtocolHandler, error) {
	pm.mu.RLock()
	defer pm.mu.RUnlock()
	
	handler, exists := pm.handlers[protocol]
	if !exists {
		return nil, fmt.Errorf("unsupported protocol: %s", protocol)
	}
	
	return handler, nil
}

// HandleConnection 处理连接
func (pm *ProtocolManager) HandleConnection(ctx context.Context, protocol string, clientConn net.Conn, target *types.Endpoint) error {
	handler, err := pm.GetHandler(protocol)
	if err != nil {
		return err
	}
	
	// 更新统计信息
	pm.mu.Lock()
	pm.connectionCounts[protocol]++
	pm.mu.Unlock()
	
	// 处理连接
	err = handler.HandleConnection(ctx, clientConn, target)
	
	// 更新错误统计
	if err != nil {
		pm.mu.Lock()
		pm.errorCounts[protocol]++
		pm.mu.Unlock()
	}
	
	return err
}

// GetStats 获取统计信息
func (pm *ProtocolManager) GetStats() map[string]*ProtocolStats {
	pm.mu.RLock()
	defer pm.mu.RUnlock()
	
	stats := make(map[string]*ProtocolStats)
	for protocol := range pm.handlers {
		stats[protocol] = &ProtocolStats{
			Protocol:              protocol,
			ConnectionCount:       pm.connectionCounts[protocol],
			ErrorCount:           pm.errorCounts[protocol],
			TotalBytesTransferred: pm.totalBytesTransferred[protocol],
		}
	}
	
	return stats
}

// ProtocolStats 协议统计信息
type ProtocolStats struct {
	Protocol              string `json:"protocol"`
	ConnectionCount       int64  `json:"connection_count"`
	ErrorCount           int64  `json:"error_count"`
	TotalBytesTransferred int64  `json:"total_bytes_transferred"`
}

// === TCP协议处理器 ===

// TCPHandler TCP协议处理器
type TCPHandler struct {
	bufferSize int
}

// NewTCPHandler 创建TCP处理器
func NewTCPHandler() *TCPHandler {
	return &TCPHandler{
		bufferSize: 32 * 1024, // 32KB缓冲区
	}
}

// HandleConnection 处理TCP连接
func (h *TCPHandler) HandleConnection(ctx context.Context, clientConn net.Conn, target *types.Endpoint) error {
	// 连接到目标端点
	targetAddr := fmt.Sprintf("%s:%d", target.IP, target.Port)
	targetConn, err := net.DialTimeout("tcp", targetAddr, 10*time.Second)
	if err != nil {
		return fmt.Errorf("failed to connect to target %s: %w", targetAddr, err)
	}
	defer targetConn.Close()
	
	// 双向数据转发
	return h.relayData(ctx, clientConn, targetConn)
}

// relayData 双向数据转发
func (h *TCPHandler) relayData(ctx context.Context, conn1, conn2 net.Conn) error {
	var wg sync.WaitGroup
	var err1, err2 error
	
	// 从conn1到conn2
	wg.Add(1)
	go func() {
		defer wg.Done()
		_, err1 = h.copyWithContext(ctx, conn2, conn1)
	}()
	
	// 从conn2到conn1
	wg.Add(1)
	go func() {
		defer wg.Done()
		_, err2 = h.copyWithContext(ctx, conn1, conn2)
	}()
	
	wg.Wait()
	
	// 返回第一个遇到的错误
	if err1 != nil && err1 != io.EOF {
		return err1
	}
	if err2 != nil && err2 != io.EOF {
		return err2
	}
	
	return nil
}

// copyWithContext 带上下文的数据复制
func (h *TCPHandler) copyWithContext(ctx context.Context, dst io.Writer, src io.Reader) (int64, error) {
	buf := make([]byte, h.bufferSize)
	var written int64
	
	for {
		select {
		case <-ctx.Done():
			return written, ctx.Err()
		default:
		}
		
		// 设置读取超时
		if conn, ok := src.(net.Conn); ok {
			conn.SetReadDeadline(time.Now().Add(5 * time.Second))
		}
		
		n, err := src.Read(buf)
		if n > 0 {
			nw, ew := dst.Write(buf[:n])
			if nw > 0 {
				written += int64(nw)
			}
			if ew != nil {
				return written, ew
			}
		}
		
		if err != nil {
			if err == io.EOF {
				return written, nil
			}
			return written, err
		}
	}
}

// GetProtocol 获取协议名称
func (h *TCPHandler) GetProtocol() string {
	return "tcp"
}

// SupportsEncryption 是否支持加密
func (h *TCPHandler) SupportsEncryption() bool {
	return false
}

// Close 关闭处理器
func (h *TCPHandler) Close() error {
	return nil
}

// === UDP协议处理器 ===

// UDPHandler UDP协议处理器
type UDPHandler struct {
	bufferSize    int
	sessionTimeout time.Duration
	sessions      map[string]*UDPSession
	sessionsMu    sync.RWMutex
}

// UDPSession UDP会话
type UDPSession struct {
	ClientAddr  *net.UDPAddr
	TargetConn  *net.UDPConn
	LastActive  time.Time
	BytesTransferred int64
}

// NewUDPHandler 创建UDP处理器
func NewUDPHandler() *UDPHandler {
	h := &UDPHandler{
		bufferSize:    64 * 1024, // 64KB缓冲区
		sessionTimeout: 5 * time.Minute,
		sessions:      make(map[string]*UDPSession),
	}
	
	// 启动会话清理goroutine
	go h.cleanupSessions()
	
	return h
}

// HandleConnection 处理UDP连接
func (h *UDPHandler) HandleConnection(ctx context.Context, clientConn net.Conn, target *types.Endpoint) error {
	// UDP连接实际上是UDPConn类型
	udpConn, ok := clientConn.(*net.UDPConn)
	if !ok {
		return fmt.Errorf("expected UDP connection, got %T", clientConn)
	}
	
	// 创建到目标的UDP连接
	targetAddr := fmt.Sprintf("%s:%d", target.IP, target.Port)
	targetUDPAddr, err := net.ResolveUDPAddr("udp", targetAddr)
	if err != nil {
		return fmt.Errorf("failed to resolve target address %s: %w", targetAddr, err)
	}
	
	targetConn, err := net.DialUDP("udp", nil, targetUDPAddr)
	if err != nil {
		return fmt.Errorf("failed to dial target %s: %w", targetAddr, err)
	}
	defer targetConn.Close()
	
	// 处理UDP数据包
	return h.handleUDPPackets(ctx, udpConn, targetConn)
}

// handleUDPPackets 处理UDP数据包
func (h *UDPHandler) handleUDPPackets(ctx context.Context, clientConn, targetConn *net.UDPConn) error {
	buf := make([]byte, h.bufferSize)
	
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		
		// 设置读取超时
		clientConn.SetReadDeadline(time.Now().Add(1 * time.Second))
		
		n, clientAddr, err := clientConn.ReadFromUDP(buf)
		if err != nil {
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				continue
			}
			return err
		}
		
		// 转发到目标
		_, err = targetConn.Write(buf[:n])
		if err != nil {
			return fmt.Errorf("failed to write to target: %w", err)
		}
		
		// 创建或更新会话
		sessionKey := clientAddr.String()
		h.updateSession(sessionKey, clientAddr, targetConn, int64(n))
		
		// 处理响应（简化实现，实际应该在单独的goroutine中处理）
		go h.handleResponse(clientConn, targetConn, clientAddr)
	}
}

// updateSession 更新会话
func (h *UDPHandler) updateSession(key string, clientAddr *net.UDPAddr, targetConn *net.UDPConn, bytes int64) {
	h.sessionsMu.Lock()
	defer h.sessionsMu.Unlock()
	
	session, exists := h.sessions[key]
	if !exists {
		session = &UDPSession{
			ClientAddr: clientAddr,
			TargetConn: targetConn,
		}
		h.sessions[key] = session
	}
	
	session.LastActive = time.Now()
	session.BytesTransferred += bytes
}

// handleResponse 处理响应
func (h *UDPHandler) handleResponse(clientConn, targetConn *net.UDPConn, clientAddr *net.UDPAddr) {
	buf := make([]byte, h.bufferSize)
	
	targetConn.SetReadDeadline(time.Now().Add(5 * time.Second))
	n, err := targetConn.Read(buf)
	if err != nil {
		return
	}
	
	// 转发响应给客户端
	clientConn.WriteToUDP(buf[:n], clientAddr)
}

// cleanupSessions 清理过期会话
func (h *UDPHandler) cleanupSessions() {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()
	
	for range ticker.C {
		h.sessionsMu.Lock()
		now := time.Now()
		for key, session := range h.sessions {
			if now.Sub(session.LastActive) > h.sessionTimeout {
				delete(h.sessions, key)
			}
		}
		h.sessionsMu.Unlock()
	}
}

// GetProtocol 获取协议名称
func (h *UDPHandler) GetProtocol() string {
	return "udp"
}

// SupportsEncryption 是否支持加密
func (h *UDPHandler) SupportsEncryption() bool {
	return false
}

// Close 关闭处理器
func (h *UDPHandler) Close() error {
	// 关闭所有会话
	h.sessionsMu.Lock()
	defer h.sessionsMu.Unlock()
	
	for _, session := range h.sessions {
		if session.TargetConn != nil {
			session.TargetConn.Close()
		}
	}
	
	h.sessions = make(map[string]*UDPSession)
	return nil
}

// === STCP协议处理器 ===

// STCPHandler STCP（安全TCP）协议处理器
type STCPHandler struct {
	tcpHandler *TCPHandler
	tlsConfig  *tls.Config
}

// NewSTCPHandler 创建STCP处理器
func NewSTCPHandler() *STCPHandler {
	return &STCPHandler{
		tcpHandler: NewTCPHandler(),
		tlsConfig: &tls.Config{
			InsecureSkipVerify: true, // 开发环境下跳过证书验证
		},
	}
}

// SetTLSConfig 设置TLS配置
func (h *STCPHandler) SetTLSConfig(config *tls.Config) {
	h.tlsConfig = config
}

// HandleConnection 处理STCP连接
func (h *STCPHandler) HandleConnection(ctx context.Context, clientConn net.Conn, target *types.Endpoint) error {
	// 建立到目标的TLS连接
	targetAddr := fmt.Sprintf("%s:%d", target.IP, target.Port)
	dialer := &net.Dialer{Timeout: 10 * time.Second}
	
	rawConn, err := dialer.DialContext(ctx, "tcp", targetAddr)
	if err != nil {
		return fmt.Errorf("failed to connect to target %s: %w", targetAddr, err)
	}
	
	// 升级为TLS连接
	tlsConn := tls.Client(rawConn, h.tlsConfig)
	defer tlsConn.Close()
	
	// 执行TLS握手
	if err := tlsConn.Handshake(); err != nil {
		return fmt.Errorf("TLS handshake failed: %w", err)
	}
	
	// 使用TCP处理器进行数据转发
	return h.tcpHandler.relayData(ctx, clientConn, tlsConn)
}

// GetProtocol 获取协议名称
func (h *STCPHandler) GetProtocol() string {
	return "stcp"
}

// SupportsEncryption 是否支持加密
func (h *STCPHandler) SupportsEncryption() bool {
	return true
}

// Close 关闭处理器
func (h *STCPHandler) Close() error {
	return h.tcpHandler.Close()
}

// === SUDP协议处理器 ===

// SUDPHandler SUDP（安全UDP）协议处理器
type SUDPHandler struct {
	udpHandler *UDPHandler
	tlsConfig  *tls.Config
}

// NewSUDPHandler 创建SUDP处理器
func NewSUDPHandler() *SUDPHandler {
	return &SUDPHandler{
		udpHandler: NewUDPHandler(),
		tlsConfig: &tls.Config{
			InsecureSkipVerify: true,
		},
	}
}

// SetTLSConfig 设置TLS配置
func (h *SUDPHandler) SetTLSConfig(config *tls.Config) {
	h.tlsConfig = config
}

// HandleConnection 处理SUDP连接
func (h *SUDPHandler) HandleConnection(ctx context.Context, clientConn net.Conn, target *types.Endpoint) error {
	// SUDP是UDP over TLS的实现
	// 首先建立TLS连接作为隧道
	targetAddr := fmt.Sprintf("%s:%d", target.IP, target.Port)
	dialer := &net.Dialer{Timeout: 10 * time.Second}
	
	rawConn, err := dialer.DialContext(ctx, "tcp", targetAddr)
	if err != nil {
		return fmt.Errorf("failed to connect to target %s: %w", targetAddr, err)
	}
	
	// 升级为TLS连接
	tlsConn := tls.Client(rawConn, h.tlsConfig)
	defer tlsConn.Close()
	
	// 执行TLS握手
	if err := tlsConn.Handshake(); err != nil {
		return fmt.Errorf("TLS handshake failed: %w", err)
	}
	
	// 在TLS隧道内传输UDP数据
	return h.relayUDPOverTLS(ctx, clientConn, tlsConn)
}

// relayUDPOverTLS 在TLS隧道内转发UDP数据
func (h *SUDPHandler) relayUDPOverTLS(ctx context.Context, udpConn net.Conn, tlsConn *tls.Conn) error {
	var wg sync.WaitGroup
	
	// UDP到TLS
	wg.Add(1)
	go func() {
		defer wg.Done()
		h.udpToTLS(ctx, udpConn, tlsConn)
	}()
	
	// TLS到UDP
	wg.Add(1)
	go func() {
		defer wg.Done()
		h.tlsToUDP(ctx, tlsConn, udpConn)
	}()
	
	wg.Wait()
	return nil
}

// udpToTLS UDP数据到TLS
func (h *SUDPHandler) udpToTLS(ctx context.Context, udpConn net.Conn, tlsConn *tls.Conn) {
	buf := make([]byte, 64*1024)
	
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		
		n, err := udpConn.Read(buf)
		if err != nil {
			return
		}
		
		// 写入数据长度头（4字节）+ 数据
		lengthBuf := make([]byte, 4)
		lengthBuf[0] = byte(n >> 24)
		lengthBuf[1] = byte(n >> 16)
		lengthBuf[2] = byte(n >> 8)
		lengthBuf[3] = byte(n)
		
		if _, err := tlsConn.Write(lengthBuf); err != nil {
			return
		}
		
		if _, err := tlsConn.Write(buf[:n]); err != nil {
			return
		}
	}
}

// tlsToUDP TLS数据到UDP
func (h *SUDPHandler) tlsToUDP(ctx context.Context, tlsConn *tls.Conn, udpConn net.Conn) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		
		// 读取数据长度
		lengthBuf := make([]byte, 4)
		if _, err := io.ReadFull(tlsConn, lengthBuf); err != nil {
			return
		}
		
		length := int(lengthBuf[0])<<24 | int(lengthBuf[1])<<16 | int(lengthBuf[2])<<8 | int(lengthBuf[3])
		
		// 读取数据
		buf := make([]byte, length)
		if _, err := io.ReadFull(tlsConn, buf); err != nil {
			return
		}
		
		// 写入UDP连接
		if _, err := udpConn.Write(buf); err != nil {
			return
		}
	}
}

// GetProtocol 获取协议名称
func (h *SUDPHandler) GetProtocol() string {
	return "sudp"
}

// SupportsEncryption 是否支持加密
func (h *SUDPHandler) SupportsEncryption() bool {
	return true
}

// Close 关闭处理器
func (h *SUDPHandler) Close() error {
	return h.udpHandler.Close()
}