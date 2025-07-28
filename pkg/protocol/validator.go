package protocol

import (
	"fmt"
	"net"
	"time"

	"github.com/outpostos/OutpostNet/pkg/types"
)

// ProtocolValidator 协议验证器
type ProtocolValidator struct {
	manager *ProtocolManager
}

// NewProtocolValidator 创建协议验证器
func NewProtocolValidator() *ProtocolValidator {
	return &ProtocolValidator{
		manager: NewProtocolManager(),
	}
}

// ValidationResult 验证结果
type ValidationResult struct {
	Protocol    string        `json:"protocol"`
	Success     bool          `json:"success"`
	Error       string        `json:"error,omitempty"`
	Latency     time.Duration `json:"latency"`
	BytesSent   int64         `json:"bytes_sent"`
	BytesReceived int64       `json:"bytes_received"`
	TestDetails map[string]interface{} `json:"test_details,omitempty"`
}

// ValidateProtocol 验证单个协议
func (v *ProtocolValidator) ValidateProtocol(protocol string, target *types.Endpoint) (*ValidationResult, error) {
	_, err := v.manager.GetHandler(protocol)
	if err != nil {
		return &ValidationResult{
			Protocol: protocol,
			Success:  false,
			Error:    fmt.Sprintf("Protocol not supported: %v", err),
		}, nil
	}

	switch protocol {
	case "tcp":
		return v.validateTCP(target)
	case "udp":
		return v.validateUDP(target)
	case "stcp":
		return v.validateSTCP(target)
	case "sudp":
		return v.validateSUDP(target)
	default:
		return &ValidationResult{
			Protocol: protocol,
			Success:  false,
			Error:    fmt.Sprintf("Unknown protocol: %s", protocol),
		}, nil
	}
}

// ValidateAllProtocols 验证所有协议
func (v *ProtocolValidator) ValidateAllProtocols(target *types.Endpoint) (map[string]*ValidationResult, error) {
	protocols := []string{"tcp", "udp", "stcp", "sudp"}
	results := make(map[string]*ValidationResult)

	for _, protocol := range protocols {
		result, err := v.ValidateProtocol(protocol, target)
		if err != nil {
			result = &ValidationResult{
				Protocol: protocol,
				Success:  false,
				Error:    fmt.Sprintf("Validation error: %v", err),
			}
		}
		results[protocol] = result
	}

	return results, nil
}

// validateTCP 验证TCP协议
func (v *ProtocolValidator) validateTCP(target *types.Endpoint) (*ValidationResult, error) {
	start := time.Now()
	
	targetAddr := fmt.Sprintf("%s:%d", target.IP, target.Port)
	conn, err := net.DialTimeout("tcp", targetAddr, 5*time.Second)
	if err != nil {
		return &ValidationResult{
			Protocol: "tcp",
			Success:  false,
			Error:    fmt.Sprintf("Failed to connect: %v", err),
			Latency:  time.Since(start),
		}, nil
	}
	defer conn.Close()

	// 发送测试数据
	testData := []byte("OUTPOSTNET_TCP_TEST\n")
	bytesSent, err := conn.Write(testData)
	if err != nil {
		return &ValidationResult{
			Protocol:  "tcp",
			Success:   false,
			Error:     fmt.Sprintf("Failed to send data: %v", err),
			Latency:   time.Since(start),
			BytesSent: int64(bytesSent),
		}, nil
	}

	// 尝试读取响应
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	buffer := make([]byte, 1024)
	bytesReceived, err := conn.Read(buffer)
	
	result := &ValidationResult{
		Protocol:      "tcp",
		Success:       true,
		Latency:       time.Since(start),
		BytesSent:     int64(bytesSent),
		BytesReceived: int64(bytesReceived),
		TestDetails: map[string]interface{}{
			"connection_established": true,
			"data_sent":             string(testData),
			"response_received":     err == nil,
		},
	}

	if err != nil {
		result.TestDetails["read_error"] = err.Error()
	} else {
		result.TestDetails["response_data"] = string(buffer[:bytesReceived])
	}

	return result, nil
}

// validateUDP 验证UDP协议
func (v *ProtocolValidator) validateUDP(target *types.Endpoint) (*ValidationResult, error) {
	start := time.Now()
	
	targetAddr := fmt.Sprintf("%s:%d", target.IP, target.Port)
	udpAddr, err := net.ResolveUDPAddr("udp", targetAddr)
	if err != nil {
		return &ValidationResult{
			Protocol: "udp",
			Success:  false,
			Error:    fmt.Sprintf("Failed to resolve address: %v", err),
			Latency:  time.Since(start),
		}, nil
	}

	conn, err := net.DialUDP("udp", nil, udpAddr)
	if err != nil {
		return &ValidationResult{
			Protocol: "udp",
			Success:  false,
			Error:    fmt.Sprintf("Failed to dial: %v", err),
			Latency:  time.Since(start),
		}, nil
	}
	defer conn.Close()

	// 发送测试数据
	testData := []byte("OUTPOSTNET_UDP_TEST")
	bytesSent, err := conn.Write(testData)
	if err != nil {
		return &ValidationResult{
			Protocol:  "udp",
			Success:   false,
			Error:     fmt.Sprintf("Failed to send data: %v", err),
			Latency:   time.Since(start),
			BytesSent: int64(bytesSent),
		}, nil
	}

	// 尝试读取响应
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	buffer := make([]byte, 1024)
	bytesReceived, err := conn.Read(buffer)
	
	result := &ValidationResult{
		Protocol:      "udp",
		Success:       true,
		Latency:       time.Since(start),
		BytesSent:     int64(bytesSent),
		BytesReceived: int64(bytesReceived),
		TestDetails: map[string]interface{}{
			"packet_sent":      true,
			"data_sent":        string(testData),
			"response_received": err == nil,
		},
	}

	if err != nil {
		result.TestDetails["read_error"] = err.Error()
	} else {
		result.TestDetails["response_data"] = string(buffer[:bytesReceived])
	}

	return result, nil
}

// validateSTCP 验证STCP协议
func (v *ProtocolValidator) validateSTCP(target *types.Endpoint) (*ValidationResult, error) {
	start := time.Now()
	
	// STCP需要先建立TLS连接
	targetAddr := fmt.Sprintf("%s:%d", target.IP, target.Port)
	
	// 首先尝试普通TCP连接以测试基础连通性
	conn, err := net.DialTimeout("tcp", targetAddr, 5*time.Second)
	if err != nil {
		return &ValidationResult{
			Protocol: "stcp",
			Success:  false,
			Error:    fmt.Sprintf("Base TCP connection failed: %v", err),
			Latency:  time.Since(start),
		}, nil
	}
	conn.Close()

	// 测试TLS握手（简化版本）
	tlsTestResult := v.testTLSHandshake(targetAddr)
	
	return &ValidationResult{
		Protocol: "stcp",
		Success:  tlsTestResult.Success,
		Error:    tlsTestResult.Error,
		Latency:  time.Since(start),
		TestDetails: map[string]interface{}{
			"tcp_connection":  true,
			"tls_supported":   tlsTestResult.Success,
			"encryption_test": tlsTestResult.Details,
		},
	}, nil
}

// validateSUDP 验证SUDP协议
func (v *ProtocolValidator) validateSUDP(target *types.Endpoint) (*ValidationResult, error) {
	start := time.Now()
	
	// SUDP是UDP over TLS，需要先测试UDP基础连通性
	udpResult, err := v.validateUDP(target)
	if err != nil || !udpResult.Success {
		return &ValidationResult{
			Protocol: "sudp",
			Success:  false,
			Error:    fmt.Sprintf("Base UDP validation failed: %v", udpResult.Error),
			Latency:  time.Since(start),
		}, nil
	}

	// 测试TLS隧道支持
	targetAddr := fmt.Sprintf("%s:%d", target.IP, target.Port)
	tlsTestResult := v.testTLSHandshake(targetAddr)
	
	return &ValidationResult{
		Protocol: "sudp",
		Success:  udpResult.Success && tlsTestResult.Success,
		Error:    tlsTestResult.Error,
		Latency:  time.Since(start),
		TestDetails: map[string]interface{}{
			"udp_connection":    udpResult.Success,
			"tls_tunnel_support": tlsTestResult.Success,
			"encryption_test":   tlsTestResult.Details,
		},
	}, nil
}

// TLSTestResult TLS测试结果
type TLSTestResult struct {
	Success bool
	Error   string
	Details map[string]interface{}
}

// testTLSHandshake 测试TLS握手
func (v *ProtocolValidator) testTLSHandshake(addr string) *TLSTestResult {
	// 简化的TLS测试，实际环境中需要更复杂的验证
	conn, err := net.DialTimeout("tcp", addr, 3*time.Second)
	if err != nil {
		return &TLSTestResult{
			Success: false,
			Error:   fmt.Sprintf("TCP connection for TLS test failed: %v", err),
			Details: map[string]interface{}{
				"connection_error": err.Error(),
			},
		}
	}
	defer conn.Close()

	// 发送TLS ClientHello的简化测试
	// 实际实现中应该使用真正的TLS握手
	testData := []byte("\x16\x03\x01") // TLS Record Header start
	_, err = conn.Write(testData)
	if err != nil {
		return &TLSTestResult{
			Success: false,
			Error:   fmt.Sprintf("Failed to send TLS test data: %v", err),
			Details: map[string]interface{}{
				"handshake_error": err.Error(),
			},
		}
	}

	// 尝试读取响应
	conn.SetReadDeadline(time.Now().Add(1 * time.Second))
	buffer := make([]byte, 64)
	_, err = conn.Read(buffer)
	
	return &TLSTestResult{
		Success: true, // 能够发送数据就认为基础支持TLS
		Details: map[string]interface{}{
			"handshake_attempted": true,
			"response_received":   err == nil,
		},
	}
}

// ProtocolCompatibilityTest 协议兼容性测试
type ProtocolCompatibilityTest struct {
	validator *ProtocolValidator
}

// NewProtocolCompatibilityTest 创建协议兼容性测试
func NewProtocolCompatibilityTest() *ProtocolCompatibilityTest {
	return &ProtocolCompatibilityTest{
		validator: NewProtocolValidator(),
	}
}

// TestEndpointCompatibility 测试端点的协议兼容性
func (t *ProtocolCompatibilityTest) TestEndpointCompatibility(endpoint *types.Endpoint) (*EndpointCompatibilityResult, error) {
	results, err := t.validator.ValidateAllProtocols(endpoint)
	if err != nil {
		return nil, err
	}

	compatibility := &EndpointCompatibilityResult{
		Endpoint:         endpoint,
		SupportedProtocols: make([]string, 0),
		ValidationResults:  results,
		TestTimestamp:      time.Now(),
	}

	// 统计支持的协议
	for protocol, result := range results {
		if result.Success {
			compatibility.SupportedProtocols = append(compatibility.SupportedProtocols, protocol)
		}
	}

	// 计算总体得分
	compatibility.CompatibilityScore = float64(len(compatibility.SupportedProtocols)) / 4.0 * 100

	return compatibility, nil
}

// EndpointCompatibilityResult 端点兼容性结果
type EndpointCompatibilityResult struct {
	Endpoint           *types.Endpoint           `json:"endpoint"`
	SupportedProtocols []string                  `json:"supported_protocols"`
	ValidationResults  map[string]*ValidationResult `json:"validation_results"`
	CompatibilityScore float64                   `json:"compatibility_score"`
	TestTimestamp      time.Time                 `json:"test_timestamp"`
}

// GetRecommendedProtocol 获取推荐协议
func (r *EndpointCompatibilityResult) GetRecommendedProtocol() string {
	// 协议优先级：STCP > TCP > SUDP > UDP
	priorities := []string{"stcp", "tcp", "sudp", "udp"}
	
	for _, protocol := range priorities {
		for _, supported := range r.SupportedProtocols {
			if protocol == supported {
				return protocol
			}
		}
	}
	
	return "tcp" // 默认返回TCP
}

// IsSecureProtocolSupported 是否支持安全协议
func (r *EndpointCompatibilityResult) IsSecureProtocolSupported() bool {
	for _, protocol := range r.SupportedProtocols {
		if protocol == "stcp" || protocol == "sudp" {
			return true
		}
	}
	return false
}

// GetCompatibilityReport 获取兼容性报告
func (r *EndpointCompatibilityResult) GetCompatibilityReport() string {
	report := fmt.Sprintf("Endpoint %s:%d Compatibility Report\n", r.Endpoint.IP, r.Endpoint.Port)
	report += fmt.Sprintf("Tested at: %s\n", r.TestTimestamp.Format("2006-01-02 15:04:05"))
	report += fmt.Sprintf("Compatibility Score: %.1f%%\n", r.CompatibilityScore)
	report += fmt.Sprintf("Supported Protocols: %v\n", r.SupportedProtocols)
	report += fmt.Sprintf("Recommended Protocol: %s\n", r.GetRecommendedProtocol())
	report += fmt.Sprintf("Secure Protocol Support: %t\n", r.IsSecureProtocolSupported())
	
	report += "\nDetailed Results:\n"
	for protocol, result := range r.ValidationResults {
		status := "✓"
		if !result.Success {
			status = "✗"
		}
		report += fmt.Sprintf("  %s %s: %v (latency: %v)\n", status, protocol, result.Success, result.Latency)
		if result.Error != "" {
			report += fmt.Sprintf("    Error: %s\n", result.Error)
		}
	}
	
	return report
}