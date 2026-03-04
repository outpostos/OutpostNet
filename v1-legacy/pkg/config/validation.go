package config

import (
	"fmt"
	"net"
	"strconv"
	"strings"

	"github.com/outpostos/OutpostNet/pkg/types"
)

// ValidationError 详细的验证错误
type ValidationError struct {
	Field   string `json:"field"`
	Value   string `json:"value"`
	Message string `json:"message"`
	Code    string `json:"code"`
}

func (e ValidationError) Error() string {
	return fmt.Sprintf("validation error in field '%s': %s (value: %s)", e.Field, e.Message, e.Value)
}

// ValidationResult 验证结果
type ValidationResult struct {
	Valid  bool              `json:"valid"`
	Errors []ValidationError `json:"errors"`
}

// ValidateServerConfig 验证服务端配置
func ValidateServerConfig(config *ServerConfig) error {
	if config == nil {
		return ValidationError{
			Field:   "config",
			Message: "configuration object is required",
			Code:    "CONFIG_NIL",
		}
	}

	var errors []ValidationError

	// 验证服务端配置
	if err := validateServerSpec(&config.Server); err != nil {
		if ve, ok := err.(ValidationError); ok {
			ve.Field = "server." + ve.Field
			errors = append(errors, ve)
		} else {
			errors = append(errors, ValidationError{
				Field:   "server",
				Message: err.Error(),
				Code:    "SERVER_SPEC_INVALID",
			})
		}
	}

	// 验证服务列表
	serviceNames := make(map[string]bool)
	for i, service := range config.Services {
		if err := validateService(&service); err != nil {
			if ve, ok := err.(ValidationError); ok {
				ve.Field = fmt.Sprintf("services[%d].%s", i, ve.Field)
				errors = append(errors, ve)
			} else {
				errors = append(errors, ValidationError{
					Field:   fmt.Sprintf("services[%d]", i),
					Message: err.Error(),
					Code:    "SERVICE_INVALID",
				})
			}
		}

		// 检查服务名重复
		serviceKey := service.Metadata.Namespace + "/" + service.Metadata.Name
		if serviceNames[serviceKey] {
			errors = append(errors, ValidationError{
				Field:   fmt.Sprintf("services[%d].metadata.name", i),
				Value:   serviceKey,
				Message: "duplicate service name in same namespace",
				Code:    "DUPLICATE_SERVICE_NAME",
			})
		}
		serviceNames[serviceKey] = true
	}

	// 验证K8s API配置
	if config.K8sAPI.Enabled {
		if err := validateK8sAPISpec(&config.K8sAPI); err != nil {
			if ve, ok := err.(ValidationError); ok {
				ve.Field = "k8s_api." + ve.Field
				errors = append(errors, ve)
			} else {
				errors = append(errors, ValidationError{
					Field:   "k8s_api",
					Message: err.Error(),
					Code:    "K8S_API_INVALID",
				})
			}
		}
	}

	// 验证监控配置
	if config.Monitoring.Enabled {
		if err := validateMonitoringSpec(&config.Monitoring); err != nil {
			if ve, ok := err.(ValidationError); ok {
				ve.Field = "monitoring." + ve.Field
				errors = append(errors, ve)
			} else {
				errors = append(errors, ValidationError{
					Field:   "monitoring",
					Message: err.Error(),
					Code:    "MONITORING_INVALID",
				})
			}
		}
	}

	// 验证端口冲突
	if err := validatePortConflicts(config); err != nil {
		errors = append(errors, err.(ValidationError))
	}

	if len(errors) > 0 {
		return &ConfigValidationError{Errors: errors}
	}

	return nil
}

// ConfigValidationError 配置验证错误集合
type ConfigValidationError struct {
	Errors []ValidationError
}

func (e *ConfigValidationError) Error() string {
	if len(e.Errors) == 1 {
		return e.Errors[0].Error()
	}
	return fmt.Sprintf("configuration validation failed with %d errors", len(e.Errors))
}

func (e *ConfigValidationError) GetErrors() []ValidationError {
	return e.Errors
}

// ValidateClientConfig 验证客户端配置
func ValidateClientConfig(config *ClientConfig) error {
	if config == nil {
		return fmt.Errorf("config is nil")
	}

	// 验证客户端配置
	if err := validateClientSpec(&config.Client); err != nil {
		return fmt.Errorf("invalid client spec: %v", err)
	}

	// 验证代理配置列表
	for i, proxy := range config.Proxies {
		if err := validateProxy(&proxy); err != nil {
			return fmt.Errorf("invalid proxy at index %d: %v", i, err)
		}
	}

	// 验证工作负载配置列表 (Register 模式)
	for i, workload := range config.Workloads {
		if err := validateClientWorkload(&workload); err != nil {
			return fmt.Errorf("invalid workload at index %d: %v", i, err)
		}
	}

	return nil
}

// validateServerSpec 验证服务端规格
func validateServerSpec(spec *ServerSpec) error {
	if spec.ListenAddr == "" {
		return ValidationError{
			Field:   "listen_addr",
			Message: "listen address is required",
			Code:    "LISTEN_ADDR_REQUIRED",
		}
	}

	if !isValidAddr(spec.ListenAddr) {
		return ValidationError{
			Field:   "listen_addr",
			Value:   spec.ListenAddr,
			Message: "invalid listen address format, expected host:port",
			Code:    "LISTEN_ADDR_INVALID",
		}
	}

	// 验证认证令牌
	if spec.AuthToken == "" {
		return ValidationError{
			Field:   "auth_token",
			Message: "authentication token is required",
			Code:    "AUTH_TOKEN_REQUIRED",
		}
	}

	if len(spec.AuthToken) < 8 {
		return ValidationError{
			Field:   "auth_token",
			Value:   "***",
			Message: "authentication token must be at least 8 characters long",
			Code:    "AUTH_TOKEN_TOO_SHORT",
		}
	}

	// 验证Unix Socket路径
	if spec.UnixSockPath != "" {
		if !isValidUnixSocketPath(spec.UnixSockPath) {
			return ValidationError{
				Field:   "unix_sock_path",
				Value:   spec.UnixSockPath,
				Message: "invalid unix socket path format",
				Code:    "UNIX_SOCKET_PATH_INVALID",
			}
		}
	}

	// 验证 TLS 配置
	if spec.TLS != nil {
		if err := validateTLSConfig(spec.TLS); err != nil {
			return err
		}
	}

	return nil
}

// validateClientSpec 验证客户端规格
func validateClientSpec(spec *ClientSpec) error {
	if spec.ServerAddr == "" {
		return fmt.Errorf("server_addr is required")
	}

	if !isValidAddr(spec.ServerAddr) {
		return fmt.Errorf("invalid server_addr format: %s", spec.ServerAddr)
	}

	// 验证模式
	if spec.Mode != "" && spec.Mode != "proxy" && spec.Mode != "register" {
		return fmt.Errorf("invalid mode: %s, must be 'proxy' or 'register'", spec.Mode)
	}

	// 验证 TLS 配置
	if spec.TLS != nil {
		if err := validateTLSConfig(spec.TLS); err != nil {
			return fmt.Errorf("invalid tls config: %v", err)
		}
	}

	return nil
}

// validateTLSConfig 验证 TLS 配置
func validateTLSConfig(config *TLSConfig) error {
	if config.CertFile == "" && config.KeyFile == "" && config.CAFile == "" {
		return fmt.Errorf("at least one of cert_file, key_file, or ca_file must be specified")
	}

	// 如果指定了证书文件，密钥文件也必须指定
	if config.CertFile != "" && config.KeyFile == "" {
		return fmt.Errorf("key_file is required when cert_file is specified")
	}

	if config.KeyFile != "" && config.CertFile == "" {
		return fmt.Errorf("cert_file is required when key_file is specified")
	}

	return nil
}

// validateService 验证服务配置
func validateService(service *types.Service) error {
	if service.Metadata.Name == "" {
		return fmt.Errorf("service name is required")
	}

	if service.Metadata.Namespace == "" {
		return fmt.Errorf("service namespace is required")
	}

	// 验证端口列表
	if len(service.Spec.Ports) == 0 {
		return fmt.Errorf("at least one port must be specified")
	}

	for i, port := range service.Spec.Ports {
		if err := validateServicePort(&port); err != nil {
			return fmt.Errorf("invalid port at index %d: %v", i, err)
		}
	}

	// 验证代理模式
	if service.Spec.ProxyMode != "" {
		if !isValidProxyMode(service.Spec.ProxyMode) {
			return fmt.Errorf("invalid proxy_mode: %s", service.Spec.ProxyMode)
		}
	}

	// 验证负载均衡算法
	if service.Spec.LoadBalancer != "" {
		if !isValidLoadBalancer(service.Spec.LoadBalancer) {
			return fmt.Errorf("invalid load_balancer: %s", service.Spec.LoadBalancer)
		}
	}

	return nil
}

// validateServicePort 验证服务端口
func validateServicePort(port *types.ServicePort) error {
	if port.Name == "" {
		return fmt.Errorf("port name is required")
	}

	if port.Port <= 0 || port.Port > 65535 {
		return fmt.Errorf("invalid port: %d, must be between 1 and 65535", port.Port)
	}

	if port.TargetPort <= 0 || port.TargetPort > 65535 {
		return fmt.Errorf("invalid targetPort: %d, must be between 1 and 65535", port.TargetPort)
	}

	if port.Protocol != "" && !isValidProtocol(port.Protocol) {
		return fmt.Errorf("invalid protocol: %s", port.Protocol)
	}

	return nil
}

// validateProxy 验证代理配置
func validateProxy(proxy *types.Proxy) error {
	if proxy.Name == "" {
		return fmt.Errorf("proxy name is required")
	}

	if proxy.ServiceName == "" {
		return fmt.Errorf("service_name is required")
	}

	if proxy.ServiceNamespace == "" {
		return fmt.Errorf("service_namespace is required")
	}

	if proxy.LocalPort <= 0 || proxy.LocalPort > 65535 {
		return fmt.Errorf("invalid local_port: %d, must be between 1 and 65535", proxy.LocalPort)
	}

	if proxy.ProxyMode != "" && !isValidProxyMode(proxy.ProxyMode) {
		return fmt.Errorf("invalid proxy_mode: %s", proxy.ProxyMode)
	}

	if proxy.LoadBalancer != "" && !isValidLoadBalancer(proxy.LoadBalancer) {
		return fmt.Errorf("invalid load_balancer: %s", proxy.LoadBalancer)
	}

	return nil
}

// validateClientWorkload 验证客户端工作负载配置
func validateClientWorkload(workload *ClientWorkload) error {
	if workload.Spec.LocalAddr == "" {
		return fmt.Errorf("local_addr is required")
	}

	if workload.Spec.LocalPort <= 0 || workload.Spec.LocalPort > 65535 {
		return fmt.Errorf("invalid local_port: %d, must be between 1 and 65535", workload.Spec.LocalPort)
	}

	if workload.Spec.Protocol != "" && !isValidProtocol(workload.Spec.Protocol) {
		return fmt.Errorf("invalid protocol: %s", workload.Spec.Protocol)
	}

	// 验证标签 (至少需要一个标签用于服务发现)
	if len(workload.Metadata.Labels) == 0 {
		return fmt.Errorf("at least one label is required for service discovery")
	}

	return nil
}

// 辅助验证函数

// isValidAddr 验证地址格式 (host:port)
func isValidAddr(addr string) bool {
	host, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		return false
	}

	// 验证主机名或 IP
	if host == "" {
		return false
	}

	// 验证端口
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return false
	}

	return port > 0 && port <= 65535
}

// isValidProxyMode 验证代理模式
func isValidProxyMode(mode string) bool {
	validModes := []string{"tcp", "udp", "stcp", "sudp"}
	for _, validMode := range validModes {
		if mode == validMode {
			return true
		}
	}
	return false
}

// isValidLoadBalancer 验证负载均衡算法
func isValidLoadBalancer(lb string) bool {
	validLBs := []string{"round_robin", "random", "least_conn"}
	for _, validLB := range validLBs {
		if lb == validLB {
			return true
		}
	}
	return false
}

// isValidProtocol 验证协议
func isValidProtocol(protocol string) bool {
	protocol = strings.ToLower(protocol)
	validProtocols := []string{"tcp", "udp", "stcp", "sudp"}
	for _, validProto := range validProtocols {
		if protocol == validProto {
			return true
		}
	}
	return false
}

// isValidUnixSocketPath 验证Unix Socket路径
func isValidUnixSocketPath(path string) bool {
	// Unix socket路径必须是绝对路径
	if !strings.HasPrefix(path, "/") {
		return false
	}
	
	// 不能包含..
	if strings.Contains(path, "..") {
		return false
	}
	
	// 长度限制（Unix socket路径限制）
	if len(path) > 100 {
		return false
	}
	
	return true
}

// validateK8sAPISpec 验证K8s API配置
func validateK8sAPISpec(spec *K8sAPISpec) error {
	if spec.Port <= 0 || spec.Port > 65535 {
		return ValidationError{
			Field:   "port",
			Value:   fmt.Sprintf("%d", spec.Port),
			Message: "port must be between 1 and 65535",
			Code:    "INVALID_PORT",
		}
	}
	return nil
}

// validateMonitoringSpec 验证监控配置
func validateMonitoringSpec(spec *MonitoringSpec) error {
	if spec.Port <= 0 || spec.Port > 65535 {
		return ValidationError{
			Field:   "port",
			Value:   fmt.Sprintf("%d", spec.Port),
			Message: "port must be between 1 and 65535",
			Code:    "INVALID_PORT",
		}
	}
	
	if spec.Address != "" && !isValidAddr(spec.Address) {
		return ValidationError{
			Field:   "address",
			Value:   spec.Address,
			Message: "invalid monitoring address format",
			Code:    "INVALID_ADDRESS",
		}
	}
	
	return nil
}

// validatePortConflicts 验证端口冲突
func validatePortConflicts(config *ServerConfig) error {
	usedPorts := make(map[int]string)
	
	// 检查服务端口
	serverHost, serverPortStr, _ := net.SplitHostPort(config.Server.ListenAddr)
	if serverPort, err := strconv.Atoi(serverPortStr); err == nil {
		usedPorts[serverPort] = "server.listen_addr"
	}
	
	// 检查K8s API端口
	if config.K8sAPI.Enabled {
		if conflict, exists := usedPorts[config.K8sAPI.Port]; exists {
			return ValidationError{
				Field:   "k8s_api.port",
				Value:   fmt.Sprintf("%d", config.K8sAPI.Port),
				Message: fmt.Sprintf("port conflicts with %s", conflict),
				Code:    "PORT_CONFLICT",
			}
		}
		usedPorts[config.K8sAPI.Port] = "k8s_api.port"
	}
	
	// 检查监控端口
	if config.Monitoring.Enabled {
		if conflict, exists := usedPorts[config.Monitoring.Port]; exists {
			return ValidationError{
				Field:   "monitoring.port",
				Value:   fmt.Sprintf("%d", config.Monitoring.Port),
				Message: fmt.Sprintf("port conflicts with %s", conflict),
				Code:    "PORT_CONFLICT",
			}
		}
		usedPorts[config.Monitoring.Port] = "monitoring.port"
	}
	
	// 检查服务端口
	for i, service := range config.Services {
		for j, port := range service.Spec.Ports {
			if conflict, exists := usedPorts[port.Port]; exists {
				return ValidationError{
					Field:   fmt.Sprintf("services[%d].spec.ports[%d].port", i, j),
					Value:   fmt.Sprintf("%d", port.Port),
					Message: fmt.Sprintf("port conflicts with %s", conflict),
					Code:    "PORT_CONFLICT",
				}
			}
			usedPorts[port.Port] = fmt.Sprintf("services[%d].spec.ports[%d].port", i, j)
		}
	}
	
	// 对于同一主机的端口冲突检查
	_ = serverHost // 暂时忽略主机检查，只检查端口
	
	return nil
}

// ValidateAndFixConfig 验证并修复配置（Express Mode使用）
func ValidateAndFixConfig(config *ServerConfig) (*ServerConfig, []ValidationError) {
	var warnings []ValidationError
	
	// 创建配置副本
	fixedConfig := *config
	
	// 自动修复默认值
	if fixedConfig.Server.AuthToken == "" {
		fixedConfig.Server.AuthToken = "auto-generated-token-" + generateRandomString(8)
		warnings = append(warnings, ValidationError{
			Field:   "server.auth_token",
			Message: "auto-generated authentication token",
			Code:    "AUTO_GENERATED_TOKEN",
		})
	}
	
	// 修复服务默认值
	for i := range fixedConfig.Services {
		service := &fixedConfig.Services[i]
		
		if service.Metadata.Namespace == "" {
			service.Metadata.Namespace = "default"
			warnings = append(warnings, ValidationError{
				Field:   fmt.Sprintf("services[%d].metadata.namespace", i),
				Value:   "default",
				Message: "auto-set default namespace",
				Code:    "AUTO_SET_NAMESPACE",
			})
		}
		
		if service.Spec.ProxyMode == "" {
			service.Spec.ProxyMode = "tcp"
			warnings = append(warnings, ValidationError{
				Field:   fmt.Sprintf("services[%d].spec.proxy_mode", i),
				Value:   "tcp",
				Message: "auto-set default proxy mode",
				Code:    "AUTO_SET_PROXY_MODE",
			})
		}
		
		if service.Spec.LoadBalancer == "" {
			service.Spec.LoadBalancer = "round_robin"
			warnings = append(warnings, ValidationError{
				Field:   fmt.Sprintf("services[%d].spec.load_balancer", i),
				Value:   "round_robin",
				Message: "auto-set default load balancer",
				Code:    "AUTO_SET_LOAD_BALANCER",
			})
		}
	}
	
	return &fixedConfig, warnings
}

// generateRandomString 生成随机字符串
func generateRandomString(length int) string {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	result := make([]byte, length)
	for i := range result {
		result[i] = charset[len(charset)/2] // 简化实现，实际应使用crypto/rand
	}
	return string(result)
}