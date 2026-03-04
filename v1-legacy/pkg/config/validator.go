// 配置验证器
package config

import (
	"fmt"
	"net"
	"os"
	"strings"
	"time"
)

// Validator 配置验证器
type Validator struct {
	errors   []string
	warnings []string
}

// NewValidator 创建配置验证器
func NewValidator() *Validator {
	return &Validator{
		errors:   make([]string, 0),
		warnings: make([]string, 0),
	}
}

// ValidateFile 验证配置文件
func (v *Validator) ValidateFile(filename string) error {
	// 加载配置文件
	deployment, err := LoadDeployment(filename)
	if err != nil {
		return fmt.Errorf("failed to load deployment: %w", err)
	}

	// 验证配置
	return v.Validate(deployment)
}

// Validate 验证部署配置
func (v *Validator) Validate(deployment *Deployment) error {
	v.errors = make([]string, 0)
	v.warnings = make([]string, 0)

	// 验证基本信息
	v.validateBasicInfo(deployment)

	// 验证集群配置
	v.validateCluster(&deployment.Cluster)

	// 验证服务配置
	v.validateServices(deployment.Services)

	// 验证拓扑配置
	v.validateTopology(&deployment.Topology)

	// 验证安全配置
	v.validateSecurity(&deployment.Security)

	// 检查是否有错误
	if len(v.errors) > 0 {
		return fmt.Errorf("validation failed with %d errors:\n%s", 
			len(v.errors), strings.Join(v.errors, "\n"))
	}

	// 输出警告
	if len(v.warnings) > 0 {
		fmt.Printf("Validation completed with %d warnings:\n%s\n", 
			len(v.warnings), strings.Join(v.warnings, "\n"))
	}

	return nil
}

// validateBasicInfo 验证基本信息
func (v *Validator) validateBasicInfo(deployment *Deployment) {
	if deployment.Name == "" {
		v.addError("deployment name is required")
	}

	if deployment.Version == "" {
		v.addWarning("deployment version is empty")
	}

	if deployment.Environment == "" {
		v.addWarning("deployment environment is empty, defaults to 'dev'")
	} else {
		validEnvs := []string{"dev", "staging", "production"}
		if !contains(validEnvs, deployment.Environment) {
			v.addWarning(fmt.Sprintf("unknown environment '%s', valid values: %s", 
				deployment.Environment, strings.Join(validEnvs, ", ")))
		}
	}
}

// validateCluster 验证集群配置
func (v *Validator) validateCluster(cluster *ClusterConfig) {
	// 验证集群类型
	validTypes := []string{"standalone", "region-mesh", "multi-cluster"}
	if !contains(validTypes, cluster.Type) {
		v.addError(fmt.Sprintf("invalid cluster type '%s', valid values: %s", 
			cluster.Type, strings.Join(validTypes, ", ")))
	}

	// 验证节点配置
	if len(cluster.Nodes) == 0 {
		v.addError("at least one node is required")
		return
	}

	nodeIDs := make(map[string]bool)
	primaryNodes := 0

	for i, node := range cluster.Nodes {
		// 验证节点ID唯一性
		if node.NodeID == "" {
			v.addError(fmt.Sprintf("node[%d]: node_id is required", i))
		} else if nodeIDs[node.NodeID] {
			v.addError(fmt.Sprintf("node[%d]: duplicate node_id '%s'", i, node.NodeID))
		} else {
			nodeIDs[node.NodeID] = true
		}

		// 验证节点角色
		if node.Role == "primary" {
			primaryNodes++
		}

		// 验证网络配置
		v.validateNodeNetwork(&node.Network, i)

		// 验证地理位置
		v.validateLocation(&node.Location, fmt.Sprintf("node[%d]", i))
	}

	// 检查主节点数量
	if primaryNodes == 0 {
		v.addWarning("no primary node found, first node will be treated as primary")
	} else if primaryNodes > 1 {
		v.addWarning(fmt.Sprintf("multiple primary nodes found (%d), may cause conflicts", primaryNodes))
	}

	// 验证区域配置
	if cluster.Region.Enabled {
		v.validateRegionConfig(&cluster.Region)
	}
}

// validateNodeNetwork 验证节点网络配置
func (v *Validator) validateNodeNetwork(network *NetworkConfig, nodeIndex int) {
	prefix := fmt.Sprintf("node[%d].network", nodeIndex)

	// 验证监听地址
	if network.ListenAddr == "" {
		v.addError(fmt.Sprintf("%s: listen_addr is required", prefix))
	} else {
		if _, err := net.ResolveTCPAddr("tcp", network.ListenAddr+":80"); err != nil {
			v.addError(fmt.Sprintf("%s: invalid listen_addr '%s': %v", prefix, network.ListenAddr, err))
		}
	}

	// 验证端口配置
	v.validatePorts(&network.Ports, prefix)

	// 验证TLS配置
	if network.TLS.Enabled {
		v.validateTLS(&network.TLS, prefix)
	}
}

// validatePorts 验证端口配置
func (v *Validator) validatePorts(ports *PortConfig, prefix string) {
	if ports.Control <= 0 || ports.Control > 65535 {
		v.addError(fmt.Sprintf("%s.ports: invalid control port %d", prefix, ports.Control))
	}

	if ports.RegionMesh != 0 && (ports.RegionMesh <= 0 || ports.RegionMesh > 65535) {
		v.addError(fmt.Sprintf("%s.ports: invalid region_mesh port %d", prefix, ports.RegionMesh))
	}

	if ports.Metrics != 0 && (ports.Metrics <= 0 || ports.Metrics > 65535) {
		v.addError(fmt.Sprintf("%s.ports: invalid metrics port %d", prefix, ports.Metrics))
	}

	if ports.Web != 0 && (ports.Web <= 0 || ports.Web > 65535) {
		v.addError(fmt.Sprintf("%s.ports: invalid web port %d", prefix, ports.Web))
	}

	// 检查端口冲突
	portMap := make(map[int]string)
	checkPortConflict := func(port int, name string) {
		if port != 0 {
			if existing, exists := portMap[port]; exists {
				v.addError(fmt.Sprintf("%s.ports: port %d conflict between %s and %s", 
					prefix, port, name, existing))
			} else {
				portMap[port] = name
			}
		}
	}

	checkPortConflict(ports.Control, "control")
	checkPortConflict(ports.RegionMesh, "region_mesh")
	checkPortConflict(ports.Metrics, "metrics")
	checkPortConflict(ports.Web, "web")
}

// validateTLS 验证TLS配置
func (v *Validator) validateTLS(tls *NodeTLSConfig, prefix string) {
	if tls.CertFile == "" {
		v.addError(fmt.Sprintf("%s.tls: cert_file is required when TLS is enabled", prefix))
	} else if _, err := os.Stat(tls.CertFile); os.IsNotExist(err) {
		v.addError(fmt.Sprintf("%s.tls: cert_file '%s' does not exist", prefix, tls.CertFile))
	}

	if tls.KeyFile == "" {
		v.addError(fmt.Sprintf("%s.tls: key_file is required when TLS is enabled", prefix))
	} else if _, err := os.Stat(tls.KeyFile); os.IsNotExist(err) {
		v.addError(fmt.Sprintf("%s.tls: key_file '%s' does not exist", prefix, tls.KeyFile))
	}

	if tls.CAFile != "" {
		if _, err := os.Stat(tls.CAFile); os.IsNotExist(err) {
			v.addError(fmt.Sprintf("%s.tls: ca_file '%s' does not exist", prefix, tls.CAFile))
		}
	}
}

// validateLocation 验证地理位置配置
func (v *Validator) validateLocation(location *LocationConfig, prefix string) {
	if location.Region == "" {
		v.addWarning(fmt.Sprintf("%s.location: region is empty", prefix))
	}

	if location.Zone == "" {
		v.addWarning(fmt.Sprintf("%s.location: zone is empty", prefix))
	}

	if location.Site == "" {
		v.addWarning(fmt.Sprintf("%s.location: site is empty", prefix))
	}
}

// validateRegionConfig 验证区域配置
func (v *Validator) validateRegionConfig(region *RegionConfig) {
	if region.RegionID == "" {
		v.addError("region.region_id is required when region is enabled")
	}

	// 验证服务同步配置
	if region.ServiceSync.Interval != "" {
		if _, err := time.ParseDuration(region.ServiceSync.Interval); err != nil {
			v.addError(fmt.Sprintf("region.service_sync.interval: invalid duration '%s': %v", 
				region.ServiceSync.Interval, err))
		}
	}

	if region.ServiceSync.Timeout != "" {
		if _, err := time.ParseDuration(region.ServiceSync.Timeout); err != nil {
			v.addError(fmt.Sprintf("region.service_sync.timeout: invalid duration '%s': %v", 
				region.ServiceSync.Timeout, err))
		}
	}

	// 验证负载均衡配置
	validStrategies := []string{"round_robin", "weighted", "least_connections", 
		"consistent_hash", "geographic_aware", "health_aware", "latency_aware"}
	if region.LoadBalancer.Strategy != "" && !contains(validStrategies, region.LoadBalancer.Strategy) {
		v.addWarning(fmt.Sprintf("region.load_balancer.strategy: unknown strategy '%s', valid values: %s", 
			region.LoadBalancer.Strategy, strings.Join(validStrategies, ", ")))
	}

	// 验证健康检查配置
	if region.HealthCheck.Interval != "" {
		if _, err := time.ParseDuration(region.HealthCheck.Interval); err != nil {
			v.addError(fmt.Sprintf("region.health_check.interval: invalid duration '%s': %v", 
				region.HealthCheck.Interval, err))
		}
	}

	if region.HealthCheck.Timeout != "" {
		if _, err := time.ParseDuration(region.HealthCheck.Timeout); err != nil {
			v.addError(fmt.Sprintf("region.health_check.timeout: invalid duration '%s': %v", 
				region.HealthCheck.Timeout, err))
		}
	}

	// TODO: Update validation for DeploymentHealthCheckConfig
	// if region.HealthCheck.MaxFailures <= 0 {
	//	v.addError("region.health_check.max_failures must be greater than 0")
	// }

	// if region.HealthCheck.PhiThreshold <= 0 {
	//	v.addError("region.health_check.phi_threshold must be greater than 0")
	// }
}

// validateServices 验证服务配置
func (v *Validator) validateServices(services []ServiceConfig) {
	serviceNames := make(map[string]bool)

	for i, service := range services {
		prefix := fmt.Sprintf("service[%d]", i)

		// 验证服务名称唯一性
		serviceKey := fmt.Sprintf("%s.%s", service.Name, service.Namespace)
		if serviceNames[serviceKey] {
			v.addError(fmt.Sprintf("%s: duplicate service '%s' in namespace '%s'", 
				prefix, service.Name, service.Namespace))
		} else {
			serviceNames[serviceKey] = true
		}

		// 验证基本字段
		if service.Name == "" {
			v.addError(fmt.Sprintf("%s: service name is required", prefix))
		}

		if service.Namespace == "" {
			v.addError(fmt.Sprintf("%s: service namespace is required", prefix))
		}

		// 验证服务类型
		validTypes := []string{"NodePort", "ClusterIP"}
		if !contains(validTypes, service.Type) {
			v.addError(fmt.Sprintf("%s: invalid service type '%s', valid values: %s", 
				prefix, service.Type, strings.Join(validTypes, ", ")))
		}

		// 验证代理模式
		validProxyModes := []string{"tcp", "udp", "stcp", "sudp"}
		if !contains(validProxyModes, service.ProxyMode) {
			v.addError(fmt.Sprintf("%s: invalid proxy_mode '%s', valid values: %s", 
				prefix, service.ProxyMode, strings.Join(validProxyModes, ", ")))
		}

		// 验证端口配置
		if len(service.Ports) == 0 {
			v.addError(fmt.Sprintf("%s: at least one port is required", prefix))
		} else {
			for j, port := range service.Ports {
				v.validateServicePort(&port, fmt.Sprintf("%s.port[%d]", prefix, j))
			}
		}

		// 验证后端配置
		for j, backend := range service.Backends {
			v.validateBackend(&backend, fmt.Sprintf("%s.backend[%d]", prefix, j))
		}
	}
}

// validateServicePort 验证服务端口
func (v *Validator) validateServicePort(port *ServicePort, prefix string) {
	if port.Name == "" {
		v.addError(fmt.Sprintf("%s: port name is required", prefix))
	}

	if port.Port <= 0 || port.Port > 65535 {
		v.addError(fmt.Sprintf("%s: invalid port %d", prefix, port.Port))
	}

	if port.TargetPort <= 0 || port.TargetPort > 65535 {
		v.addError(fmt.Sprintf("%s: invalid target_port %d", prefix, port.TargetPort))
	}

	if port.NodePort != 0 && (port.NodePort <= 0 || port.NodePort > 65535) {
		v.addError(fmt.Sprintf("%s: invalid node_port %d", prefix, port.NodePort))
	}

	validProtocols := []string{"tcp", "udp", "stcp", "sudp"}
	if !contains(validProtocols, port.Protocol) {
		v.addError(fmt.Sprintf("%s: invalid protocol '%s', valid values: %s", 
			prefix, port.Protocol, strings.Join(validProtocols, ", ")))
	}
}

// validateBackend 验证后端配置
func (v *Validator) validateBackend(backend *BackendConfig, prefix string) {
	if backend.Name == "" {
		v.addError(fmt.Sprintf("%s: backend name is required", prefix))
	}

	if backend.Address == "" {
		v.addError(fmt.Sprintf("%s: backend address is required", prefix))
	} else {
		if net.ParseIP(backend.Address) == nil {
			if _, err := net.LookupHost(backend.Address); err != nil {
				v.addWarning(fmt.Sprintf("%s: backend address '%s' cannot be resolved: %v", 
					prefix, backend.Address, err))
			}
		}
	}

	if backend.Port <= 0 || backend.Port > 65535 {
		v.addError(fmt.Sprintf("%s: invalid backend port %d", prefix, backend.Port))
	}

	if backend.Weight < 0 {
		v.addError(fmt.Sprintf("%s: backend weight cannot be negative", prefix))
	}
}

// validateTopology 验证拓扑配置
func (v *Validator) validateTopology(topology *TopologyConfig) {
	// 验证连接配置
	for i, conn := range topology.Connections {
		prefix := fmt.Sprintf("topology.connection[%d]", i)

		if conn.From == "" {
			v.addError(fmt.Sprintf("%s: from is required", prefix))
		}

		if conn.To == "" {
			v.addError(fmt.Sprintf("%s: to is required", prefix))
		}

		validTypes := []string{"direct", "tunnel", "gateway"}
		if !contains(validTypes, conn.Type) {
			v.addError(fmt.Sprintf("%s: invalid connection type '%s', valid values: %s", 
				prefix, conn.Type, strings.Join(validTypes, ", ")))
		}
	}

	// 验证路由配置
	if topology.Routing.Strategy != "" {
		validStrategies := []string{"local-first", "geographic", "latency-based"}
		if !contains(validStrategies, topology.Routing.Strategy) {
			v.addWarning(fmt.Sprintf("topology.routing.strategy: unknown strategy '%s', valid values: %s", 
				topology.Routing.Strategy, strings.Join(validStrategies, ", ")))
		}
	}

	if topology.Routing.Fallback != "" {
		validFallbacks := []string{"cloud", "regional", "none"}
		if !contains(validFallbacks, topology.Routing.Fallback) {
			v.addWarning(fmt.Sprintf("topology.routing.fallback: unknown fallback '%s', valid values: %s", 
				topology.Routing.Fallback, strings.Join(validFallbacks, ", ")))
		}
	}
}

// validateSecurity 验证安全配置
func (v *Validator) validateSecurity(security *SecurityConfig) {
	// 验证认证配置
	validAuthMethods := []string{"token", "certificate", "none"}
	if !contains(validAuthMethods, security.Auth.Method) {
		v.addError(fmt.Sprintf("security.auth.method: invalid method '%s', valid values: %s", 
			security.Auth.Method, strings.Join(validAuthMethods, ", ")))
	}

	if security.Auth.Method == "token" && security.Auth.Token == "" {
		v.addError("security.auth.token is required when auth method is 'token'")
	}

	// 验证全局TLS配置
	if security.TLS.Enabled {
		if security.TLS.CA.CertFile == "" {
			v.addError("security.tls.ca.cert_file is required when TLS is enabled")
		} else if _, err := os.Stat(security.TLS.CA.CertFile); os.IsNotExist(err) {
			v.addError(fmt.Sprintf("security.tls.ca.cert_file '%s' does not exist", security.TLS.CA.CertFile))
		}

		if security.TLS.CA.KeyFile == "" {
			v.addError("security.tls.ca.key_file is required when TLS is enabled")
		} else if _, err := os.Stat(security.TLS.CA.KeyFile); os.IsNotExist(err) {
			v.addError(fmt.Sprintf("security.tls.ca.key_file '%s' does not exist", security.TLS.CA.KeyFile))
		}

		// 验证证书配置
		for i, cert := range security.TLS.Certs {
			prefix := fmt.Sprintf("security.tls.cert[%d]", i)

			if cert.Name == "" {
				v.addError(fmt.Sprintf("%s: cert name is required", prefix))
			}

			if cert.CertFile == "" {
				v.addError(fmt.Sprintf("%s: cert_file is required", prefix))
			} else if _, err := os.Stat(cert.CertFile); os.IsNotExist(err) {
				v.addError(fmt.Sprintf("%s: cert_file '%s' does not exist", prefix, cert.CertFile))
			}

			if cert.KeyFile == "" {
				v.addError(fmt.Sprintf("%s: key_file is required", prefix))
			} else if _, err := os.Stat(cert.KeyFile); os.IsNotExist(err) {
				v.addError(fmt.Sprintf("%s: key_file '%s' does not exist", prefix, cert.KeyFile))
			}
		}
	}
}

// addError 添加错误
func (v *Validator) addError(msg string) {
	v.errors = append(v.errors, fmt.Sprintf("ERROR: %s", msg))
}

// addWarning 添加警告
func (v *Validator) addWarning(msg string) {
	v.warnings = append(v.warnings, fmt.Sprintf("WARNING: %s", msg))
}

// contains 检查字符串是否在切片中
func contains(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}