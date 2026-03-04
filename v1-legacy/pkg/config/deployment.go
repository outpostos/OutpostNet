// 部署配置定义和解析
package config

import (
	"fmt"
	"os"
	"path/filepath"

	"gopkg.in/yaml.v2"
)

// Deployment 部署配置
type Deployment struct {
	// 基本信息
	Name        string `yaml:"name"`
	Version     string `yaml:"version"`
	Environment string `yaml:"environment"` // dev, staging, production
	
	// 集群配置
	Cluster ClusterConfig `yaml:"cluster"`
	
	// 服务定义
	Services []ServiceConfig `yaml:"services"`
	
	// 网络拓扑
	Topology TopologyConfig `yaml:"topology"`
	
	// 安全配置
	Security SecurityConfig `yaml:"security"`
	
	// 监控配置
	Monitoring MonitoringConfig `yaml:"monitoring"`
}

// ClusterConfig 集群配置
type ClusterConfig struct {
	// 集群类型
	Type string `yaml:"type"` // standalone, region-mesh, multi-cluster
	
	// 节点配置
	Nodes []NodeConfig `yaml:"nodes"`
	
	// 区域配置 (Phase 2)
	Region RegionConfig `yaml:"region,omitempty"`
}

// NodeConfig 节点配置
type NodeConfig struct {
	// 节点标识
	NodeID   string `yaml:"node_id"`
	Name     string `yaml:"name"`
	Role     string `yaml:"role"` // primary, secondary, edge
	
	// 网络配置
	Network NetworkConfig `yaml:"network"`
	
	// 地理位置
	Location LocationConfig `yaml:"location"`
	
	// 资源限制
	Resources ResourceConfig `yaml:"resources,omitempty"`
}

// NetworkConfig 网络配置
type NetworkConfig struct {
	// 监听地址
	ListenAddr string `yaml:"listen_addr"`
	
	// 外部地址
	ExternalAddr string `yaml:"external_addr,omitempty"`
	
	// 端口配置
	Ports PortConfig `yaml:"ports"`
	
	// TLS配置
	TLS NodeTLSConfig `yaml:"tls,omitempty"`
}

// PortConfig 端口配置
type PortConfig struct {
	// 控制端口
	Control int `yaml:"control"`
	
	// 区域网格端口
	RegionMesh int `yaml:"region_mesh,omitempty"`
	
	// 监控端口
	Metrics int `yaml:"metrics,omitempty"`
	
	// Web界面端口
	Web int `yaml:"web,omitempty"`
}

// LocationConfig 地理位置配置
type LocationConfig struct {
	Region string `yaml:"region"`
	Zone   string `yaml:"zone"`
	Site   string `yaml:"site"`
}

// ResourceConfig 资源配置
type ResourceConfig struct {
	CPU    string `yaml:"cpu,omitempty"`
	Memory string `yaml:"memory,omitempty"`
	Disk   string `yaml:"disk,omitempty"`
}

// RegionConfig 区域配置
type RegionConfig struct {
	Enabled   bool   `yaml:"enabled"`
	RegionID  string `yaml:"region_id"`
	
	// 服务同步配置
	ServiceSync ServiceSyncConfig `yaml:"service_sync"`
	
	// 负载均衡配置
	LoadBalancer LoadBalancerConfig `yaml:"load_balancer"`
	
	// 健康检查配置
	HealthCheck DeploymentHealthCheckConfig `yaml:"health_check"`
}

// ServiceSyncConfig 服务同步配置
type ServiceSyncConfig struct {
	Interval string `yaml:"interval"`
	Timeout  string `yaml:"timeout"`
}

// LoadBalancerConfig 负载均衡配置
type LoadBalancerConfig struct {
	Strategy string `yaml:"strategy"`
	Enabled  bool   `yaml:"enabled"`
}

// DeploymentHealthCheckConfig 部署级健康检查配置
type DeploymentHealthCheckConfig struct {
	Enabled      bool   `yaml:"enabled"`
	Interval     string `yaml:"interval"`
	Timeout      string `yaml:"timeout"`
	MaxFailures  int    `yaml:"max_failures"`
	PhiThreshold float64 `yaml:"phi_threshold"`
}

// ServiceConfig 服务配置
type ServiceConfig struct {
	// 服务基本信息
	Name      string `yaml:"name"`
	Namespace string `yaml:"namespace"`
	Type      string `yaml:"type"` // NodePort, ClusterIP
	
	// 服务选择器
	Selector map[string]string `yaml:"selector"`
	
	// 端口配置
	Ports []ServicePort `yaml:"ports"`
	
	// 代理模式
	ProxyMode string `yaml:"proxy_mode"` // tcp, udp, stcp, sudp
	
	// 负载均衡
	LoadBalancer string `yaml:"load_balancer"`
	
	// 后端配置
	Backends []BackendConfig `yaml:"backends,omitempty"`
}

// ServicePort 服务端口
type ServicePort struct {
	Name       string `yaml:"name"`
	Port       int    `yaml:"port"`
	TargetPort int    `yaml:"target_port"`
	NodePort   int    `yaml:"node_port,omitempty"`
	Protocol   string `yaml:"protocol"`
}

// BackendConfig 后端配置
type BackendConfig struct {
	Name     string            `yaml:"name"`
	Address  string            `yaml:"address"`
	Port     int               `yaml:"port"`
	Weight   int               `yaml:"weight,omitempty"`
	Labels   map[string]string `yaml:"labels,omitempty"`
	Location LocationConfig    `yaml:"location,omitempty"`
}

// TopologyConfig 拓扑配置
type TopologyConfig struct {
	// 连接关系
	Connections []ConnectionConfig `yaml:"connections"`
	
	// 路由策略
	Routing RoutingConfig `yaml:"routing"`
}

// ConnectionConfig 连接配置
type ConnectionConfig struct {
	From string `yaml:"from"`
	To   string `yaml:"to"`
	Type string `yaml:"type"` // direct, tunnel, gateway
}

// RoutingConfig 路由配置
type RoutingConfig struct {
	Strategy string `yaml:"strategy"` // local-first, geographic, latency-based
	Fallback string `yaml:"fallback"` // cloud, regional, none
}

// SecurityConfig 安全配置
type SecurityConfig struct {
	// 认证配置
	Auth AuthConfig `yaml:"auth"`
	
	// TLS配置
	TLS GlobalTLSConfig `yaml:"tls"`
}

// AuthConfig 认证配置
type AuthConfig struct {
	Method string `yaml:"method"` // token, certificate, none
	Token  string `yaml:"token,omitempty"`
}

// NodeTLSConfig TLS配置 (节点级别)
type NodeTLSConfig struct {
	Enabled  bool   `yaml:"enabled"`
	CertFile string `yaml:"cert_file,omitempty"`
	KeyFile  string `yaml:"key_file,omitempty"`
	CAFile   string `yaml:"ca_file,omitempty"`
}

// GlobalTLSConfig 全局TLS配置
type GlobalTLSConfig struct {
	Enabled bool      `yaml:"enabled"`
	CA      CAConfig  `yaml:"ca"`
	Certs   []CertConfig `yaml:"certs,omitempty"`
}

// CAConfig CA配置
type CAConfig struct {
	CertFile string `yaml:"cert_file"`
	KeyFile  string `yaml:"key_file"`
}

// CertConfig 证书配置
type CertConfig struct {
	Name     string `yaml:"name"`
	CertFile string `yaml:"cert_file"`
	KeyFile  string `yaml:"key_file"`
}

// MonitoringConfig 监控配置
type MonitoringConfig struct {
	Enabled bool              `yaml:"enabled"`
	Type    string            `yaml:"type"` // prometheus, grafana
	Config  map[string]string `yaml:"config,omitempty"`
}

// LoadDeployment 加载部署配置
func LoadDeployment(filename string) (*Deployment, error) {
	data, err := os.ReadFile(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	var deployment Deployment
	if err := yaml.Unmarshal(data, &deployment); err != nil {
		return nil, fmt.Errorf("failed to parse config file: %w", err)
	}

	// 设置默认值
	if err := setDefaults(&deployment); err != nil {
		return nil, fmt.Errorf("failed to set defaults: %w", err)
	}

	return &deployment, nil
}

// SaveDeployment 保存部署配置
func SaveDeployment(deployment *Deployment, filename string) error {
	data, err := yaml.Marshal(deployment)
	if err != nil {
		return fmt.Errorf("failed to marshal config: %w", err)
	}

	dir := filepath.Dir(filename)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}

	return os.WriteFile(filename, data, 0644)
}

// setDefaults 设置默认值
func setDefaults(deployment *Deployment) error {
	// 设置默认环境
	if deployment.Environment == "" {
		deployment.Environment = "dev"
	}

	// 设置默认集群类型
	if deployment.Cluster.Type == "" {
		deployment.Cluster.Type = "standalone"
	}

	// 为每个节点设置默认值
	for i := range deployment.Cluster.Nodes {
		node := &deployment.Cluster.Nodes[i]
		
		// 设置默认角色
		if node.Role == "" {
			if i == 0 {
				node.Role = "primary"
			} else {
				node.Role = "secondary"
			}
		}

		// 设置默认端口
		if node.Network.Ports.Control == 0 {
			node.Network.Ports.Control = 7000 + i
		}
		if node.Network.Ports.RegionMesh == 0 {
			node.Network.Ports.RegionMesh = 8000 + i
		}
		if node.Network.Ports.Metrics == 0 {
			node.Network.Ports.Metrics = 9000 + i
		}
		if node.Network.Ports.Web == 0 {
			node.Network.Ports.Web = 10000 + i
		}

		// 设置默认监听地址
		if node.Network.ListenAddr == "" {
			node.Network.ListenAddr = "0.0.0.0"
		}
	}

	// 设置默认区域配置
	if deployment.Cluster.Region.Enabled && deployment.Cluster.Region.RegionID == "" {
		deployment.Cluster.Region.RegionID = "default-region"
	}

	// 设置默认服务同步配置
	if deployment.Cluster.Region.ServiceSync.Interval == "" {
		deployment.Cluster.Region.ServiceSync.Interval = "10s"
	}
	if deployment.Cluster.Region.ServiceSync.Timeout == "" {
		deployment.Cluster.Region.ServiceSync.Timeout = "60s"
	}

	// 设置默认负载均衡策略
	if deployment.Cluster.Region.LoadBalancer.Strategy == "" {
		deployment.Cluster.Region.LoadBalancer.Strategy = "geographic_aware"
	}

	// 设置默认健康检查配置
	if deployment.Cluster.Region.HealthCheck.Interval == "" {
		deployment.Cluster.Region.HealthCheck.Interval = "30s"
	}
	if deployment.Cluster.Region.HealthCheck.Timeout == "" {
		deployment.Cluster.Region.HealthCheck.Timeout = "10s"
	}
	// TODO: Fix RegionConfig vs DeploymentHealthCheckConfig mismatch
	// if deployment.Cluster.Region.HealthCheck.MaxFailures == 0 {
	//	deployment.Cluster.Region.HealthCheck.MaxFailures = 3
	// }
	// if deployment.Cluster.Region.HealthCheck.PhiThreshold == 0 {
	//	deployment.Cluster.Region.HealthCheck.PhiThreshold = 8.0
	// }

	return nil
}