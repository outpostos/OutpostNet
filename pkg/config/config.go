package config

import (
	"time"
	"github.com/outpostos/OutpostNet/pkg/types"
)

// ServerConfig outpostnet-server 配置
type ServerConfig struct {
	Server     ServerSpec      `yaml:"server" json:"server"`
	Services   []types.Service `yaml:"services" json:"services"`
	K8sAPI     K8sAPISpec      `yaml:"k8s_api,omitempty" json:"k8s_api,omitempty"`
	Monitoring MonitoringSpec  `yaml:"monitoring,omitempty" json:"monitoring,omitempty"`
	
	// Phase 2 新增：区域配置（可选）
	Region   *types.RegionConfig `yaml:"region,omitempty" json:"region,omitempty"`
}

// ServerSpec 服务端配置规格
type ServerSpec struct {
	ListenAddr   string `yaml:"listen_addr" json:"listen_addr"`
	AuthToken    string `yaml:"auth_token" json:"auth_token"`
	
	// Unix Socket 配置 - 如果配置了路径就自动启用双协议监听
	UnixSockPath string `yaml:"unix_sock_path,omitempty" json:"unix_sock_path,omitempty"`
	
	// 证书配置 (STCP 模式使用)
	TLS *TLSConfig `yaml:"tls,omitempty" json:"tls,omitempty"`
}

// TLSConfig TLS/证书配置
type TLSConfig struct {
	CertFile   string `yaml:"cert_file" json:"cert_file"`
	KeyFile    string `yaml:"key_file" json:"key_file"`
	CAFile     string `yaml:"ca_file" json:"ca_file"`
	TrustBundle string `yaml:"trust_bundle,omitempty" json:"trust_bundle,omitempty"`
}

// K8sAPISpec K8s 兼容 API 配置
type K8sAPISpec struct {
	Enabled bool `yaml:"enabled" json:"enabled"`
	Port    int  `yaml:"port" json:"port"`
}

// MonitoringSpec 监控配置
type MonitoringSpec struct {
	Enabled           bool   `yaml:"enabled" json:"enabled"`
	Address           string `yaml:"address,omitempty" json:"address,omitempty"`
	Port              int    `yaml:"port" json:"port"`
	PrometheusEnabled bool   `yaml:"prometheus_enabled" json:"prometheus_enabled"`
	AlertsEnabled     bool   `yaml:"alerts_enabled" json:"alerts_enabled"`
	MetricsPath       string `yaml:"metrics_path,omitempty" json:"metrics_path,omitempty"`
}

// ClientConfig outpostnet-client 配置
type ClientConfig struct {
	Client      ClientSpec         `yaml:"client" json:"client"`
	Proxies     []types.Proxy      `yaml:"proxies,omitempty" json:"proxies,omitempty"`
	
	// Register 模式下的工作负载配置 (K8s Pod 风格)
	Workloads   []ClientWorkload   `yaml:"workloads,omitempty" json:"workloads,omitempty"`
	
	// 健康检查配置
	HealthCheck HealthCheckConfig  `yaml:"health_check,omitempty" json:"health_check,omitempty"`
}

// ClientSpec 客户端配置规格
type ClientSpec struct {
	ServerAddr string `yaml:"server_addr" json:"server_addr"`
	AuthToken  string `yaml:"auth_token" json:"auth_token"`
	Mode       string `yaml:"mode" json:"mode"` // proxy 或 register
	
	// Unix Socket 连接配置
	ConnProto  string `yaml:"conn_proto,omitempty" json:"conn_proto,omitempty"` // "tcp"|"unix"|"auto"
	
	// 证书配置 (STCP 模式使用)
	TLS *TLSConfig `yaml:"tls,omitempty" json:"tls,omitempty"`
}

// ClientWorkload Register 模式下的工作负载定义 (K8s Pod 风格)
type ClientWorkload struct {
	Metadata ClientWorkloadMetadata `yaml:"metadata" json:"metadata"`
	Spec     ClientWorkloadSpec     `yaml:"spec" json:"spec"`
}

// ClientWorkloadMetadata 工作负载元数据
type ClientWorkloadMetadata struct {
	Labels map[string]string `yaml:"labels" json:"labels"`
}

// ClientWorkloadSpec 工作负载规格
type ClientWorkloadSpec struct {
	LocalAddr string `yaml:"local_addr" json:"local_addr"`
	LocalPort int    `yaml:"local_port" json:"local_port"`
	Protocol  string `yaml:"protocol,omitempty" json:"protocol,omitempty"`
}

// HealthCheckConfig 健康检查配置
type HealthCheckConfig struct {
	Enabled  bool   `yaml:"enabled" json:"enabled"`
	Interval string `yaml:"interval,omitempty" json:"interval,omitempty"` // 如 "10s"
	Timeout  string `yaml:"timeout,omitempty" json:"timeout,omitempty"`   // 如 "5s"
}

// DefaultServerConfig 返回服务端默认配置
func DefaultServerConfig() *ServerConfig {
	return &ServerConfig{
		Server: ServerSpec{
			ListenAddr: "0.0.0.0:7629",
			AuthToken:  "outpostnet-default-token",
		},
		Services: []types.Service{
			{
				Metadata: types.ServiceMetadata{
					Name:      "default-service",
					Namespace: "default",
					Labels: map[string]string{
						"app":         "default",
						"managed-by":  "outpostnet",
						"environment": "development",
					},
				},
				Spec: types.ServiceSpec{
					Selector: map[string]string{
						"app": "default",
					},
					Ports: []types.ServicePort{
						{
							Name:       "http",
							Port:       80,
							TargetPort: 8081,
							Protocol:   "tcp",
						},
					},
					ProxyMode:    "tcp",
					LoadBalancer: "round_robin",
				},
			},
		},
		K8sAPI: K8sAPISpec{
			Enabled: true,
			Port:    8443,
		},
	}
}

// DefaultClientConfig 返回客户端默认配置
func DefaultClientConfig(mode string) *ClientConfig {
	config := &ClientConfig{
		Client: ClientSpec{
			ServerAddr: "127.0.0.1:7629",
			AuthToken:  "outpostnet-default-token",
			Mode:       mode,
		},
	}

	if mode == "proxy" {
		config.Proxies = []types.Proxy{
			{
				Name:             "default-proxy",
				ServiceName:      "default-service",
				ServiceNamespace: "default",
				LocalPort:        8081,
				ProxyMode:        "tcp",
				Selector: map[string]string{
					"app": "default",
				},
				LoadBalancer: "round_robin",
			},
		}
	} else if mode == "register" {
		config.Workloads = []ClientWorkload{
			{
				Metadata: ClientWorkloadMetadata{
					Labels: map[string]string{
						"app":         "default",
						"environment": "development",
						"node":        "localhost",
					},
				},
				Spec: ClientWorkloadSpec{
					LocalAddr: "127.0.0.1",
					LocalPort: 8081,
					Protocol:  "tcp",
				},
			},
		}
	}

	return config
}

// DefaultRegionConfig 返回默认区域配置 (Phase 2)
func DefaultRegionConfig(regionID, nodeID string) *types.RegionConfig {
	return &types.RegionConfig{
		Enabled:  false, // 默认关闭，需要显式启用
		RegionID: regionID,
		NodeID:   nodeID,
		Neighbors: []types.NeighborNode{},
		ServiceSync: types.ServiceSyncConfig{
			SyncInterval: 30 * time.Second,
			RetryCount:   3,
			HealthCheck:  true,
			CacheTimeout: 5 * time.Minute,
		},
	}
}

// ServerConfigWithRegion 创建带区域配置的服务端配置
func ServerConfigWithRegion(regionID, nodeID string, neighbors []types.NeighborNode) *ServerConfig {
	config := DefaultServerConfig()
	
	// 添加区域配置
	config.Region = DefaultRegionConfig(regionID, nodeID)
	config.Region.Enabled = true
	config.Region.Neighbors = neighbors
	
	// 为服务添加地理元数据
	for i := range config.Services {
		config.Services[i].Metadata.Region = regionID
		config.Services[i].Metadata.Site = nodeID // 简化：使用nodeID作为siteID
		if config.Services[i].Metadata.Labels == nil {
			config.Services[i].Metadata.Labels = make(map[string]string)
		}
		config.Services[i].Metadata.Labels["outpostnet.io/region"] = regionID
		config.Services[i].Metadata.Labels["outpostnet.io/node"] = nodeID
	}
	
	return config
}