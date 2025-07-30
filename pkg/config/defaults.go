package config

import (
	"fmt"

	"github.com/outpostos/OutpostNet/pkg/types"
)

// SmartDefaults 智能默认配置生成器
type SmartDefaults struct {
	ServiceName string
	ServicePort int
	BackendAddr string
	BackendPort int
	ProxyMode   string
	Labels      map[string]string
}

// DefaultSmartDefaults 返回智能默认配置
func DefaultSmartDefaults() *SmartDefaults {
	return &SmartDefaults{
		ServiceName: "my-service",
		ServicePort: 80,
		BackendAddr: "127.0.0.1",
		BackendPort: 8080,
		ProxyMode:   "tcp",
		Labels: map[string]string{
			"app":         "my-app",
			"environment": "development",
		},
	}
}

// GenerateServerConfig 生成智能默认服务端配置
func (sd *SmartDefaults) GenerateServerConfig(port int, token string) *ServerConfig {
	config := DefaultServerConfig()
	
	// 使用传入的参数覆盖默认值
	if port > 0 {
		config.Server.ListenAddr = fmt.Sprintf("0.0.0.0:%d", port)
	}
	if token != "" {
		config.Server.AuthToken = token
	}

	// 如果提供了服务信息，添加默认服务
	if sd.ServiceName != "" {
		service := types.Service{
			Metadata: types.ServiceMetadata{
				Name:      sd.ServiceName,
				Namespace: "default",
				Labels:    sd.Labels,
			},
			Spec: types.ServiceSpec{
				Selector: sd.Labels,
				Ports: []types.ServicePort{
					{
						Name:       "main",
						Port:       sd.ServicePort,
						TargetPort: sd.BackendPort,
						Protocol:   sd.ProxyMode,
					},
				},
				ProxyMode:    sd.ProxyMode,
				LoadBalancer: "round_robin",
			},
		}
		config.Services = append(config.Services, service)
	}

	return config
}

// GenerateClientConfig 生成智能默认客户端配置
func (sd *SmartDefaults) GenerateClientConfig(mode, serverAddr, token string) *ClientConfig {
	config := DefaultClientConfig(mode)
	
	// 使用传入的参数覆盖默认值
	if serverAddr != "" {
		config.Client.ServerAddr = serverAddr
	}
	if token != "" {
		config.Client.AuthToken = token
	}

	switch mode {
	case "proxy":
		if sd.ServiceName != "" {
			proxy := types.Proxy{
				Name:             sd.ServiceName + "-proxy",
				ServiceName:      sd.ServiceName,
				ServiceNamespace: "default",
				LocalPort:        sd.ServicePort + 1000, // 代理端口偏移
				ProxyMode:        sd.ProxyMode,
				Selector:         sd.Labels,
				LoadBalancer:     "round_robin",
			}
			config.Proxies = append(config.Proxies, proxy)
		}
	case "register":
		if sd.BackendAddr != "" && sd.BackendPort > 0 {
			workload := ClientWorkload{
				Metadata: ClientWorkloadMetadata{
					Labels: sd.Labels,
				},
				Spec: ClientWorkloadSpec{
					LocalAddr: sd.BackendAddr,
					LocalPort: sd.BackendPort,
					Protocol:  sd.ProxyMode,
				},
			}
			config.Workloads = append(config.Workloads, workload)
		}
	}

	return config
}

// ApplyDefaults 应用默认配置到现有配置
func ApplyDefaults(config interface{}) interface{} {
	switch cfg := config.(type) {
	case *ServerConfig:
		return applyServerDefaults(cfg)
	case *ClientConfig:
		return applyClientDefaults(cfg)
	default:
		return config
	}
}

// applyServerDefaults 应用服务端默认配置
func applyServerDefaults(config *ServerConfig) *ServerConfig {
	defaults := DefaultServerConfig()
	
	// 只填充未设置的字段
	if config.Server.ListenAddr == "" {
		config.Server.ListenAddr = defaults.Server.ListenAddr
	}
	if config.Server.AuthToken == "" {
		config.Server.AuthToken = defaults.Server.AuthToken
	}
	
	// K8sAPI 默认值
	if !config.K8sAPI.Enabled && config.K8sAPI.Port == 0 {
		config.K8sAPI = defaults.K8sAPI
	}
	
	// Monitoring 默认值
	if config.Monitoring.Port == 0 {
		config.Monitoring.Port = defaults.Monitoring.Port
	}
	if config.Monitoring.Address == "" {
		config.Monitoring.Address = defaults.Monitoring.Address
	}
	if config.Monitoring.MetricsPath == "" {
		config.Monitoring.MetricsPath = defaults.Monitoring.MetricsPath
	}
	
	return config
}

// applyClientDefaults 应用客户端默认配置
func applyClientDefaults(config *ClientConfig) *ClientConfig {
	defaults := DefaultClientConfig(config.Client.Mode)
	
	// 只填充未设置的字段
	if config.Client.ServerAddr == "" {
		config.Client.ServerAddr = defaults.Client.ServerAddr
	}
	if config.Client.AuthToken == "" {
		config.Client.AuthToken = defaults.Client.AuthToken
	}
	if config.Client.ConnProto == "" {
		config.Client.ConnProto = defaults.Client.ConnProto
	}
	
	// HealthCheck 默认值
	if !config.HealthCheck.Enabled && config.HealthCheck.Interval == "" {
		config.HealthCheck = defaults.HealthCheck
	}
	
	return config
}

// ConfigPriority 配置优先级：命令行 > 配置文件 > 默认值
type ConfigPriority struct {
	CommandLine map[string]interface{}
	ConfigFile  interface{}
	Defaults    interface{}
}

// Merge 按优先级合并配置
func (cp *ConfigPriority) Merge() interface{} {
	// 先应用默认值
	result := ApplyDefaults(cp.ConfigFile)
	
	// 然后应用命令行参数（这里简化处理，实际应用中需要更复杂的合并逻辑）
	// 具体的合并逻辑应该在各个主程序中实现
	
	return result
}

// ValidateAndSanitize 验证并修正配置
func ValidateAndSanitize(config interface{}) error {
	switch cfg := config.(type) {
	case *ServerConfig:
		return validateAndSanitizeServerConfig(cfg)
	case *ClientConfig:
		return validateAndSanitizeClientConfig(cfg)
	}
	return nil
}

// validateAndSanitizeServerConfig 验证服务端配置
func validateAndSanitizeServerConfig(config *ServerConfig) error {
	if config.Server.ListenAddr == "" {
		return fmt.Errorf("server listen address is required")
	}
	if config.Server.AuthToken == "" {
		return fmt.Errorf("server auth token is required")
	}
	
	// 验证端口范围
	if config.K8sAPI.Enabled && (config.K8sAPI.Port < 1 || config.K8sAPI.Port > 65535) {
		return fmt.Errorf("K8s API port must be between 1 and 65535")
	}
	
	return nil
}

// validateAndSanitizeClientConfig 验证客户端配置
func validateAndSanitizeClientConfig(config *ClientConfig) error {
	if config.Client.ServerAddr == "" {
		return fmt.Errorf("client server address is required")
	}
	if config.Client.AuthToken == "" {
		return fmt.Errorf("client auth token is required")
	}
	if config.Client.Mode != "proxy" && config.Client.Mode != "register" {
		return fmt.Errorf("client mode must be 'proxy' or 'register'")
	}
	
	// 验证模式特定配置
	switch config.Client.Mode {
	case "proxy":
		if len(config.Proxies) == 0 {
			return fmt.Errorf("proxy mode requires at least one proxy configuration")
		}
	case "register":
		if len(config.Workloads) == 0 {
			return fmt.Errorf("register mode requires at least one workload configuration")
		}
	}
	
	return nil
}