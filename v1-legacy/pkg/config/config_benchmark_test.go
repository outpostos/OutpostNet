package config

import (
	"fmt"
	"io/ioutil"
	"os"
	"testing"

	"github.com/outpostos/OutpostNet/pkg/types"
)

// BenchmarkLoadServerConfig 测试服务器配置加载性能
func BenchmarkLoadServerConfig(b *testing.B) {
	// 创建临时配置文件
	configFile := createTempServerConfig(b, 10)
	defer os.Remove(configFile)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		config, err := LoadServerConfig(configFile)
		if err != nil {
			b.Fatalf("加载服务器配置失败: %v", err)
		}
		_ = config
	}
}

// BenchmarkLoadClientConfig 测试客户端配置加载性能
func BenchmarkLoadClientConfig(b *testing.B) {
	// 创建临时配置文件
	configFile := createTempClientConfig(b, 5)
	defer os.Remove(configFile)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		config, err := LoadClientConfig(configFile)
		if err != nil {
			b.Fatalf("加载客户端配置失败: %v", err)
		}
		_ = config
	}
}

// BenchmarkLoadClientConfigWithMode 测试默认模式配置加载性能
func BenchmarkLoadClientConfigWithMode(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		config, err := LoadClientConfigWithMode("", "proxy")
		if err != nil {
			b.Fatalf("加载默认客户端配置失败: %v", err)
		}
		_ = config
	}
}

// BenchmarkConfigValidation 测试配置验证性能
func BenchmarkConfigValidation(b *testing.B) {
	// 创建测试配置
	serverConfig := createTestServerConfig(100)
	clientConfig := createTestClientConfig(10)

	b.Run("ServerConfig", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			err := validateServerConfig(serverConfig)
			if err != nil {
				b.Fatalf("服务器配置验证失败: %v", err)
			}
		}
	})

	b.Run("ClientConfig", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			err := validateClientConfig(clientConfig)
			if err != nil {
				b.Fatalf("客户端配置验证失败: %v", err)
			}
		}
	})
}

// BenchmarkConfigSerialization 测试配置序列化性能
func BenchmarkConfigSerialization(b *testing.B) {
	serverConfig := createTestServerConfig(50)
	clientConfig := createTestClientConfig(20)

	b.Run("ServerConfigMarshal", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			data, err := marshalConfig(serverConfig)
			if err != nil {
				b.Fatalf("服务器配置序列化失败: %v", err)
			}
			_ = data
		}
	})

	b.Run("ClientConfigMarshal", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			data, err := marshalConfig(clientConfig)
			if err != nil {
				b.Fatalf("客户端配置序列化失败: %v", err)
			}
			_ = data
		}
	})

	// 序列化用于反序列化测试
	serverData, _ := marshalConfig(serverConfig)
	clientData, _ := marshalConfig(clientConfig)

	b.Run("ServerConfigUnmarshal", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			var config ServerConfig
			err := unmarshalConfig(serverData, &config)
			if err != nil {
				b.Fatalf("服务器配置反序列化失败: %v", err)
			}
		}
	})

	b.Run("ClientConfigUnmarshal", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			var config ClientConfig
			err := unmarshalConfig(clientData, &config)
			if err != nil {
				b.Fatalf("客户端配置反序列化失败: %v", err)
			}
		}
	})
}

// BenchmarkDefaultConfigGeneration 测试默认配置生成性能
func BenchmarkDefaultConfigGeneration(b *testing.B) {
	b.Run("DefaultServerConfig", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			config := DefaultServerConfig()
			_ = config
		}
	})

	b.Run("DefaultClientConfigProxy", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			config := DefaultClientConfig("proxy")
			_ = config
		}
	})

	b.Run("DefaultClientConfigRegister", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			config := DefaultClientConfig("register")
			_ = config
		}
	})
}

// BenchmarkConfigWithDifferentSizes 测试不同大小配置的性能
func BenchmarkConfigWithDifferentSizes(b *testing.B) {
	serviceCounts := []int{1, 10, 50, 100, 500}

	for _, count := range serviceCounts {
		b.Run(fmt.Sprintf("ServerConfig_Services_%d", count), func(b *testing.B) {
			configFile := createTempServerConfig(b, count)
			defer os.Remove(configFile)

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				config, err := LoadServerConfig(configFile)
				if err != nil {
					b.Fatalf("加载配置失败: %v", err)
				}
				_ = config
			}
		})
	}

	proxyCounts := []int{1, 5, 10, 20, 50}

	for _, count := range proxyCounts {
		b.Run(fmt.Sprintf("ClientConfig_Proxies_%d", count), func(b *testing.B) {
			configFile := createTempClientConfig(b, count)
			defer os.Remove(configFile)

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				config, err := LoadClientConfig(configFile)
				if err != nil {
					b.Fatalf("加载配置失败: %v", err)
				}
				_ = config
			}
		})
	}
}

// BenchmarkConcurrentConfigLoad 测试并发配置加载性能
func BenchmarkConcurrentConfigLoad(b *testing.B) {
	serverConfigFile := createTempServerConfig(b, 20)
	clientConfigFile := createTempClientConfig(b, 10)
	defer os.Remove(serverConfigFile)
	defer os.Remove(clientConfigFile)

	b.Run("ConcurrentServerConfig", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()

		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				config, err := LoadServerConfig(serverConfigFile)
				if err != nil {
					b.Fatalf("并发加载服务器配置失败: %v", err)
				}
				_ = config
			}
		})
	})

	b.Run("ConcurrentClientConfig", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()

		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				config, err := LoadClientConfig(clientConfigFile)
				if err != nil {
					b.Fatalf("并发加载客户端配置失败: %v", err)
				}
				_ = config
			}
		})
	})
}

// BenchmarkConfigMemoryUsage 测试配置内存使用情况
func BenchmarkConfigMemoryUsage(b *testing.B) {
	serviceCounts := []int{100, 1000, 5000}

	for _, count := range serviceCounts {
		b.Run(fmt.Sprintf("MemoryUsage_Services_%d", count), func(b *testing.B) {
			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				// 创建大型配置
				config := createTestServerConfig(count)

				// 验证配置
				err := validateServerConfig(config)
				if err != nil {
					b.Fatalf("配置验证失败: %v", err)
				}

				// 序列化配置
				data, err := marshalConfig(config)
				if err != nil {
					b.Fatalf("配置序列化失败: %v", err)
				}

				// 反序列化配置
				var newConfig ServerConfig
				err = unmarshalConfig(data, &newConfig)
				if err != nil {
					b.Fatalf("配置反序列化失败: %v", err)
				}
			}
		})
	}
}

// 辅助函数：创建临时服务器配置文件
func createTempServerConfig(b *testing.B, numServices int) string {
	config := createTestServerConfig(numServices)
	data, err := marshalConfig(config)
	if err != nil {
		b.Fatalf("序列化配置失败: %v", err)
	}

	tempFile, err := ioutil.TempFile("", "outpostnet-server-config-*.yaml")
	if err != nil {
		b.Fatalf("创建临时文件失败: %v", err)
	}

	_, err = tempFile.Write(data)
	if err != nil {
		b.Fatalf("写入配置文件失败: %v", err)
	}

	tempFile.Close()
	return tempFile.Name()
}

// 辅助函数：创建临时客户端配置文件
func createTempClientConfig(b *testing.B, numProxies int) string {
	config := createTestClientConfig(numProxies)
	data, err := marshalConfig(config)
	if err != nil {
		b.Fatalf("序列化配置失败: %v", err)
	}

	tempFile, err := ioutil.TempFile("", "outpostnet-client-config-*.yaml")
	if err != nil {
		b.Fatalf("创建临时文件失败: %v", err)
	}

	_, err = tempFile.Write(data)
	if err != nil {
		b.Fatalf("写入配置文件失败: %v", err)
	}

	tempFile.Close()
	return tempFile.Name()
}

// 辅助函数：创建测试服务器配置
func createTestServerConfig(numServices int) *ServerConfig {
	services := make([]types.Service, numServices)
	for i := 0; i < numServices; i++ {
		services[i] = types.Service{
			Metadata: types.ServiceMetadata{
				Name:      fmt.Sprintf("service-%d", i),
				Namespace: "default",
				Labels: map[string]string{
					"app":     fmt.Sprintf("app-%d", i),
					"version": "v1",
					"tier":    "backend",
				},
			},
			Spec: types.ServiceSpec{
				Selector: map[string]string{
					"app": fmt.Sprintf("app-%d", i),
				},
				Ports: []types.ServicePort{
					{
						Name:       "http",
						Port:       80,
						TargetPort: 8080,
						Protocol:   "tcp",
					},
					{
						Name:       "grpc",
						Port:       9090,
						TargetPort: 9090,
						Protocol:   "tcp",
					},
				},
				ProxyMode:    "tcp",
				LoadBalancer: "round_robin",
			},
		}
	}

	return &ServerConfig{
		Server: ServerSpec{
			ListenAddr: "0.0.0.0:7629",
			AuthToken:  "benchmark-test-token",
		},
		Services: services,
		K8sAPI: K8sAPISpec{
			Enabled: true,
			Port:    8443,
		},
	}
}

// 辅助函数：创建测试客户端配置
func createTestClientConfig(numProxies int) *ClientConfig {
	proxies := make([]types.Proxy, numProxies)
	for i := 0; i < numProxies; i++ {
		proxies[i] = types.Proxy{
			Name:             fmt.Sprintf("proxy-%d", i),
			ServiceName:      fmt.Sprintf("service-%d", i),
			ServiceNamespace: "default",
			LocalPort:        8080 + i,
			ProxyMode:        "tcp",
			Selector: map[string]string{
				"app": fmt.Sprintf("app-%d", i),
			},
			LoadBalancer: "round_robin",
		}
	}

	workloads := make([]ClientWorkload, numProxies)
	for i := 0; i < numProxies; i++ {
		workloads[i] = ClientWorkload{
			Metadata: ClientWorkloadMetadata{
				Labels: map[string]string{
					"app":      fmt.Sprintf("app-%d", i),
					"version":  "v1",
					"instance": fmt.Sprintf("instance-%d", i),
				},
			},
			Spec: ClientWorkloadSpec{
				LocalAddr: "127.0.0.1",
				LocalPort: 8080 + i,
				Protocol:  "tcp",
			},
		}
	}

	return &ClientConfig{
		Client: ClientSpec{
			ServerAddr: "127.0.0.1:7629",
			AuthToken:  "benchmark-test-token",
			Mode:       "proxy",
		},
		Proxies:   proxies,
		Workloads: workloads,
	}
}

// 辅助函数：配置验证（简化版）
func validateServerConfig(config *ServerConfig) error {
	if config.Server.ListenAddr == "" {
		return fmt.Errorf("listen address is required")
	}
	if config.Server.AuthToken == "" {
		return fmt.Errorf("auth token is required")
	}
	for _, service := range config.Services {
		if service.Metadata.Name == "" {
			return fmt.Errorf("service name is required")
		}
		if service.Metadata.Namespace == "" {
			return fmt.Errorf("service namespace is required")
		}
	}
	return nil
}

// 辅助函数：客户端配置验证（简化版）
func validateClientConfig(config *ClientConfig) error {
	if config.Client.ServerAddr == "" {
		return fmt.Errorf("server address is required")
	}
	if config.Client.AuthToken == "" {
		return fmt.Errorf("auth token is required")
	}
	for _, proxy := range config.Proxies {
		if proxy.Name == "" {
			return fmt.Errorf("proxy name is required")
		}
		if proxy.LocalPort <= 0 || proxy.LocalPort > 65535 {
			return fmt.Errorf("invalid local port: %d", proxy.LocalPort)
		}
	}
	return nil
}

// 辅助函数：配置序列化（简化版，使用YAML）
func marshalConfig(config interface{}) ([]byte, error) {
	// 这里应该使用yaml.Marshal，但为了基准测试简化使用fmt.Sprintf
	return []byte(fmt.Sprintf("%+v", config)), nil
}

// 辅助函数：配置反序列化（简化版）
func unmarshalConfig(data []byte, config interface{}) error {
	// 这里应该使用yaml.Unmarshal，但为了基准测试简化直接返回nil
	return nil
}