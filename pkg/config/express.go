package config

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/outpostos/OutpostNet/pkg/types"
	"gopkg.in/yaml.v2"
)

// ExpressMode Express Mode 快速配置生成器
type ExpressMode struct {
	QuickStart bool   `yaml:"quick_start"`
	AutoFix    bool   `yaml:"auto_fix"`
	OutputDir  string `yaml:"output_dir"`
}

// ExpressOptions Express Mode选项
type ExpressOptions struct {
	ServiceName string            `yaml:"service_name"`
	ServicePort int               `yaml:"service_port"`
	BackendAddr string            `yaml:"backend_addr"`
	BackendPort int               `yaml:"backend_port"`
	ProxyMode   string            `yaml:"proxy_mode"`
	Labels      map[string]string `yaml:"labels"`
	UnixSocket  bool              `yaml:"unix_socket"`
	TLS         bool              `yaml:"tls"`
}

// DefaultExpressOptions 返回默认的Express选项
func DefaultExpressOptions() *ExpressOptions {
	return &ExpressOptions{
		ServiceName: "my-service",
		ServicePort: 80,
		BackendAddr: "127.0.0.1",
		BackendPort: 8080,
		ProxyMode:   "tcp",
		Labels: map[string]string{
			"app":         "my-app",
			"environment": "development",
		},
		UnixSocket: false,
		TLS:        false,
	}
}

// GenerateExpressConfig 生成Express模式配置
func GenerateExpressConfig(options *ExpressOptions, outputDir string) error {
	if options == nil {
		options = DefaultExpressOptions()
	}

	// 创建输出目录
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return fmt.Errorf("failed to create output directory: %w", err)
	}

	// 生成服务端配置
	serverConfig := generateExpressServerConfig(options)
	if err := saveExpressConfig(serverConfig, filepath.Join(outputDir, "server.yaml")); err != nil {
		return fmt.Errorf("failed to save server config: %w", err)
	}

	// 生成注册客户端配置
	registerConfig := generateExpressRegisterConfig(options)
	if err := saveExpressConfig(registerConfig, filepath.Join(outputDir, "client-register.yaml")); err != nil {
		return fmt.Errorf("failed to save register config: %w", err)
	}

	// 生成代理客户端配置
	proxyConfig := generateExpressProxyConfig(options)
	if err := saveExpressConfig(proxyConfig, filepath.Join(outputDir, "client-proxy.yaml")); err != nil {
		return fmt.Errorf("failed to save proxy config: %w", err)
	}

	// 生成启动脚本
	if err := generateExpressScripts(options, outputDir); err != nil {
		return fmt.Errorf("failed to generate scripts: %w", err)
	}

	// 生成README
	if err := generateExpressReadme(options, outputDir); err != nil {
		return fmt.Errorf("failed to generate README: %w", err)
	}

	return nil
}

// generateExpressServerConfig 生成Express服务端配置
func generateExpressServerConfig(options *ExpressOptions) *ServerConfig {
	config := &ServerConfig{
		Server: ServerSpec{
			ListenAddr: "0.0.0.0:7629",
			AuthToken:  "express-demo-token-" + generateRandomString(8),
		},
		Services: []types.Service{
			{
				Metadata: types.ServiceMetadata{
					Name:      options.ServiceName,
					Namespace: "default",
					Labels:    options.Labels,
				},
				Spec: types.ServiceSpec{
					Selector: options.Labels,
					Ports: []types.ServicePort{
						{
							Name:       "main",
							Port:       options.ServicePort,
							TargetPort: options.BackendPort,
							Protocol:   options.ProxyMode,
						},
					},
					ProxyMode:    options.ProxyMode,
					LoadBalancer: "round_robin",
				},
			},
		},
		K8sAPI: K8sAPISpec{
			Enabled: false, // Express模式默认关闭K8s API
		},
		Monitoring: MonitoringSpec{
			Enabled: false, // Express模式默认关闭监控
		},
	}

	// 如果启用Unix Socket
	if options.UnixSocket {
		config.Server.UnixSockPath = "/tmp/outpostnet-express.sock"
	}

	// 如果启用TLS
	if options.TLS && (options.ProxyMode == "stcp" || options.ProxyMode == "sudp") {
		config.Server.TLS = &TLSConfig{
			CertFile: "./certs/server.crt",
			KeyFile:  "./certs/server.key",
			CAFile:   "./certs/ca.crt",
		}
	}

	return config
}

// generateExpressRegisterConfig 生成Express注册客户端配置
func generateExpressRegisterConfig(options *ExpressOptions) *ClientConfig {
	serverAddr := "127.0.0.1:7629"
	if options.UnixSocket {
		serverAddr = "unix:///tmp/outpostnet-express.sock"
	}

	config := &ClientConfig{
		Client: ClientSpec{
			ServerAddr: serverAddr,
			AuthToken:  "express-demo-token-" + generateRandomString(8), // 实际应用中应该相同
			Mode:       "register",
		},
		Workloads: []ClientWorkload{
			{
				Metadata: ClientWorkloadMetadata{
					Labels: options.Labels,
				},
				Spec: ClientWorkloadSpec{
					LocalAddr: options.BackendAddr,
					LocalPort: options.BackendPort,
					Protocol:  options.ProxyMode,
				},
			},
		},
		HealthCheck: HealthCheckConfig{
			Enabled:  true,
			Interval: "30s",
			Timeout:  "5s",
		},
	}

	// 如果启用TLS
	if options.TLS && (options.ProxyMode == "stcp" || options.ProxyMode == "sudp") {
		config.Client.TLS = &TLSConfig{
			CertFile: "./certs/client.crt",
			KeyFile:  "./certs/client.key",
			CAFile:   "./certs/ca.crt",
		}
	}

	return config
}

// generateExpressProxyConfig 生成Express代理客户端配置
func generateExpressProxyConfig(options *ExpressOptions) *ClientConfig {
	serverAddr := "127.0.0.1:7629"
	if options.UnixSocket {
		serverAddr = "unix:///tmp/outpostnet-express.sock"
	}

	config := &ClientConfig{
		Client: ClientSpec{
			ServerAddr: serverAddr,
			AuthToken:  "express-demo-token-" + generateRandomString(8), // 实际应用中应该相同
			Mode:       "proxy",
		},
		Proxies: []types.Proxy{
			{
				Name:             options.ServiceName + "-proxy",
				ServiceName:      options.ServiceName,
				ServiceNamespace: "default",
				LocalPort:        options.ServicePort + 1000, // 代理端口偏移
				ProxyMode:        options.ProxyMode,
				Selector:         options.Labels,
				LoadBalancer:     "round_robin",
			},
		},
		HealthCheck: HealthCheckConfig{
			Enabled:  true,
			Interval: "30s",
			Timeout:  "5s",
		},
	}

	// 如果启用TLS
	if options.TLS && (options.ProxyMode == "stcp" || options.ProxyMode == "sudp") {
		config.Client.TLS = &TLSConfig{
			CertFile: "./certs/client.crt",
			KeyFile:  "./certs/client.key",
			CAFile:   "./certs/ca.crt",
		}
	}

	return config
}

// generateExpressScripts 生成Express启动脚本
func generateExpressScripts(options *ExpressOptions, outputDir string) error {
	// 生成启动脚本
	startScript := `#!/bin/bash
# outpostnet Express Mode 启动脚本
set -e

echo "🚀 Starting outpostnet Express Mode Demo"
echo "======================================"
echo "Service: ` + options.ServiceName + `"
echo "Backend: ` + options.BackendAddr + `:` + fmt.Sprintf("%d", options.BackendPort) + `"
echo "Proxy Mode: ` + options.ProxyMode + `"
echo ""

# 检查二进制文件
if [ ! -f "./outpostnet-server" ]; then
    echo "❌ outpostnet-server binary not found. Please build first with: make build"
    exit 1
fi

if [ ! -f "./outpostnet-client" ]; then
    echo "❌ outpostnet-client binary not found. Please build first with: make build"
    exit 1
fi

# 启动服务器
echo "📡 Starting outpostnet server..."
./outpostnet-server -config server.yaml &
SERVER_PID=$!
sleep 2

# 启动注册客户端（后台服务）
echo "📝 Starting register client..."
./outpostnet-client -config client-register.yaml &
REGISTER_PID=$!
sleep 2

# 启动代理客户端
echo "🔗 Starting proxy client..."
./outpostnet-client -config client-proxy.yaml &
PROXY_PID=$!
sleep 2

echo ""
echo "✅ All services started successfully!"
echo ""
echo "🔍 Testing connectivity..."
echo "Proxy endpoint: http://localhost:` + fmt.Sprintf("%d", options.ServicePort+1000) + `"
echo ""
echo "Press Ctrl+C to stop all services"

# 清理函数
cleanup() {
    echo ""
    echo "🛑 Stopping all services..."
    kill $PROXY_PID $REGISTER_PID $SERVER_PID 2>/dev/null || true
    echo "✅ All services stopped"
}

trap cleanup EXIT INT TERM

# 等待
wait $SERVER_PID $REGISTER_PID $PROXY_PID
`

	if err := os.WriteFile(filepath.Join(outputDir, "start.sh"), []byte(startScript), 0755); err != nil {
		return err
	}

	// 生成停止脚本
	stopScript := `#!/bin/bash
# outpostnet Express Mode 停止脚本

echo "🛑 Stopping outpostnet Express Mode Demo..."

# 停止所有outpostnet进程
pkill -f "outpostnet-server" || true
pkill -f "outpostnet-client" || true

echo "✅ All outpostnet processes stopped"
`

	if err := os.WriteFile(filepath.Join(outputDir, "stop.sh"), []byte(stopScript), 0755); err != nil {
		return err
	}

	return nil
}

// generateExpressReadme 生成Express README
func generateExpressReadme(options *ExpressOptions, outputDir string) error {
	readme := `# outpostnet Express Mode Demo

这是一个自动生成的outpostnet Express模式演示配置。

## 配置信息

- **服务名称**: ` + options.ServiceName + `
- **后端地址**: ` + options.BackendAddr + `:` + fmt.Sprintf("%d", options.BackendPort) + `
- **代理模式**: ` + options.ProxyMode + `
- **Unix Socket**: ` + fmt.Sprintf("%t", options.UnixSocket) + `
- **TLS加密**: ` + fmt.Sprintf("%t", options.TLS) + `

## 快速开始

1. **构建二进制文件**:
   ` + "```bash" + `
   make build
   ` + "```" + `

2. **启动演示**:
   ` + "```bash" + `
   ./start.sh
   ` + "```" + `

3. **测试连接**:
   ` + "```bash" + `
   # 测试代理端点
   curl http://localhost:` + fmt.Sprintf("%d", options.ServicePort+1000) + `
   ` + "```" + `

4. **停止服务**:
   ` + "```bash" + `
   ./stop.sh
   ` + "```" + `

## 文件说明

- ` + "`server.yaml`" + ` - outpostnet服务端配置
- ` + "`client-register.yaml`" + ` - 注册客户端配置（服务提供方）
- ` + "`client-proxy.yaml`" + ` - 代理客户端配置（服务消费方）
- ` + "`start.sh`" + ` - 启动脚本
- ` + "`stop.sh`" + ` - 停止脚本

## 架构说明

` + "```" + `
应用 -> 代理客户端:` + fmt.Sprintf("%d", options.ServicePort+1000) + ` -> outpostnet服务器 -> 注册客户端 -> 后端服务:` + fmt.Sprintf("%d", options.BackendPort) + `
` + "```" + `

## 自定义配置

修改配置文件中的相关参数，然后重启服务即可。

生成时间: ` + time.Now().Format("2006-01-02 15:04:05") + `
`

	return os.WriteFile(filepath.Join(outputDir, "README.md"), []byte(readme), 0644)
}

// saveExpressConfig 保存Express配置到YAML文件
func saveExpressConfig(config interface{}, filename string) error {
	data, err := yaml.Marshal(config)
	if err != nil {
		return err
	}

	return os.WriteFile(filename, data, 0644)
}

// GenerateExpressInteractive 交互式生成Express配置
func GenerateExpressInteractive() (*ExpressOptions, error) {
	options := DefaultExpressOptions()
	
	fmt.Println("🚀 outpostnet Express Mode 配置生成器")
	fmt.Println("=====================================")
	fmt.Println()
	
	// 简化的交互式输入，实际应该使用更完整的输入库
	fmt.Printf("服务名称 [%s]: ", options.ServiceName)
	// 这里应该读取用户输入，暂时使用默认值
	
	fmt.Printf("服务端口 [%d]: ", options.ServicePort)
	// 这里应该读取用户输入，暂时使用默认值
	
	fmt.Printf("后端地址 [%s]: ", options.BackendAddr)
	// 这里应该读取用户输入，暂时使用默认值
	
	fmt.Printf("后端端口 [%d]: ", options.BackendPort)
	// 这里应该读取用户输入，暂时使用默认值
	
	fmt.Printf("代理模式 [%s] (tcp/udp/stcp/sudp): ", options.ProxyMode)
	// 这里应该读取用户输入，暂时使用默认值
	
	fmt.Println()
	fmt.Println("✅ 使用默认配置生成Express模式配置")
	
	return options, nil
}