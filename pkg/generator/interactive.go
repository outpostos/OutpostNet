// 交互式配置向导
package generator

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/outpostos/OutpostNet/pkg/config"
)

// InteractiveWizard 交互式配置向导
type InteractiveWizard struct {
	generator *ConfigGenerator
	scanner   *bufio.Scanner
}

// NewInteractiveWizard 创建交互式配置向导
func NewInteractiveWizard(generator *ConfigGenerator) *InteractiveWizard {
	return &InteractiveWizard{
		generator: generator,
		scanner:   bufio.NewScanner(os.Stdin),
	}
}

// Run 运行交互式配置向导
func (w *InteractiveWizard) Run(outputDir string) error {
	fmt.Println("🔧 outpostnet Interactive Configuration Wizard")
	fmt.Println("===========================================")
	fmt.Println()

	// 创建部署配置
	deployment := &config.Deployment{}

	// 收集基本信息
	if err := w.collectBasicInfo(deployment); err != nil {
		return err
	}

	// 收集集群配置
	if err := w.collectClusterConfig(deployment); err != nil {
		return err
	}

	// 收集节点配置
	if err := w.collectNodeConfig(deployment); err != nil {
		return err
	}

	// 收集服务配置
	if err := w.collectServiceConfig(deployment); err != nil {
		return err
	}

	// 收集安全配置
	if err := w.collectSecurityConfig(deployment); err != nil {
		return err
	}

	// 显示配置摘要
	w.displaySummary(deployment)

	// 确认生成配置
	if !w.confirm("Generate configuration files?") {
		fmt.Println("Configuration generation cancelled.")
		return nil
	}

	// 生成配置文件
	if err := w.generator.Generate(deployment, outputDir); err != nil {
		return fmt.Errorf("failed to generate configuration: %w", err)
	}

	fmt.Printf("✅ Configuration files generated successfully in: %s\n", outputDir)
	fmt.Println()
	fmt.Println("Next steps:")
	fmt.Println("1. Review the generated configuration files")
	fmt.Println("2. Customize settings as needed")
	fmt.Printf("3. Run: cd %s && ./scripts/start.sh\n", outputDir)

	return nil
}

// collectBasicInfo 收集基本信息
func (w *InteractiveWizard) collectBasicInfo(deployment *config.Deployment) error {
	fmt.Println("📋 Basic Information")
	fmt.Println("-------------------")

	// 部署名称
	deployment.Name = w.promptString("Deployment name", "outpostnet-deployment")

	// 版本
	deployment.Version = w.promptString("Version", "1.0.0")

	// 环境
	deployment.Environment = w.promptChoice("Environment", []string{"dev", "staging", "production"}, "dev")

	fmt.Println()
	return nil
}

// collectClusterConfig 收集集群配置
func (w *InteractiveWizard) collectClusterConfig(deployment *config.Deployment) error {
	fmt.Println("🏗️  Cluster Configuration")
	fmt.Println("-------------------------")

	// 集群类型
	clusterType := w.promptChoice("Cluster type", []string{"standalone", "region-mesh", "multi-cluster"}, "standalone")
	deployment.Cluster.Type = clusterType

	// 是否启用区域网格
	if clusterType == "region-mesh" || clusterType == "multi-cluster" {
		deployment.Cluster.Region.Enabled = true
		deployment.Cluster.Region.RegionID = w.promptString("Region ID", "default-region")

		// 服务同步配置
		fmt.Println("\n🔄 Service Sync Configuration")
		deployment.Cluster.Region.ServiceSync.Interval = w.promptString("Sync interval", "10s")
		deployment.Cluster.Region.ServiceSync.Timeout = w.promptString("Sync timeout", "60s")

		// 负载均衡配置
		fmt.Println("\n⚖️  Load Balancer Configuration")
		deployment.Cluster.Region.LoadBalancer.Enabled = true
		deployment.Cluster.Region.LoadBalancer.Strategy = w.promptChoice(
			"Load balancing strategy",
			[]string{"round_robin", "geographic_aware", "health_aware", "latency_aware"},
			"geographic_aware",
		)

		// 健康检查配置
		fmt.Println("\n🏥 Health Check Configuration")
		deployment.Cluster.Region.HealthCheck.Enabled = true
		deployment.Cluster.Region.HealthCheck.Interval = w.promptString("Health check interval", "30s")
		deployment.Cluster.Region.HealthCheck.Timeout = w.promptString("Health check timeout", "10s")
		deployment.Cluster.Region.HealthCheck.MaxFailures = w.promptInt("Max failures", 3)
		deployment.Cluster.Region.HealthCheck.PhiThreshold = w.promptFloat("Phi threshold", 8.0)
	}

	fmt.Println()
	return nil
}

// collectNodeConfig 收集节点配置
func (w *InteractiveWizard) collectNodeConfig(deployment *config.Deployment) error {
	fmt.Println("🖥️  Node Configuration")
	fmt.Println("----------------------")

	nodeCount := w.promptInt("Number of nodes", 1)
	if nodeCount < 1 {
		nodeCount = 1
	}

	deployment.Cluster.Nodes = make([]config.NodeConfig, nodeCount)

	for i := 0; i < nodeCount; i++ {
		fmt.Printf("\n📍 Node %d Configuration\n", i+1)
		fmt.Println("------------------------")

		node := &deployment.Cluster.Nodes[i]

		// 节点标识
		node.NodeID = w.promptString("Node ID", fmt.Sprintf("node-%d", i+1))
		node.Name = w.promptString("Node name", fmt.Sprintf("node-%d", i+1))

		// 节点角色
		if i == 0 {
			node.Role = "primary"
		} else {
			node.Role = w.promptChoice("Node role", []string{"secondary", "edge"}, "secondary")
		}

		// 网络配置
		fmt.Println("\n🌐 Network Configuration")
		node.Network.ListenAddr = w.promptString("Listen address", "0.0.0.0")
		node.Network.ExternalAddr = w.promptString("External address", "127.0.0.1")

		// 端口配置
		fmt.Println("\n🔌 Port Configuration")
		node.Network.Ports.Control = w.promptInt("Control port", 7000+i)
		if deployment.Cluster.Region.Enabled {
			node.Network.Ports.RegionMesh = w.promptInt("Region mesh port", 8000+i)
		}
		node.Network.Ports.Metrics = w.promptInt("Metrics port", 9000+i)
		node.Network.Ports.Web = w.promptInt("Web port", 10000+i)

		// 地理位置
		fmt.Println("\n📍 Location Configuration")
		node.Location.Region = w.promptString("Region", "default")
		node.Location.Zone = w.promptString("Zone", fmt.Sprintf("zone-%d", i+1))
		node.Location.Site = w.promptString("Site", fmt.Sprintf("site-%d", i+1))

		// 资源配置
		if deployment.Environment == "production" {
			fmt.Println("\n💻 Resource Configuration")
			node.Resources.CPU = w.promptString("CPU limit", "2")
			node.Resources.Memory = w.promptString("Memory limit", "4Gi")
			node.Resources.Disk = w.promptString("Disk limit", "50Gi")
		}
	}

	fmt.Println()
	return nil
}

// collectServiceConfig 收集服务配置
func (w *InteractiveWizard) collectServiceConfig(deployment *config.Deployment) error {
	fmt.Println("🚀 Service Configuration")
	fmt.Println("------------------------")

	serviceCount := w.promptInt("Number of services", 1)
	if serviceCount < 1 {
		serviceCount = 1
	}

	deployment.Services = make([]config.ServiceConfig, serviceCount)

	for i := 0; i < serviceCount; i++ {
		fmt.Printf("\n🔧 Service %d Configuration\n", i+1)
		fmt.Println("---------------------------")

		service := &deployment.Services[i]

		// 服务基本信息
		service.Name = w.promptString("Service name", fmt.Sprintf("service-%d", i+1))
		service.Namespace = w.promptString("Service namespace", "default")
		service.Type = w.promptChoice("Service type", []string{"NodePort", "ClusterIP"}, "NodePort")

		// 代理模式
		service.ProxyMode = w.promptChoice("Proxy mode", []string{"tcp", "udp", "stcp", "sudp"}, "tcp")

		// 负载均衡策略
		service.LoadBalancer = w.promptChoice(
			"Load balancer",
			[]string{"round_robin", "weighted", "least_connections", "geographic_aware", "health_aware"},
			"round_robin",
		)

		// 端口配置
		fmt.Println("\n🔌 Port Configuration")
		portCount := w.promptInt("Number of ports", 1)
		if portCount < 1 {
			portCount = 1
		}

		service.Ports = make([]config.ServicePort, portCount)
		for j := 0; j < portCount; j++ {
			port := &service.Ports[j]
			port.Name = w.promptString("Port name", fmt.Sprintf("port-%d", j+1))
			port.Port = w.promptInt("Port", 80+j)
			port.TargetPort = w.promptInt("Target port", 8080+j)
			port.Protocol = service.ProxyMode
			
			if service.Type == "NodePort" {
				port.NodePort = w.promptInt("Node port", 30000+i*10+j)
			}
		}

		// 服务选择器
		fmt.Println("\n🏷️  Service Selector")
		service.Selector = make(map[string]string)
		service.Selector["app"] = w.promptString("App label", service.Name)

		// 后端配置
		fmt.Println("\n🖥️  Backend Configuration")
		backendCount := w.promptInt("Number of backends", 1)
		if backendCount < 1 {
			backendCount = 1
		}

		service.Backends = make([]config.BackendConfig, backendCount)
		for j := 0; j < backendCount; j++ {
			backend := &service.Backends[j]
			backend.Name = w.promptString("Backend name", fmt.Sprintf("%s-backend-%d", service.Name, j+1))
			backend.Address = w.promptString("Backend address", "127.0.0.1")
			backend.Port = w.promptInt("Backend port", service.Ports[0].TargetPort)
			backend.Weight = w.promptInt("Backend weight", 1)
			
			// 后端标签
			backend.Labels = make(map[string]string)
			backend.Labels["app"] = service.Selector["app"]
			backend.Labels["instance"] = backend.Name
		}
	}

	fmt.Println()
	return nil
}

// collectSecurityConfig 收集安全配置
func (w *InteractiveWizard) collectSecurityConfig(deployment *config.Deployment) error {
	fmt.Println("🔐 Security Configuration")
	fmt.Println("-------------------------")

	// 认证方法
	authMethod := w.promptChoice("Authentication method", []string{"token", "certificate", "none"}, "token")
	deployment.Security.Auth.Method = authMethod

	if authMethod == "token" {
		deployment.Security.Auth.Token = w.promptString("Auth token", "default-token")
	}

	// TLS配置
	enableTLS := w.promptBool("Enable TLS", false)
	deployment.Security.TLS.Enabled = enableTLS

	if enableTLS {
		fmt.Println("\n🔒 TLS Certificate Configuration")
		deployment.Security.TLS.CA.CertFile = w.promptString("CA certificate file", "/etc/ssl/certs/ca.crt")
		deployment.Security.TLS.CA.KeyFile = w.promptString("CA key file", "/etc/ssl/private/ca.key")
		
		certCount := w.promptInt("Number of certificates", 1)
		deployment.Security.TLS.Certs = make([]config.CertConfig, certCount)
		
		for i := 0; i < certCount; i++ {
			cert := &deployment.Security.TLS.Certs[i]
			cert.Name = w.promptString("Certificate name", fmt.Sprintf("cert-%d", i+1))
			cert.CertFile = w.promptString("Certificate file", fmt.Sprintf("/etc/ssl/certs/%s.crt", cert.Name))
			cert.KeyFile = w.promptString("Key file", fmt.Sprintf("/etc/ssl/private/%s.key", cert.Name))
		}
	}

	// 监控配置
	fmt.Println("\n📊 Monitoring Configuration")
	enableMonitoring := w.promptBool("Enable monitoring", true)
	deployment.Monitoring.Enabled = enableMonitoring

	if enableMonitoring {
		deployment.Monitoring.Type = w.promptChoice("Monitoring type", []string{"prometheus", "grafana"}, "prometheus")
	}

	fmt.Println()
	return nil
}

// displaySummary 显示配置摘要
func (w *InteractiveWizard) displaySummary(deployment *config.Deployment) {
	fmt.Println("📋 Configuration Summary")
	fmt.Println("========================")
	fmt.Printf("Name: %s\n", deployment.Name)
	fmt.Printf("Version: %s\n", deployment.Version)
	fmt.Printf("Environment: %s\n", deployment.Environment)
	fmt.Printf("Cluster Type: %s\n", deployment.Cluster.Type)
	fmt.Printf("Nodes: %d\n", len(deployment.Cluster.Nodes))
	fmt.Printf("Services: %d\n", len(deployment.Services))
	fmt.Printf("Security: %s\n", deployment.Security.Auth.Method)
	fmt.Printf("TLS: %t\n", deployment.Security.TLS.Enabled)
	fmt.Printf("Monitoring: %t\n", deployment.Monitoring.Enabled)
	
	if deployment.Cluster.Region.Enabled {
		fmt.Printf("Region Mesh: %s\n", deployment.Cluster.Region.RegionID)
		fmt.Printf("Load Balancer: %s\n", deployment.Cluster.Region.LoadBalancer.Strategy)
		fmt.Printf("Health Check: %t\n", deployment.Cluster.Region.HealthCheck.Enabled)
	}
	
	fmt.Println()
}

// 辅助方法

// promptString 提示用户输入字符串
func (w *InteractiveWizard) promptString(prompt, defaultValue string) string {
	fmt.Printf("%s [%s]: ", prompt, defaultValue)
	w.scanner.Scan()
	input := strings.TrimSpace(w.scanner.Text())
	if input == "" {
		return defaultValue
	}
	return input
}

// promptInt 提示用户输入整数
func (w *InteractiveWizard) promptInt(prompt string, defaultValue int) int {
	fmt.Printf("%s [%d]: ", prompt, defaultValue)
	w.scanner.Scan()
	input := strings.TrimSpace(w.scanner.Text())
	if input == "" {
		return defaultValue
	}
	
	if value, err := strconv.Atoi(input); err == nil {
		return value
	}
	
	fmt.Printf("Invalid input, using default value: %d\n", defaultValue)
	return defaultValue
}

// promptFloat 提示用户输入浮点数
func (w *InteractiveWizard) promptFloat(prompt string, defaultValue float64) float64 {
	fmt.Printf("%s [%.1f]: ", prompt, defaultValue)
	w.scanner.Scan()
	input := strings.TrimSpace(w.scanner.Text())
	if input == "" {
		return defaultValue
	}
	
	if value, err := strconv.ParseFloat(input, 64); err == nil {
		return value
	}
	
	fmt.Printf("Invalid input, using default value: %.1f\n", defaultValue)
	return defaultValue
}

// promptBool 提示用户输入布尔值
func (w *InteractiveWizard) promptBool(prompt string, defaultValue bool) bool {
	defaultStr := "n"
	if defaultValue {
		defaultStr = "y"
	}
	
	fmt.Printf("%s [%s]: ", prompt, defaultStr)
	w.scanner.Scan()
	input := strings.TrimSpace(strings.ToLower(w.scanner.Text()))
	
	if input == "" {
		return defaultValue
	}
	
	return input == "y" || input == "yes" || input == "true"
}

// promptChoice 提示用户选择选项
func (w *InteractiveWizard) promptChoice(prompt string, choices []string, defaultValue string) string {
	fmt.Printf("%s [%s]:\n", prompt, defaultValue)
	for i, choice := range choices {
		fmt.Printf("  %d) %s\n", i+1, choice)
	}
	fmt.Print("Choice: ")
	
	w.scanner.Scan()
	input := strings.TrimSpace(w.scanner.Text())
	
	if input == "" {
		return defaultValue
	}
	
	// 尝试解析为数字
	if index, err := strconv.Atoi(input); err == nil {
		if index >= 1 && index <= len(choices) {
			return choices[index-1]
		}
	}
	
	// 检查是否是有效选项
	for _, choice := range choices {
		if strings.EqualFold(input, choice) {
			return choice
		}
	}
	
	fmt.Printf("Invalid choice, using default: %s\n", defaultValue)
	return defaultValue
}

// confirm 确认操作
func (w *InteractiveWizard) confirm(prompt string) bool {
	fmt.Printf("%s [y/N]: ", prompt)
	w.scanner.Scan()
	input := strings.TrimSpace(strings.ToLower(w.scanner.Text()))
	return input == "y" || input == "yes"
}