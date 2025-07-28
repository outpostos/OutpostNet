// 配置生成器核心实现
package generator

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"text/template"

	"github.com/outpostos/OutpostNet/pkg/config"
	"gopkg.in/yaml.v2"
)

// ConfigGenerator 配置生成器
type ConfigGenerator struct {
	templates map[string]*template.Template
}

// NewConfigGenerator 创建配置生成器
func NewConfigGenerator() *ConfigGenerator {
	return &ConfigGenerator{
		templates: make(map[string]*template.Template),
	}
}

// Generate 生成配置文件
func (g *ConfigGenerator) Generate(deployment *config.Deployment, outputDir string) error {
	// 创建输出目录
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return fmt.Errorf("failed to create output directory: %w", err)
	}

	// 生成服务端配置
	if err := g.generateServerConfigs(deployment, outputDir); err != nil {
		return fmt.Errorf("failed to generate server configs: %w", err)
	}

	// 生成客户端配置
	if err := g.generateClientConfigs(deployment, outputDir); err != nil {
		return fmt.Errorf("failed to generate client configs: %w", err)
	}

	// 生成Docker配置
	if err := g.generateDockerConfigs(deployment, outputDir); err != nil {
		return fmt.Errorf("failed to generate docker configs: %w", err)
	}

	// 生成Kubernetes配置
	if err := g.generateKubernetesConfigs(deployment, outputDir); err != nil {
		return fmt.Errorf("failed to generate kubernetes configs: %w", err)
	}

	// 生成启动脚本
	if err := g.generateStartupScripts(deployment, outputDir); err != nil {
		return fmt.Errorf("failed to generate startup scripts: %w", err)
	}

	return nil
}

// GenerateFromTemplate 从模板生成配置
func (g *ConfigGenerator) GenerateFromTemplate(templateName, outputDir string) error {
	var deployment *config.Deployment
	var err error

	switch templateName {
	case "express":
		deployment, err = g.createExpressTemplate()
	case "region-mesh":
		deployment, err = g.createRegionMeshTemplate()
	case "production":
		deployment, err = g.createProductionTemplate()
	default:
		return fmt.Errorf("unknown template: %s", templateName)
	}

	if err != nil {
		return fmt.Errorf("failed to create template: %w", err)
	}

	return g.Generate(deployment, outputDir)
}

// generateServerConfigs 生成服务端配置
func (g *ConfigGenerator) generateServerConfigs(deployment *config.Deployment, outputDir string) error {
	serverDir := filepath.Join(outputDir, "servers")
	if err := os.MkdirAll(serverDir, 0755); err != nil {
		return err
	}

	for _, node := range deployment.Cluster.Nodes {
		serverConfig := g.createServerConfig(deployment, &node)
		
		filename := filepath.Join(serverDir, fmt.Sprintf("server-%s.yaml", node.NodeID))
		if err := g.saveYAML(serverConfig, filename); err != nil {
			return fmt.Errorf("failed to save server config for node %s: %w", node.NodeID, err)
		}
	}

	return nil
}

// generateClientConfigs 生成客户端配置
func (g *ConfigGenerator) generateClientConfigs(deployment *config.Deployment, outputDir string) error {
	clientDir := filepath.Join(outputDir, "clients")
	if err := os.MkdirAll(clientDir, 0755); err != nil {
		return err
	}

	// 生成注册客户端配置
	for _, service := range deployment.Services {
		if len(service.Backends) > 0 {
			for _, backend := range service.Backends {
				registerConfig := g.createRegisterClientConfig(deployment, &service, &backend)
				
				filename := filepath.Join(clientDir, fmt.Sprintf("register-%s-%s.yaml", 
					service.Name, backend.Name))
				if err := g.saveYAML(registerConfig, filename); err != nil {
					return fmt.Errorf("failed to save register client config: %w", err)
				}
			}
		}
	}

	// 生成代理客户端配置
	for _, service := range deployment.Services {
		proxyConfig := g.createProxyClientConfig(deployment, &service)
		
		filename := filepath.Join(clientDir, fmt.Sprintf("proxy-%s.yaml", service.Name))
		if err := g.saveYAML(proxyConfig, filename); err != nil {
			return fmt.Errorf("failed to save proxy client config: %w", err)
		}
	}

	return nil
}

// generateDockerConfigs 生成Docker配置
func (g *ConfigGenerator) generateDockerConfigs(deployment *config.Deployment, outputDir string) error {
	dockerDir := filepath.Join(outputDir, "docker")
	if err := os.MkdirAll(dockerDir, 0755); err != nil {
		return err
	}

	// 生成docker-compose.yml
	dockerCompose := g.createDockerCompose(deployment)
	filename := filepath.Join(dockerDir, "docker-compose.yml")
	if err := g.saveYAML(dockerCompose, filename); err != nil {
		return fmt.Errorf("failed to save docker-compose.yml: %w", err)
	}

	// 生成Dockerfile
	dockerfile := g.createDockerfile(deployment)
	filename = filepath.Join(dockerDir, "Dockerfile")
	if err := os.WriteFile(filename, []byte(dockerfile), 0644); err != nil {
		return fmt.Errorf("failed to save Dockerfile: %w", err)
	}

	return nil
}

// generateKubernetesConfigs 生成Kubernetes配置
func (g *ConfigGenerator) generateKubernetesConfigs(deployment *config.Deployment, outputDir string) error {
	k8sDir := filepath.Join(outputDir, "kubernetes")
	if err := os.MkdirAll(k8sDir, 0755); err != nil {
		return err
	}

	// 生成命名空间
	namespace := g.createNamespace(deployment)
	filename := filepath.Join(k8sDir, "namespace.yaml")
	if err := g.saveYAML(namespace, filename); err != nil {
		return fmt.Errorf("failed to save namespace.yaml: %w", err)
	}

	// 生成ConfigMap
	configMap := g.createConfigMap(deployment)
	filename = filepath.Join(k8sDir, "configmap.yaml")
	if err := g.saveYAML(configMap, filename); err != nil {
		return fmt.Errorf("failed to save configmap.yaml: %w", err)
	}

	// 生成Deployment
	for _, node := range deployment.Cluster.Nodes {
		k8sDeployment := g.createK8sDeployment(deployment, &node)
		filename = filepath.Join(k8sDir, fmt.Sprintf("deployment-%s.yaml", node.NodeID))
		if err := g.saveYAML(k8sDeployment, filename); err != nil {
			return fmt.Errorf("failed to save deployment-%s.yaml: %w", node.NodeID, err)
		}
	}

	// 生成Service
	for _, service := range deployment.Services {
		k8sService := g.createK8sService(deployment, &service)
		filename = filepath.Join(k8sDir, fmt.Sprintf("service-%s.yaml", service.Name))
		if err := g.saveYAML(k8sService, filename); err != nil {
			return fmt.Errorf("failed to save service-%s.yaml: %w", service.Name, err)
		}
	}

	return nil
}

// generateStartupScripts 生成启动脚本
func (g *ConfigGenerator) generateStartupScripts(deployment *config.Deployment, outputDir string) error {
	scriptsDir := filepath.Join(outputDir, "scripts")
	if err := os.MkdirAll(scriptsDir, 0755); err != nil {
		return err
	}

	// 生成启动脚本
	startScript := g.createStartScript(deployment)
	filename := filepath.Join(scriptsDir, "start.sh")
	if err := os.WriteFile(filename, []byte(startScript), 0755); err != nil {
		return fmt.Errorf("failed to save start.sh: %w", err)
	}

	// 生成停止脚本
	stopScript := g.createStopScript(deployment)
	filename = filepath.Join(scriptsDir, "stop.sh")
	if err := os.WriteFile(filename, []byte(stopScript), 0755); err != nil {
		return fmt.Errorf("failed to save stop.sh: %w", err)
	}

	// 生成演示脚本
	demoScript := g.createDemoScript(deployment)
	filename = filepath.Join(scriptsDir, "demo.sh")
	if err := os.WriteFile(filename, []byte(demoScript), 0755); err != nil {
		return fmt.Errorf("failed to save demo.sh: %w", err)
	}

	return nil
}

// createServerConfig 创建服务端配置
func (g *ConfigGenerator) createServerConfig(deployment *config.Deployment, node *config.NodeConfig) map[string]interface{} {
	serverConfig := map[string]interface{}{
		"server": map[string]interface{}{
			"listen_addr": fmt.Sprintf("%s:%d", node.Network.ListenAddr, node.Network.Ports.Control),
			"auth_token":  deployment.Security.Auth.Token,
		},
		"services": []map[string]interface{}{},
	}

	// 添加区域配置
	if deployment.Cluster.Region.Enabled {
		serverConfig["server"].(map[string]interface{})["region"] = map[string]interface{}{
			"enabled":     true,
			"region_id":   deployment.Cluster.Region.RegionID,
			"node_id":     node.NodeID,
			"site_id":     node.Location.Site,
			"zone_id":     node.Location.Zone,
			"listen_addr": fmt.Sprintf("%s:%d", node.Network.ListenAddr, node.Network.Ports.RegionMesh),
			"neighbors":   g.createNeighborsList(deployment, node),
			"service_sync": map[string]interface{}{
				"sync_interval": deployment.Cluster.Region.ServiceSync.Interval,
				"cache_timeout": deployment.Cluster.Region.ServiceSync.Timeout,
			},
			"load_balancer": map[string]interface{}{
				"strategy":             deployment.Cluster.Region.LoadBalancer.Strategy,
				"health_check_enabled": deployment.Cluster.Region.HealthCheck.Enabled,
			},
		}
	}

	// 添加服务定义
	services := make([]map[string]interface{}, len(deployment.Services))
	for i, service := range deployment.Services {
		services[i] = map[string]interface{}{
			"name":           service.Name,
			"namespace":      service.Namespace,
			"type":           service.Type,
			"selector":       service.Selector,
			"ports":          g.convertPorts(service.Ports),
			"proxy_mode":     service.ProxyMode,
			"load_balancer":  service.LoadBalancer,
		}
	}
	serverConfig["services"] = services

	return serverConfig
}

// createRegisterClientConfig 创建注册客户端配置
func (g *ConfigGenerator) createRegisterClientConfig(deployment *config.Deployment, service *config.ServiceConfig, backend *config.BackendConfig) map[string]interface{} {
	// 找到主节点
	var primaryNode *config.NodeConfig
	for _, node := range deployment.Cluster.Nodes {
		if node.Role == "primary" {
			primaryNode = &node
			break
		}
	}
	if primaryNode == nil {
		primaryNode = &deployment.Cluster.Nodes[0]
	}

	return map[string]interface{}{
		"client": map[string]interface{}{
			"server_addr": fmt.Sprintf("%s:%d", primaryNode.Network.ExternalAddr, primaryNode.Network.Ports.Control),
			"auth_token":  deployment.Security.Auth.Token,
			"mode":        "register",
		},
		"services": []map[string]interface{}{
			{
				"name":       service.Name,
				"namespace":  service.Namespace,
				"local_addr": backend.Address,
				"local_port": backend.Port,
				"labels":     g.mergeLabels(service.Selector, backend.Labels),
			},
		},
	}
}

// createProxyClientConfig 创建代理客户端配置
func (g *ConfigGenerator) createProxyClientConfig(deployment *config.Deployment, service *config.ServiceConfig) map[string]interface{} {
	// 找到主节点
	var primaryNode *config.NodeConfig
	for _, node := range deployment.Cluster.Nodes {
		if node.Role == "primary" {
			primaryNode = &node
			break
		}
	}
	if primaryNode == nil {
		primaryNode = &deployment.Cluster.Nodes[0]
	}

	return map[string]interface{}{
		"client": map[string]interface{}{
			"server_addr": fmt.Sprintf("%s:%d", primaryNode.Network.ExternalAddr, primaryNode.Network.Ports.Control),
			"auth_token":  deployment.Security.Auth.Token,
			"mode":        "proxy",
		},
		"proxies": []map[string]interface{}{
			{
				"name":              fmt.Sprintf("%s-proxy", service.Name),
				"service_name":      service.Name,
				"service_namespace": service.Namespace,
				"local_port":        service.Ports[0].Port + 1000, // 代理端口偏移
				"proxy_mode":        service.ProxyMode,
				"selector":          service.Selector,
			},
		},
	}
}

// createDockerCompose 创建Docker Compose配置
func (g *ConfigGenerator) createDockerCompose(deployment *config.Deployment) map[string]interface{} {
	compose := map[string]interface{}{
		"version": "3.8",
		"services": map[string]interface{}{},
		"networks": map[string]interface{}{
			"outpostnet": map[string]interface{}{
				"driver": "bridge",
			},
		},
	}

	services := compose["services"].(map[string]interface{})

	// 添加服务端容器
	for _, node := range deployment.Cluster.Nodes {
		services[fmt.Sprintf("outpostnet-server-%s", node.NodeID)] = map[string]interface{}{
			"image": "outpostnet:latest",
			"command": []string{
				"./outpostnet-server",
				"-config", fmt.Sprintf("/etc/outpostnet/server-%s.yaml", node.NodeID),
			},
			"ports": []string{
				fmt.Sprintf("%d:%d", node.Network.Ports.Control, node.Network.Ports.Control),
				fmt.Sprintf("%d:%d", node.Network.Ports.RegionMesh, node.Network.Ports.RegionMesh),
			},
			"volumes": []string{
				"./servers:/etc/outpostnet:ro",
			},
			"networks": []string{"outpostnet"},
			"restart": "unless-stopped",
		}
	}

	return compose
}

// createDockerfile 创建Dockerfile
func (g *ConfigGenerator) createDockerfile(deployment *config.Deployment) string {
	return `FROM golang:1.21-alpine AS builder

WORKDIR /app
COPY . .
RUN go mod download
RUN go build -o outpostnet-server ./cmd/outpostnet-server
RUN go build -o outpostnet-client ./cmd/outpostnet-client

FROM alpine:latest
RUN apk --no-cache add ca-certificates
WORKDIR /root/

COPY --from=builder /app/outpostnet-server .
COPY --from=builder /app/outpostnet-client .

CMD ["./outpostnet-server"]
`
}

// createNamespace 创建Kubernetes命名空间
func (g *ConfigGenerator) createNamespace(deployment *config.Deployment) map[string]interface{} {
	return map[string]interface{}{
		"apiVersion": "v1",
		"kind":       "Namespace",
		"metadata": map[string]interface{}{
			"name": deployment.Name,
			"labels": map[string]string{
				"app":     "outpostnet",
				"version": deployment.Version,
			},
		},
	}
}

// createConfigMap 创建Kubernetes ConfigMap
func (g *ConfigGenerator) createConfigMap(deployment *config.Deployment) map[string]interface{} {
	data := make(map[string]string)

	// 添加服务端配置
	for _, node := range deployment.Cluster.Nodes {
		serverConfig := g.createServerConfig(deployment, &node)
		configYAML, _ := yaml.Marshal(serverConfig)
		data[fmt.Sprintf("server-%s.yaml", node.NodeID)] = string(configYAML)
	}

	return map[string]interface{}{
		"apiVersion": "v1",
		"kind":       "ConfigMap",
		"metadata": map[string]interface{}{
			"name":      fmt.Sprintf("%s-config", deployment.Name),
			"namespace": deployment.Name,
		},
		"data": data,
	}
}

// createK8sDeployment 创建Kubernetes Deployment
func (g *ConfigGenerator) createK8sDeployment(deployment *config.Deployment, node *config.NodeConfig) map[string]interface{} {
	return map[string]interface{}{
		"apiVersion": "apps/v1",
		"kind":       "Deployment",
		"metadata": map[string]interface{}{
			"name":      fmt.Sprintf("outpostnet-server-%s", node.NodeID),
			"namespace": deployment.Name,
		},
		"spec": map[string]interface{}{
			"replicas": 1,
			"selector": map[string]interface{}{
				"matchLabels": map[string]string{
					"app":     "outpostnet-server",
					"node-id": node.NodeID,
				},
			},
			"template": map[string]interface{}{
				"metadata": map[string]interface{}{
					"labels": map[string]string{
						"app":     "outpostnet-server",
						"node-id": node.NodeID,
					},
				},
				"spec": map[string]interface{}{
					"containers": []map[string]interface{}{
						{
							"name":  "outpostnet-server",
							"image": "outpostnet:latest",
							"command": []string{
								"./outpostnet-server",
								"-config", fmt.Sprintf("/etc/outpostnet/server-%s.yaml", node.NodeID),
							},
							"ports": []map[string]interface{}{
								{
									"name":          "control",
									"containerPort": node.Network.Ports.Control,
									"protocol":      "TCP",
								},
								{
									"name":          "region-mesh",
									"containerPort": node.Network.Ports.RegionMesh,
									"protocol":      "TCP",
								},
							},
							"volumeMounts": []map[string]interface{}{
								{
									"name":      "config",
									"mountPath": "/etc/outpostnet",
									"readOnly":  true,
								},
							},
							"resources": g.convertResources(node.Resources),
						},
					},
					"volumes": []map[string]interface{}{
						{
							"name": "config",
							"configMap": map[string]interface{}{
								"name": fmt.Sprintf("%s-config", deployment.Name),
							},
						},
					},
				},
			},
		},
	}
}

// createK8sService 创建Kubernetes Service
func (g *ConfigGenerator) createK8sService(deployment *config.Deployment, service *config.ServiceConfig) map[string]interface{} {
	k8sService := map[string]interface{}{
		"apiVersion": "v1",
		"kind":       "Service",
		"metadata": map[string]interface{}{
			"name":      service.Name,
			"namespace": deployment.Name,
		},
		"spec": map[string]interface{}{
			"selector": map[string]string{
				"app": "outpostnet-server",
			},
			"ports": g.convertK8sPorts(service.Ports),
		},
	}

	if service.Type == "NodePort" {
		k8sService["spec"].(map[string]interface{})["type"] = "NodePort"
	} else {
		k8sService["spec"].(map[string]interface{})["type"] = "ClusterIP"
	}

	return k8sService
}

// createStartScript 创建启动脚本
func (g *ConfigGenerator) createStartScript(deployment *config.Deployment) string {
	var sb strings.Builder
	
	sb.WriteString("#!/bin/bash\n")
	sb.WriteString("# outpostnet start script\n")
	sb.WriteString("set -e\n\n")
	
	sb.WriteString("echo \"Starting outpostnet deployment: " + deployment.Name + "\"\n")
	sb.WriteString("echo \"Environment: " + deployment.Environment + "\"\n")
	sb.WriteString("echo \"Version: " + deployment.Version + "\"\n\n")
	
	// 启动服务端
	sb.WriteString("# Start servers\n")
	for _, node := range deployment.Cluster.Nodes {
		sb.WriteString(fmt.Sprintf("echo \"Starting server %s...\"\n", node.NodeID))
		sb.WriteString(fmt.Sprintf("./outpostnet-server -config servers/server-%s.yaml &\n", node.NodeID))
		sb.WriteString("sleep 2\n\n")
	}
	
	sb.WriteString("echo \"All servers started successfully!\"\n")
	sb.WriteString("echo \"Press Ctrl+C to stop all services\"\n")
	sb.WriteString("wait\n")
	
	return sb.String()
}

// createStopScript 创建停止脚本
func (g *ConfigGenerator) createStopScript(deployment *config.Deployment) string {
	return `#!/bin/bash
# outpostnet stop script

echo "Stopping outpostnet deployment..."

# Stop all outpostnet processes
pkill -f "outpostnet-server" || true
pkill -f "outpostnet-client" || true

echo "All outpostnet processes stopped"
`
}

// createDemoScript 创建演示脚本
func (g *ConfigGenerator) createDemoScript(deployment *config.Deployment) string {
	var sb strings.Builder
	
	sb.WriteString("#!/bin/bash\n")
	sb.WriteString("# outpostnet demo script\n")
	sb.WriteString("set -e\n\n")
	
	sb.WriteString("echo \"🚀 Starting outpostnet demo: " + deployment.Name + "\"\n")
	sb.WriteString("echo \"=====================================\"\n\n")
	
	// 根据集群类型生成不同的演示脚本
	if deployment.Cluster.Type == "region-mesh" {
		sb.WriteString("echo \"📋 Demo type: Region Service Mesh\"\n")
		sb.WriteString("echo \"Features: Cross-node communication, Health checking, Load balancing\"\n\n")
	} else {
		sb.WriteString("echo \"📋 Demo type: Standalone\"\n")
		sb.WriteString("echo \"Features: Basic proxy, Service discovery\"\n\n")
	}
	
	sb.WriteString("# Start demo\n")
	sb.WriteString("./scripts/start.sh\n")
	
	return sb.String()
}

// 辅助方法

// createNeighborsList 创建邻居节点列表
func (g *ConfigGenerator) createNeighborsList(deployment *config.Deployment, currentNode *config.NodeConfig) []map[string]interface{} {
	neighbors := make([]map[string]interface{}, 0)
	
	for _, node := range deployment.Cluster.Nodes {
		if node.NodeID != currentNode.NodeID {
			neighbors = append(neighbors, map[string]interface{}{
				"node_id": node.NodeID,
				"address": fmt.Sprintf("%s:%d", node.Network.ExternalAddr, node.Network.Ports.RegionMesh),
			})
		}
	}
	
	return neighbors
}

// convertPorts 转换端口配置
func (g *ConfigGenerator) convertPorts(ports []config.ServicePort) []map[string]interface{} {
	result := make([]map[string]interface{}, len(ports))
	for i, port := range ports {
		result[i] = map[string]interface{}{
			"name":        port.Name,
			"port":        port.Port,
			"target_port": port.TargetPort,
			"protocol":    port.Protocol,
		}
		if port.NodePort != 0 {
			result[i]["node_port"] = port.NodePort
		}
	}
	return result
}

// convertK8sPorts 转换Kubernetes端口配置
func (g *ConfigGenerator) convertK8sPorts(ports []config.ServicePort) []map[string]interface{} {
	result := make([]map[string]interface{}, len(ports))
	for i, port := range ports {
		result[i] = map[string]interface{}{
			"name":       port.Name,
			"port":       port.Port,
			"targetPort": port.TargetPort,
			"protocol":   strings.ToUpper(port.Protocol),
		}
		if port.NodePort != 0 {
			result[i]["nodePort"] = port.NodePort
		}
	}
	return result
}

// convertResources 转换资源配置
func (g *ConfigGenerator) convertResources(resources config.ResourceConfig) map[string]interface{} {
	if resources.CPU == "" && resources.Memory == "" {
		return nil
	}
	
	result := map[string]interface{}{
		"limits": map[string]string{},
		"requests": map[string]string{},
	}
	
	if resources.CPU != "" {
		result["limits"].(map[string]string)["cpu"] = resources.CPU
		result["requests"].(map[string]string)["cpu"] = resources.CPU
	}
	
	if resources.Memory != "" {
		result["limits"].(map[string]string)["memory"] = resources.Memory
		result["requests"].(map[string]string)["memory"] = resources.Memory
	}
	
	return result
}

// mergeLabels 合并标签
func (g *ConfigGenerator) mergeLabels(labels1, labels2 map[string]string) map[string]string {
	result := make(map[string]string)
	
	for k, v := range labels1 {
		result[k] = v
	}
	
	for k, v := range labels2 {
		result[k] = v
	}
	
	return result
}

// saveYAML 保存YAML文件
func (g *ConfigGenerator) saveYAML(data interface{}, filename string) error {
	yamlData, err := yaml.Marshal(data)
	if err != nil {
		return err
	}
	
	return os.WriteFile(filename, yamlData, 0644)
}