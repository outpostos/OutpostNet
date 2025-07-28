package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/outpostos/OutpostNet/pkg/config"
	"github.com/outpostos/OutpostNet/pkg/server"
	"github.com/outpostos/OutpostNet/pkg/types"
	"gopkg.in/yaml.v2"
)

var (
	configFile = flag.String("config", "", "path to configuration file")
	version    = flag.Bool("version", false, "show version")
	
	// Express Mode 参数
	express     = flag.Bool("express", false, "enable express mode for quick setup")
	port        = flag.Int("port", 7629, "express mode: server listen port")
	token       = flag.String("token", "outpostnet-express-token", "express mode: authentication token")
	services    expressServices
	showConfig  = flag.Bool("show-config", false, "express mode: show generated config and exit")
	saveConfig  = flag.String("save-config", "", "express mode: save generated config to file")
	
	// Phase 2 Express Mode: 区域配置参数
	regionID    = flag.String("region-id", "", "express mode: region identifier for Phase 2 cluster mode")
	nodeID      = flag.String("node-id", "", "express mode: node identifier for Phase 2 cluster mode")
	neighbors   expressNeighbors
)

// expressServices 实现 flag.Value 接口，用于解析多个服务定义
type expressServices []string

func (s *expressServices) String() string {
	return strings.Join(*s, ",")
}

func (s *expressServices) Set(value string) error {
	*s = append(*s, value)
	return nil
}

// expressNeighbors 实现 flag.Value 接口，用于解析邻居节点定义 (Phase 2)
type expressNeighbors []string

func (n *expressNeighbors) String() string {
	return strings.Join(*n, ",")
}

func (n *expressNeighbors) Set(value string) error {
	*n = append(*n, value)
	return nil
}

func init() {
	// 注册自定义 flag
	flag.Var(&services, "service", "express mode: define service in format 'name:port:targetPort' (can be used multiple times)")
	flag.Var(&neighbors, "neighbor", "express mode: define neighbor in format 'nodeID:address:port' (can be used multiple times)")
}

func main() {
	flag.Parse()

	if *version {
		fmt.Println("outpostnet-server version 0.1.0")
		return
	}

	var cfg *config.ServerConfig
	var err error

	// Express Mode 处理
	if *express {
		cfg, err = generateExpressConfig()
		if err != nil {
			log.Fatalf("failed to generate express config: %v", err)
		}
		
		// 显示配置并退出
		if *showConfig {
			if err := printConfig(cfg); err != nil {
				log.Fatalf("failed to print config: %v", err)
			}
			return
		}
		
		// 保存配置到文件
		if *saveConfig != "" {
			if err := saveConfigToFile(cfg, *saveConfig); err != nil {
				log.Fatalf("failed to save config: %v", err)
			}
			log.Printf("Express config saved to: %s", *saveConfig)
			return
		}
		
		log.Println("🚀 Starting outpostnet-server in Express Mode")
		log.Printf("✅ Listen Address: %s", cfg.Server.ListenAddr)
		log.Printf("✅ Auth Token: %s", cfg.Server.AuthToken)
		log.Printf("✅ K8s API: https://localhost:%d", cfg.K8sAPI.Port)
		if len(cfg.Services) > 0 {
			log.Printf("✅ Pre-configured Services:")
			for _, svc := range cfg.Services {
				for _, port := range svc.Spec.Ports {
					log.Printf("   - %s:%d -> %d (%s)", svc.Metadata.Name, port.Port, port.TargetPort, port.Protocol)
				}
			}
		}
	} else {
		// 标准模式：加载配置文件
		cfg, err = config.LoadServerConfig(*configFile)
		if err != nil {
			log.Fatalf("failed to load config: %v", err)
		}
	}

	// 创建服务器
	srv, err := server.NewServer(cfg)
	if err != nil {
		log.Fatalf("failed to create server: %v", err)
	}

	// 设置信号处理
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 监听终止信号
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigCh
		log.Printf("received signal: %v, shutting down...", sig)
		cancel()
	}()

	// 启动服务器
	if *configFile == "" {
		log.Printf("starting outpostnet-server with default configuration on %s", cfg.Server.ListenAddr)
		log.Printf("using default auth token: %s", cfg.Server.AuthToken)
		log.Printf("K8s API enabled on port %d", cfg.K8sAPI.Port)
	} else {
		log.Printf("starting outpostnet-server with config file: %s", *configFile)
		log.Printf("listening on %s", cfg.Server.ListenAddr)
	}
	if err := srv.Run(ctx); err != nil {
		log.Fatalf("server error: %v", err)
	}
}

// generateExpressConfig 生成Express Mode配置
func generateExpressConfig() (*config.ServerConfig, error) {
	cfg := &config.ServerConfig{
		Server: config.ServerSpec{
			ListenAddr: fmt.Sprintf("0.0.0.0:%d", *port),
			AuthToken:  *token,
		},
		K8sAPI: config.K8sAPISpec{
			Enabled: true,
			Port:    8443,
		},
	}

	// Phase 2: 检查并生成区域配置
	if *regionID != "" && *nodeID != "" {
		neighborNodes, err := parseNeighborDefinitions(neighbors)
		if err != nil {
			return nil, fmt.Errorf("invalid neighbor definitions: %v", err)
		}
		
		cfg.Region = &types.RegionConfig{
			Enabled:  true,
			RegionID: *regionID,
			NodeID:   *nodeID,
			Neighbors: neighborNodes,
			ServiceSync: types.ServiceSyncConfig{
				SyncInterval: 30000000000, // 30秒，以纳秒为单位
				RetryCount:   3,
				HealthCheck:  true,
				CacheTimeout: 300000000000, // 5分钟，以纳秒为单位
			},
		}
		
		log.Printf("🌍 Phase 2 Region Mode Enabled:")
		log.Printf("   Region ID: %s", *regionID)
		log.Printf("   Node ID: %s", *nodeID)
		log.Printf("   Neighbors: %d nodes", len(neighborNodes))
	}

	// 解析服务定义
	for _, serviceDef := range services {
		service, err := parseServiceDefinition(serviceDef)
		if err != nil {
			return nil, fmt.Errorf("invalid service definition '%s': %v", serviceDef, err)
		}
		
		// Phase 2: 为服务添加地理元数据
		if cfg.Region != nil && cfg.Region.Enabled {
			service.Metadata.Region = cfg.Region.RegionID
			service.Metadata.Site = cfg.Region.NodeID // 简化：使用nodeID作为siteID
			if service.Metadata.Labels == nil {
				service.Metadata.Labels = make(map[string]string)
			}
			service.Metadata.Labels["outpostnet.io/region"] = cfg.Region.RegionID
			service.Metadata.Labels["outpostnet.io/node"] = cfg.Region.NodeID
		}
		
		cfg.Services = append(cfg.Services, *service)
	}

	// 如果没有定义服务，添加默认服务
	if len(cfg.Services) == 0 {
		service := types.Service{
			Metadata: types.ServiceMetadata{
				Name:      "express-default-service",
				Namespace: "default",
				Labels: map[string]string{
					"app":                    "express-default",
					"outpostnet.io/express":     "true",
					"outpostnet.io/created-by": "express-mode",
				},
			},
			Spec: types.ServiceSpec{
				Selector: map[string]string{
					"app": "express-default",
				},
				Ports: []types.ServicePort{
					{
						Name:       "http",
						Port:       80,
						TargetPort: 8080,
						Protocol:   "tcp",
					},
				},
				ProxyMode:    "tcp",
				LoadBalancer: "round_robin",
			},
		}
		
		// Phase 2: 为默认服务添加地理元数据
		if cfg.Region != nil && cfg.Region.Enabled {
			service.Metadata.Region = cfg.Region.RegionID
			service.Metadata.Site = cfg.Region.NodeID
			service.Metadata.Labels["outpostnet.io/region"] = cfg.Region.RegionID
			service.Metadata.Labels["outpostnet.io/node"] = cfg.Region.NodeID
		}
		
		cfg.Services = []types.Service{service}
	}

	return cfg, nil
}

// parseServiceDefinition 解析服务定义字符串: "name:port:targetPort"
func parseServiceDefinition(def string) (*types.Service, error) {
	parts := strings.Split(def, ":")
	if len(parts) != 3 {
		return nil, fmt.Errorf("service definition must be in format 'name:port:targetPort'")
	}

	name := parts[0]
	if name == "" {
		return nil, fmt.Errorf("service name cannot be empty")
	}

	port, err := parsePort(parts[1])
	if err != nil {
		return nil, fmt.Errorf("invalid port: %v", err)
	}

	targetPort, err := parsePort(parts[2])
	if err != nil {
		return nil, fmt.Errorf("invalid target port: %v", err)
	}

	service := &types.Service{
		Metadata: types.ServiceMetadata{
			Name:      name,
			Namespace: "default",
			Labels: map[string]string{
				"app":                    name,
				"outpostnet.io/express":     "true",
				"outpostnet.io/created-by": "express-mode",
			},
		},
		Spec: types.ServiceSpec{
			Selector: map[string]string{
				"app": name,
			},
			Ports: []types.ServicePort{
				{
					Name:       "http",
					Port:       port,
					TargetPort: targetPort,
					Protocol:   "tcp",
				},
			},
			ProxyMode:    "tcp",
			LoadBalancer: "round_robin",
		},
	}

	return service, nil
}

// parsePort 解析端口号
func parsePort(portStr string) (int, error) {
	var port int
	if _, err := fmt.Sscanf(portStr, "%d", &port); err != nil {
		return 0, err
	}
	if port < 1 || port > 65535 {
		return 0, fmt.Errorf("port must be between 1 and 65535")
	}
	return port, nil
}

// parseNeighborDefinitions 解析邻居节点定义: "nodeID:address:port" (Phase 2)
func parseNeighborDefinitions(neighborDefs []string) ([]types.NeighborNode, error) {
	var neighbors []types.NeighborNode
	
	for _, def := range neighborDefs {
		neighbor, err := parseNeighborDefinition(def)
		if err != nil {
			return nil, fmt.Errorf("invalid neighbor definition '%s': %v", def, err)
		}
		neighbors = append(neighbors, *neighbor)
	}
	
	return neighbors, nil
}

// parseNeighborDefinition 解析单个邻居节点定义: "nodeID:address:port"
func parseNeighborDefinition(def string) (*types.NeighborNode, error) {
	parts := strings.Split(def, ":")
	if len(parts) < 2 || len(parts) > 3 {
		return nil, fmt.Errorf("neighbor definition must be in format 'nodeID:address' or 'nodeID:address:port'")
	}
	
	nodeID := parts[0]
	if nodeID == "" {
		return nil, fmt.Errorf("node ID cannot be empty")
	}
	
	address := parts[1]
	if address == "" {
		return nil, fmt.Errorf("address cannot be empty")
	}
	
	// 如果指定了端口，解析端口；否则使用默认端口
	port := 7629 // 默认端口
	if len(parts) == 3 {
		var err error
		port, err = parsePort(parts[2])
		if err != nil {
			return nil, fmt.Errorf("invalid port: %v", err)
		}
	}
	
	// 组合完整地址
	fullAddress := fmt.Sprintf("%s:%d", address, port)
	
	neighbor := &types.NeighborNode{
		NodeID:   nodeID,
		Address:  fullAddress,
		Weight:   1, // 默认权重
		Zone:     "", // 可以后续扩展
		Status:   "unknown", // 初始状态
	}
	
	return neighbor, nil
}

// printConfig 打印配置到标准输出
func printConfig(cfg *config.ServerConfig) error {
	yamlData, err := yaml.Marshal(cfg)
	if err != nil {
		return err
	}
	fmt.Print(string(yamlData))
	return nil
}

// saveConfigToFile 保存配置到文件
func saveConfigToFile(cfg *config.ServerConfig, filename string) error {
	yamlData, err := yaml.Marshal(cfg)
	if err != nil {
		return err
	}
	return os.WriteFile(filename, yamlData, 0644)
}