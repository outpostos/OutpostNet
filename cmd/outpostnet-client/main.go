package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"

	"github.com/outpostos/OutpostNet/pkg/client"
	"github.com/outpostos/OutpostNet/pkg/config"
	"github.com/outpostos/OutpostNet/pkg/types"
	"gopkg.in/yaml.v2"
)

var (
	configFile = flag.String("config", "", "path to configuration file")
	mode       = flag.String("mode", "proxy", "client mode: proxy or register (used when no config file)")
	version    = flag.Bool("version", false, "show version")
	
	// Express Mode 参数
	express     = flag.Bool("express", false, "enable express mode for quick setup")
	server      = flag.String("server", "127.0.0.1:7629", "express mode: server address")
	token       = flag.String("token", "outpostnet-express-token", "express mode: authentication token")
	service     = flag.String("service", "", "express mode: service name")
	localPort   = flag.Int("local-port", 0, "express mode: local port (proxy mode)")
	backend     = flag.String("backend", "", "express mode: backend address (register mode)")
	labels      = flag.String("labels", "", "express mode: labels in format 'key1=value1,key2=value2' (register mode)")
	showConfig  = flag.Bool("show-config", false, "express mode: show generated config and exit")
	saveConfig  = flag.String("save-config", "", "express mode: save generated config to file")
)

func main() {
	flag.Parse()

	if *version {
		fmt.Println("outpostnet-client version 0.1.0")
		return
	}

	var cfg *config.ClientConfig
	var err error

	// Express Mode 处理
	if *express {
		cfg, err = generateExpressClientConfig()
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
		
		log.Println("🚀 Starting outpostnet-client in Express Mode")
		log.Printf("✅ Mode: %s", cfg.Client.Mode)
		log.Printf("✅ Server: %s", cfg.Client.ServerAddr)
		log.Printf("✅ Auth Token: %s", cfg.Client.AuthToken)
		if cfg.Client.Mode == "proxy" && len(cfg.Proxies) > 0 {
			log.Printf("✅ Proxy: localhost:%d -> service:%s", cfg.Proxies[0].LocalPort, cfg.Proxies[0].ServiceName)
		} else if cfg.Client.Mode == "register" && len(cfg.Workloads) > 0 {
			log.Printf("✅ Register: %s:%d as service:%s", cfg.Workloads[0].Spec.LocalAddr, cfg.Workloads[0].Spec.LocalPort, *service)
		}
	} else {
		// 标准模式：加载配置文件
		cfg, err = config.LoadClientConfigWithMode(*configFile, *mode)
		if err != nil {
			log.Fatalf("failed to load config: %v", err)
		}
	}

	// 创建客户端
	cli, err := client.NewClient(cfg)
	if err != nil {
		log.Fatalf("failed to create client: %v", err)
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

	// 启动客户端
	if *configFile == "" {
		log.Printf("starting outpostnet-client with default configuration")
		log.Printf("mode: %s, server: %s, auth token: %s", cfg.Client.Mode, cfg.Client.ServerAddr, cfg.Client.AuthToken)
		if cfg.Client.Mode == "proxy" {
			log.Printf("proxy listening on port %d for service %s", cfg.Proxies[0].LocalPort, cfg.Proxies[0].ServiceName)
		} else if cfg.Client.Mode == "register" {
			log.Printf("registering local service on port %d with labels %v", cfg.Workloads[0].Spec.LocalPort, cfg.Workloads[0].Metadata.Labels)
		}
	} else {
		log.Printf("starting outpostnet-client with config file: %s", *configFile)
		log.Printf("mode: %s, connecting to %s", cfg.Client.Mode, cfg.Client.ServerAddr)
	}
	if err := cli.Run(ctx); err != nil {
		log.Fatalf("client error: %v", err)
	}
}

// generateExpressClientConfig 生成Express Mode客户端配置
func generateExpressClientConfig() (*config.ClientConfig, error) {
	// 验证必需参数
	if *service == "" {
		return nil, fmt.Errorf("service name is required in express mode")
	}

	// 基础配置
	cfg := &config.ClientConfig{
		Client: config.ClientSpec{
			ServerAddr: *server,
			AuthToken:  *token,
			Mode:       *mode,
		},
	}

	// 根据模式生成配置
	switch *mode {
	case "proxy":
		if *localPort == 0 {
			return nil, fmt.Errorf("local-port is required for proxy mode")
		}
		
		cfg.Proxies = []types.Proxy{
			{
				Name:             fmt.Sprintf("%s-proxy", *service),
				ServiceName:      *service,
				ServiceNamespace: "default",
				LocalPort:        *localPort,
				ProxyMode:        "tcp",
				Selector: map[string]string{
					"app": *service,
				},
				LoadBalancer: "round_robin",
			},
		}

	case "register":
		if *backend == "" {
			return nil, fmt.Errorf("backend is required for register mode")
		}
		
		backendAddr, backendPort, err := parseBackend(*backend)
		if err != nil {
			return nil, fmt.Errorf("invalid backend format: %v", err)
		}
		
		// 解析标签
		labelsMap := map[string]string{
			"app":                    *service,
			"outpostnet.io/express":     "true",
			"outpostnet.io/created-by": "express-mode",
		}
		
		if *labels != "" {
			parsedLabels, err := parseLabels(*labels)
			if err != nil {
				return nil, fmt.Errorf("invalid labels format: %v", err)
			}
			for k, v := range parsedLabels {
				labelsMap[k] = v
			}
		}
		
		cfg.Workloads = []config.ClientWorkload{
			{
				Metadata: config.ClientWorkloadMetadata{
					Labels: labelsMap,
				},
				Spec: config.ClientWorkloadSpec{
					LocalAddr: backendAddr,
					LocalPort: backendPort,
					Protocol:  "tcp",
				},
			},
		}

	default:
		return nil, fmt.Errorf("unsupported mode: %s", *mode)
	}

	return cfg, nil
}

// parseBackend 解析后端地址: "host:port"
func parseBackend(backend string) (string, int, error) {
	parts := strings.Split(backend, ":")
	if len(parts) != 2 {
		return "", 0, fmt.Errorf("backend must be in format 'host:port'")
	}
	
	host := parts[0]
	if host == "" {
		return "", 0, fmt.Errorf("host cannot be empty")
	}
	
	port, err := strconv.Atoi(parts[1])
	if err != nil {
		return "", 0, fmt.Errorf("invalid port: %v", err)
	}
	
	if port < 1 || port > 65535 {
		return "", 0, fmt.Errorf("port must be between 1 and 65535")
	}
	
	return host, port, nil
}

// parseLabels 解析标签: "key1=value1,key2=value2"
func parseLabels(labelsStr string) (map[string]string, error) {
	labels := make(map[string]string)
	
	if labelsStr == "" {
		return labels, nil
	}
	
	pairs := strings.Split(labelsStr, ",")
	for _, pair := range pairs {
		kv := strings.Split(pair, "=")
		if len(kv) != 2 {
			return nil, fmt.Errorf("label must be in format 'key=value'")
		}
		
		key := strings.TrimSpace(kv[0])
		value := strings.TrimSpace(kv[1])
		
		if key == "" {
			return nil, fmt.Errorf("label key cannot be empty")
		}
		
		labels[key] = value
	}
	
	return labels, nil
}

// printConfig 打印配置到标准输出
func printConfig(cfg *config.ClientConfig) error {
	yamlData, err := yaml.Marshal(cfg)
	if err != nil {
		return err
	}
	fmt.Print(string(yamlData))
	return nil
}

// saveConfigToFile 保存配置到文件
func saveConfigToFile(cfg *config.ClientConfig, filename string) error {
	yamlData, err := yaml.Marshal(cfg)
	if err != nil {
		return err
	}
	return os.WriteFile(filename, yamlData, 0644)
}