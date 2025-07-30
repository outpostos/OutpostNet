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
	
	// 智能默认配置参数
	server      = flag.String("server", "127.0.0.1:7629", "server address")
	token       = flag.String("token", "", "authentication token")
	service     = flag.String("service", "", "service name")
	localPort   = flag.Int("local-port", 0, "local port (proxy mode)")
	backend     = flag.String("backend", "", "backend address (register mode)")
	labels      = flag.String("labels", "", "labels in format 'key1=value1,key2=value2' (register mode)")
	showConfig  = flag.Bool("show-config", false, "show current config and exit")
	saveConfig  = flag.String("save-config", "", "save current config to file")
)

func main() {
	flag.Parse()

	if *version {
		fmt.Println("outpostnet-client version 0.1.0")
		return
	}

	var cfg *config.ClientConfig
	var err error

	// 加载基础配置（文件或默认）
	if *configFile != "" {
		cfg, err = config.LoadClientConfigWithMode(*configFile, *mode)
		if err != nil {
			log.Fatalf("failed to load config: %v", err)
		}
	} else {
		// 使用智能默认配置
		cfg = generateSmartClientConfig()
	}

	// 应用命令行参数覆盖
	applyClientCommandLineOverrides(cfg)

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
		log.Printf("Config saved to: %s", *saveConfig)
		return
	}

	// 显示配置信息
	log.Printf("🚀 Starting outpostnet-client")
	log.Printf("✅ Mode: %s", cfg.Client.Mode)
	log.Printf("✅ Server: %s", cfg.Client.ServerAddr)
	log.Printf("✅ Auth Token: %s", cfg.Client.AuthToken)
	if cfg.Client.Mode == "proxy" && len(cfg.Proxies) > 0 {
		log.Printf("✅ Proxy: localhost:%d -> service:%s", cfg.Proxies[0].LocalPort, cfg.Proxies[0].ServiceName)
	} else if cfg.Client.Mode == "register" && len(cfg.Workloads) > 0 {
		log.Printf("✅ Register: %s:%d", cfg.Workloads[0].Spec.LocalAddr, cfg.Workloads[0].Spec.LocalPort)
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

// generateSmartClientConfig 生成智能默认客户端配置
func generateSmartClientConfig() *config.ClientConfig {
	// 使用默认配置作为基础
	cfg := config.DefaultClientConfig(*mode)
	
	return cfg
}

// applyClientCommandLineOverrides 应用客户端命令行参数覆盖
func applyClientCommandLineOverrides(cfg *config.ClientConfig) {
	// 应用服务器地址参数覆盖
	if *server != "127.0.0.1:7629" { // 如果不是默认值
		cfg.Client.ServerAddr = *server
	}
	
	// 应用token覆盖
	if *token != "" {
		cfg.Client.AuthToken = *token
	}

	// 根据模式和命令行参数生成配置
	switch *mode {
	case "proxy":
		if *service != "" && *localPort > 0 {
			proxy := types.Proxy{
				Name:             fmt.Sprintf("%s-proxy", *service),
				ServiceName:      *service,
				ServiceNamespace: "default",
				LocalPort:        *localPort,
				ProxyMode:        "tcp",
				Selector: map[string]string{
					"app": *service,
				},
				LoadBalancer: "round_robin",
			}
			cfg.Proxies = append(cfg.Proxies, proxy)
		}

	case "register":
		if *backend != "" {
			backendAddr, backendPort, err := parseBackend(*backend)
			if err != nil {
				log.Fatalf("invalid backend format: %v", err)
			}
			
			// 解析标签
			labelsMap := map[string]string{
				"outpostnet.io/created-by": "command-line",
			}
			
			if *service != "" {
				labelsMap["app"] = *service
			}
			
			if *labels != "" {
				parsedLabels, err := parseLabels(*labels)
				if err != nil {
					log.Fatalf("invalid labels format: %v", err)
				}
				for k, v := range parsedLabels {
					labelsMap[k] = v
				}
			}
			
			workload := config.ClientWorkload{
				Metadata: config.ClientWorkloadMetadata{
					Labels: labelsMap,
				},
				Spec: config.ClientWorkloadSpec{
					LocalAddr: backendAddr,
					LocalPort: backendPort,
					Protocol:  "tcp",
				},
			}
			cfg.Workloads = append(cfg.Workloads, workload)
		}
	}
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