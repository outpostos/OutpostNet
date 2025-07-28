package main

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

// TestEndToEndDemo 测试完整的端到端场景
func TestEndToEndDemo(t *testing.T) {
	// 确保构建了最新的二进制文件
	t.Logf("Building outpostnet binaries...")
	buildCmd := exec.Command("make", "build")
	buildCmd.Dir = "/Users/neov/src/github.com/outpostos/OutpostNet"
	if err := buildCmd.Run(); err != nil {
		t.Fatalf("Failed to build binaries: %v", err)
	}

	// 1. 启动 outpostnet-server
	t.Logf("Starting outpostnet-server...")
	serverCmd := exec.Command("./build/outpostnet-server", "-config", "demo-server-config.yaml")
	serverCmd.Dir = "/Users/neov/src/github.com/outpostos/OutpostNet"
	serverCmd.Stdout = os.Stdout
	serverCmd.Stderr = os.Stderr
	
	if err := serverCmd.Start(); err != nil {
		t.Fatalf("Failed to start outpostnet-server: %v", err)
	}
	defer func() {
		if serverCmd.Process != nil {
			serverCmd.Process.Kill()
		}
	}()

	// 等待服务器启动
	time.Sleep(2 * time.Second)

	// 2. 启动 register 模式的客户端
	t.Logf("Starting outpostnet-client in register mode...")
	registerCmd := exec.Command("./build/outpostnet-client", "-config", "demo-register-config.yaml")
	registerCmd.Dir = "/Users/neov/src/github.com/outpostos/OutpostNet"
	registerCmd.Stdout = os.Stdout
	registerCmd.Stderr = os.Stderr

	if err := registerCmd.Start(); err != nil {
		t.Fatalf("Failed to start register client: %v", err)
	}
	defer func() {
		if registerCmd.Process != nil {
			registerCmd.Process.Kill()
		}
	}()

	// 等待注册完成
	time.Sleep(2 * time.Second)

	// 3. 启动 proxy 模式的客户端
	t.Logf("Starting outpostnet-client in proxy mode...")
	proxyCmd := exec.Command("./build/outpostnet-client", "-config", "demo-proxy-config.yaml")
	proxyCmd.Dir = "/Users/neov/src/github.com/outpostos/OutpostNet"
	proxyCmd.Stdout = os.Stdout
	proxyCmd.Stderr = os.Stderr

	if err := proxyCmd.Start(); err != nil {
		t.Fatalf("Failed to start proxy client: %v", err)
	}
	defer func() {
		if proxyCmd.Process != nil {
			proxyCmd.Process.Kill()
		}
	}()

	// 等待代理启动
	time.Sleep(2 * time.Second)

	// 4. 测试代理功能
	t.Logf("Testing proxy functionality...")
	
	// 等待服务完全启动
	maxRetries := 10
	for i := 0; i < maxRetries; i++ {
		resp, err := http.Get("http://localhost:19999")
		if err == nil {
			defer resp.Body.Close()
			
			if resp.StatusCode == 200 {
				body, err := io.ReadAll(resp.Body)
				if err != nil {
					t.Fatalf("Failed to read response body: %v", err)
				}
				
				bodyStr := string(body)
				t.Logf("✓ Successfully received response from proxy:")
				t.Logf("  Status: %d", resp.StatusCode)
				t.Logf("  Body: %s", strings.TrimSpace(bodyStr))
				
				// 验证响应包含预期的内容（通常是目录列表或默认页面）
				if len(bodyStr) > 0 {
					t.Logf("✓ Proxy is working correctly!")
					return
				}
			}
		}
		
		if i == maxRetries-1 {
			t.Fatalf("Failed to connect to proxy after %d attempts, last error: %v", maxRetries, err)
		}
		
		t.Logf("Retry %d/%d: %v", i+1, maxRetries, err)
		time.Sleep(1 * time.Second)
	}
}

// TestDirectConnection 测试直接连接到 18000 端口
func TestDirectConnection(t *testing.T) {
	t.Logf("Testing direct connection to localhost:18000...")
	
	resp, err := http.Get("http://localhost:18000")
	if err != nil {
		t.Fatalf("Failed to connect directly to localhost:18000: %v", err)
	}
	defer resp.Body.Close()
	
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("Failed to read response body: %v", err)
	}
	
	t.Logf("✓ Direct connection successful:")
	t.Logf("  Status: %d", resp.StatusCode)
	t.Logf("  Body: %s", strings.TrimSpace(string(body)))
}

// TestConfigValidation 测试配置文件的有效性
func TestConfigValidation(t *testing.T) {
	configFiles := []string{
		"demo-server-config.yaml",
		"demo-register-config.yaml", 
		"demo-proxy-config.yaml",
	}
	
	for _, configFile := range configFiles {
		t.Run(configFile, func(t *testing.T) {
			// 检查配置文件是否存在
			if _, err := os.Stat(configFile); os.IsNotExist(err) {
				t.Fatalf("Config file %s does not exist", configFile)
			}
			
			// 读取配置文件内容
			content, err := os.ReadFile(configFile)
			if err != nil {
				t.Fatalf("Failed to read config file %s: %v", configFile, err)
			}
			
			t.Logf("✓ Config file %s exists and is readable", configFile)
			t.Logf("  Content length: %d bytes", len(content))
			
			// 显示配置内容
			lines := strings.Split(string(content), "\n")
			for i, line := range lines {
				if i < 10 { // 只显示前10行
					t.Logf("  %2d: %s", i+1, line)
				}
			}
			if len(lines) > 10 {
				t.Logf("  ... (%d more lines)", len(lines)-10)
			}
		})
	}
}

// TestPortAvailability 测试端口可用性
func TestPortAvailability(t *testing.T) {
	ports := []int{17000, 18000, 19999}
	
	for _, port := range ports {
		t.Run(fmt.Sprintf("port_%d", port), func(t *testing.T) {
			url := fmt.Sprintf("http://localhost:%d", port)
			
			// 尝试连接，不需要成功，只需要知道端口状态
			resp, err := http.Get(url)
			if err != nil {
				t.Logf("Port %d: %v", port, err)
				return
			}
			defer resp.Body.Close()
			
			t.Logf("Port %d: responding with status %d", port, resp.StatusCode)
		})
	}
}