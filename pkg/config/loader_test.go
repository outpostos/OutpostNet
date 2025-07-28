package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadServerConfig(t *testing.T) {
	// 创建临时目录
	tmpDir, err := os.MkdirTemp("", "outpostnet_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	tests := []struct {
		name        string
		configYAML  string
		expectError bool
	}{
		{
			name: "valid server config",
			configYAML: `
server:
  listen_addr: "0.0.0.0:8080"
  auth_token: "test-token"
services:
  - metadata:
      name: "web-service"
      namespace: "default"
      labels:
        app: "web"
    spec:
      selector:
        app: "web"
      ports:
        - name: "http"
          port: 80
          targetPort: 8080
          protocol: "tcp"
      proxy_mode: "tcp"
      load_balancer: "round_robin"
`,
			expectError: false,
		},
		{
			name: "invalid server config - missing listen_addr",
			configYAML: `
server:
  auth_token: "test-token"
services: []
`,
			expectError: true,
		},
		{
			name: "invalid server config - invalid port",
			configYAML: `
server:
  listen_addr: "0.0.0.0:8080"
services:
  - metadata:
      name: "web-service"
      namespace: "default"
    spec:
      ports:
        - name: "http"
          port: 0
          targetPort: 8080
          protocol: "tcp"
`,
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// 创建配置文件
			configFile := filepath.Join(tmpDir, "config.yaml")
			err := os.WriteFile(configFile, []byte(tt.configYAML), 0644)
			if err != nil {
				t.Fatal(err)
			}

			// 加载配置
			config, err := LoadServerConfig(configFile)

			if tt.expectError {
				if err == nil {
					t.Errorf("expected error but got none")
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
				if config == nil {
					t.Errorf("expected config but got nil")
				}
			}
		})
	}
}

func TestLoadClientConfig(t *testing.T) {
	// 创建临时目录
	tmpDir, err := os.MkdirTemp("", "outpostnet_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	tests := []struct {
		name        string
		configYAML  string
		expectError bool
	}{
		{
			name: "valid client config - proxy mode",
			configYAML: `
client:
  server_addr: "127.0.0.1:8080"
  auth_token: "test-token"
  mode: "proxy"
proxies:
  - name: "web-proxy"
    service_name: "web-service"
    service_namespace: "default"
    local_port: 8080
    proxy_mode: "tcp"
`,
			expectError: false,
		},
		{
			name: "valid client config - register mode",
			configYAML: `
client:
  server_addr: "127.0.0.1:8080"
  auth_token: "test-token"
  mode: "register"
services:
  - name: "web-service"
    namespace: "default"
    local_addr: "127.0.0.1"
    local_port: 8080
    labels:
      app: "web"
`,
			expectError: false,
		},
		{
			name: "invalid client config - missing server_addr",
			configYAML: `
client:
  auth_token: "test-token"
  mode: "proxy"
`,
			expectError: true,
		},
		{
			name: "invalid client config - invalid mode",
			configYAML: `
client:
  server_addr: "127.0.0.1:8080"
  auth_token: "test-token"
  mode: "invalid"
`,
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// 创建配置文件
			configFile := filepath.Join(tmpDir, "config.yaml")
			err := os.WriteFile(configFile, []byte(tt.configYAML), 0644)
			if err != nil {
				t.Fatal(err)
			}

			// 加载配置
			config, err := LoadClientConfig(configFile)

			if tt.expectError {
				if err == nil {
					t.Errorf("expected error but got none")
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
				if config == nil {
					t.Errorf("expected config but got nil")
				}
			}
		})
	}
}

func TestLoadConfigErrors(t *testing.T) {
	tests := []struct {
		name   string
		path   string
		loader func(string) (interface{}, error)
	}{
		{
			name: "empty path",
			path: "",
			loader: func(path string) (interface{}, error) {
				return LoadServerConfig(path)
			},
		},
		{
			name: "non-existent file",
			path: "/non/existent/file.yaml",
			loader: func(path string) (interface{}, error) {
				return LoadServerConfig(path)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := tt.loader(tt.path)
			if err == nil {
				t.Errorf("expected error but got none")
			}
		})
	}
}