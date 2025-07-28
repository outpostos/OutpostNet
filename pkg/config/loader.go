package config

import (
	"fmt"
	"os"
	"path/filepath"

	"gopkg.in/yaml.v3"
)

// LoadServerConfig 加载服务端配置文件，如果路径为空则返回默认配置
func LoadServerConfig(configPath string) (*ServerConfig, error) {
	if configPath == "" {
		return DefaultServerConfig(), nil
	}

	if !filepath.IsAbs(configPath) {
		abs, err := filepath.Abs(configPath)
		if err != nil {
			return nil, fmt.Errorf("failed to get absolute path for %s: %v", configPath, err)
		}
		configPath = abs
	}

	data, err := os.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file %s: %v", configPath, err)
	}

	var config ServerConfig
	if err := yaml.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("failed to unmarshal config file %s: %v", configPath, err)
	}

	if err := ValidateServerConfig(&config); err != nil {
		return nil, fmt.Errorf("invalid server config: %v", err)
	}

	return &config, nil
}

// LoadClientConfig 加载客户端配置文件，如果路径为空则返回默认配置
func LoadClientConfig(configPath string) (*ClientConfig, error) {
	if configPath == "" {
		return DefaultClientConfig("proxy"), nil
	}

	if !filepath.IsAbs(configPath) {
		abs, err := filepath.Abs(configPath)
		if err != nil {
			return nil, fmt.Errorf("failed to get absolute path for %s: %v", configPath, err)
		}
		configPath = abs
	}

	data, err := os.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file %s: %v", configPath, err)
	}

	var config ClientConfig
	if err := yaml.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("failed to unmarshal config file %s: %v", configPath, err)
	}

	if err := ValidateClientConfig(&config); err != nil {
		return nil, fmt.Errorf("invalid client config: %v", err)
	}

	return &config, nil
}

// LoadClientConfigWithMode 加载客户端配置文件，支持指定默认模式
func LoadClientConfigWithMode(configPath string, defaultMode string) (*ClientConfig, error) {
	if configPath == "" {
		if defaultMode == "" {
			defaultMode = "proxy"
		}
		return DefaultClientConfig(defaultMode), nil
	}

	return LoadClientConfig(configPath)
}