package config

import (
	"testing"

	"github.com/outpostos/OutpostNet/pkg/types"
)

func TestValidateServerConfig(t *testing.T) {
	tests := []struct {
		name        string
		config      *ServerConfig
		expectError bool
	}{
		{
			name: "valid server config",
			config: &ServerConfig{
				Server: ServerSpec{
					ListenAddr: "0.0.0.0:8080",
					AuthToken:  "test-token",
				},
				Services: []types.Service{
					{
						Metadata: types.ServiceMetadata{
							Name:      "web-service",
							Namespace: "default",
						},
						Spec: types.ServiceSpec{
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
					},
				},
			},
			expectError: false,
		},
		{
			name:        "nil config",
			config:      nil,
			expectError: true,
		},
		{
			name: "missing listen_addr",
			config: &ServerConfig{
				Server: ServerSpec{
					AuthToken: "test-token",
				},
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateServerConfig(tt.config)
			if tt.expectError && err == nil {
				t.Errorf("expected error but got none")
			}
			if !tt.expectError && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}

func TestValidateClientConfig(t *testing.T) {
	tests := []struct {
		name        string
		config      *ClientConfig
		expectError bool
	}{
		{
			name: "valid client config",
			config: &ClientConfig{
				Client: ClientSpec{
					ServerAddr: "127.0.0.1:8080",
					AuthToken:  "test-token",
					Mode:       "proxy",
				},
				Proxies: []types.Proxy{
					{
						Name:             "web-proxy",
						ServiceName:      "web-service",
						ServiceNamespace: "default",
						LocalPort:        8080,
						ProxyMode:        "tcp",
					},
				},
			},
			expectError: false,
		},
		{
			name:        "nil config",
			config:      nil,
			expectError: true,
		},
		{
			name: "missing server_addr",
			config: &ClientConfig{
				Client: ClientSpec{
					AuthToken: "test-token",
				},
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateClientConfig(tt.config)
			if tt.expectError && err == nil {
				t.Errorf("expected error but got none")
			}
			if !tt.expectError && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}

func TestIsValidAddr(t *testing.T) {
	tests := []struct {
		addr  string
		valid bool
	}{
		{"127.0.0.1:8080", true},
		{"0.0.0.0:80", true},
		{"localhost:3000", true},
		{"invalid", false},
		{"127.0.0.1", false},
		{"127.0.0.1:0", false},
		{"127.0.0.1:65536", false},
		{":8080", false},
	}

	for _, tt := range tests {
		t.Run(tt.addr, func(t *testing.T) {
			result := isValidAddr(tt.addr)
			if result != tt.valid {
				t.Errorf("isValidAddr(%s) = %v, want %v", tt.addr, result, tt.valid)
			}
		})
	}
}

func TestIsValidProxyMode(t *testing.T) {
	tests := []struct {
		mode  string
		valid bool
	}{
		{"tcp", true},
		{"udp", true},
		{"stcp", true},
		{"sudp", true},
		{"http", false},
		{"invalid", false},
		{"", false},
	}

	for _, tt := range tests {
		t.Run(tt.mode, func(t *testing.T) {
			result := isValidProxyMode(tt.mode)
			if result != tt.valid {
				t.Errorf("isValidProxyMode(%s) = %v, want %v", tt.mode, result, tt.valid)
			}
		})
	}
}

func TestIsValidLoadBalancer(t *testing.T) {
	tests := []struct {
		lb    string
		valid bool
	}{
		{"round_robin", true},
		{"random", true},
		{"least_conn", true},
		{"invalid", false},
		{"", false},
	}

	for _, tt := range tests {
		t.Run(tt.lb, func(t *testing.T) {
			result := isValidLoadBalancer(tt.lb)
			if result != tt.valid {
				t.Errorf("isValidLoadBalancer(%s) = %v, want %v", tt.lb, result, tt.valid)
			}
		})
	}
}

func TestIsValidProtocol(t *testing.T) {
	tests := []struct {
		protocol string
		valid    bool
	}{
		{"tcp", true},
		{"udp", true},
		{"TCP", true},
		{"UDP", true},
		{"http", false},
		{"invalid", false},
		{"", false},
	}

	for _, tt := range tests {
		t.Run(tt.protocol, func(t *testing.T) {
			result := isValidProtocol(tt.protocol)
			if result != tt.valid {
				t.Errorf("isValidProtocol(%s) = %v, want %v", tt.protocol, result, tt.valid)
			}
		})
	}
}

func TestValidateTLSConfig(t *testing.T) {
	tests := []struct {
		name        string
		config      *TLSConfig
		expectError bool
	}{
		{
			name: "valid tls config with cert and key",
			config: &TLSConfig{
				CertFile: "/path/to/cert.pem",
				KeyFile:  "/path/to/key.pem",
				CAFile:   "/path/to/ca.pem",
			},
			expectError: false,
		},
		{
			name: "valid tls config with ca only",
			config: &TLSConfig{
				CAFile: "/path/to/ca.pem",
			},
			expectError: false,
		},
		{
			name: "invalid tls config - cert without key",
			config: &TLSConfig{
				CertFile: "/path/to/cert.pem",
			},
			expectError: true,
		},
		{
			name: "invalid tls config - key without cert",
			config: &TLSConfig{
				KeyFile: "/path/to/key.pem",
			},
			expectError: true,
		},
		{
			name:        "invalid tls config - empty",
			config:      &TLSConfig{},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateTLSConfig(tt.config)
			if tt.expectError && err == nil {
				t.Errorf("expected error but got none")
			}
			if !tt.expectError && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}