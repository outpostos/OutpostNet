// 配置模板生成器
package generator

import (
	"github.com/outpostos/OutpostNet/pkg/config"
)

// createExpressTemplate 创建Express模式模板
func (g *ConfigGenerator) createExpressTemplate() (*config.Deployment, error) {
	return &config.Deployment{
		Name:        "outpostnet-express",
		Version:     "1.0.0",
		Environment: "dev",
		
		Cluster: config.ClusterConfig{
			Type: "standalone",
			Nodes: []config.NodeConfig{
				{
					NodeID: "node-1",
					Name:   "primary-node",
					Role:   "primary",
					Network: config.NetworkConfig{
						ListenAddr:   "0.0.0.0",
						ExternalAddr: "127.0.0.1",
						Ports: config.PortConfig{
							Control:    7000,
							Metrics:    9000,
							Web:        10000,
						},
					},
					Location: config.LocationConfig{
						Region: "default",
						Zone:   "default",
						Site:   "default",
					},
				},
			},
		},
		
		Services: []config.ServiceConfig{
			{
				Name:      "web-service",
				Namespace: "default",
				Type:      "NodePort",
				Selector: map[string]string{
					"app": "web",
				},
				Ports: []config.ServicePort{
					{
						Name:       "http",
						Port:       80,
						TargetPort: 8080,
						NodePort:   30080,
						Protocol:   "tcp",
					},
				},
				ProxyMode:    "tcp",
				LoadBalancer: "round_robin",
				Backends: []config.BackendConfig{
					{
						Name:    "web-backend-1",
						Address: "127.0.0.1",
						Port:    8080,
						Weight:  1,
						Labels: map[string]string{
							"app":     "web",
							"version": "v1",
						},
					},
				},
			},
		},
		
		Topology: config.TopologyConfig{
			Routing: config.RoutingConfig{
				Strategy: "local-first",
				Fallback: "none",
			},
		},
		
		Security: config.SecurityConfig{
			Auth: config.AuthConfig{
				Method: "token",
				Token:  "express-demo-token",
			},
			TLS: config.GlobalTLSConfig{
				Enabled: false,
			},
		},
		
		Monitoring: config.MonitoringConfig{
			Enabled: true,
			Type:    "prometheus",
		},
	}, nil
}

// createRegionMeshTemplate 创建区域网格模板
func (g *ConfigGenerator) createRegionMeshTemplate() (*config.Deployment, error) {
	return &config.Deployment{
		Name:        "outpostnet-region-mesh",
		Version:     "2.0.0",
		Environment: "production",
		
		Cluster: config.ClusterConfig{
			Type: "region-mesh",
			Nodes: []config.NodeConfig{
				{
					NodeID: "node-primary",
					Name:   "primary-datacenter",
					Role:   "primary",
					Network: config.NetworkConfig{
						ListenAddr:   "0.0.0.0",
						ExternalAddr: "10.0.1.100",
						Ports: config.PortConfig{
							Control:    7000,
							RegionMesh: 8000,
							Metrics:    9000,
							Web:        10000,
						},
					},
					Location: config.LocationConfig{
						Region: "region-east",
						Zone:   "zone-1",
						Site:   "datacenter-1",
					},
					Resources: config.ResourceConfig{
						CPU:    "2",
						Memory: "4Gi",
					},
				},
				{
					NodeID: "node-edge-1",
					Name:   "edge-node-1",
					Role:   "secondary",
					Network: config.NetworkConfig{
						ListenAddr:   "0.0.0.0",
						ExternalAddr: "10.0.2.100",
						Ports: config.PortConfig{
							Control:    7001,
							RegionMesh: 8001,
							Metrics:    9001,
							Web:        10001,
						},
					},
					Location: config.LocationConfig{
						Region: "region-east",
						Zone:   "zone-2",
						Site:   "edge-site-1",
					},
					Resources: config.ResourceConfig{
						CPU:    "1",
						Memory: "2Gi",
					},
				},
				{
					NodeID: "node-edge-2",
					Name:   "edge-node-2",
					Role:   "edge",
					Network: config.NetworkConfig{
						ListenAddr:   "0.0.0.0",
						ExternalAddr: "10.0.3.100",
						Ports: config.PortConfig{
							Control:    7002,
							RegionMesh: 8002,
							Metrics:    9002,
							Web:        10002,
						},
					},
					Location: config.LocationConfig{
						Region: "region-east",
						Zone:   "zone-3",
						Site:   "edge-site-2",
					},
					Resources: config.ResourceConfig{
						CPU:    "1",
						Memory: "2Gi",
					},
				},
			},
			
			Region: config.RegionConfig{
				Enabled:  true,
				RegionID: "region-east",
				
				ServiceSync: config.ServiceSyncConfig{
					Interval: "10s",
					Timeout:  "60s",
				},
				
				LoadBalancer: config.LoadBalancerConfig{
					Strategy: "geographic_aware",
					Enabled:  true,
				},
				
				HealthCheck: config.HealthCheckConfig{
					Enabled:      true,
					Interval:     "30s",
					Timeout:      "10s",
					MaxFailures:  3,
					PhiThreshold: 8.0,
				},
			},
		},
		
		Services: []config.ServiceConfig{
			{
				Name:      "web-service",
				Namespace: "default",
				Type:      "NodePort",
				Selector: map[string]string{
					"app": "web",
				},
				Ports: []config.ServicePort{
					{
						Name:       "http",
						Port:       80,
						TargetPort: 8080,
						NodePort:   30080,
						Protocol:   "tcp",
					},
				},
				ProxyMode:    "tcp",
				LoadBalancer: "geographic_aware",
				Backends: []config.BackendConfig{
					{
						Name:    "web-datacenter",
						Address: "10.0.1.10",
						Port:    8080,
						Weight:  3,
						Labels: map[string]string{
							"app":  "web",
							"tier": "frontend",
						},
						Location: config.LocationConfig{
							Region: "region-east",
							Zone:   "zone-1",
							Site:   "datacenter-1",
						},
					},
					{
						Name:    "web-edge-1",
						Address: "10.0.2.10",
						Port:    8080,
						Weight:  2,
						Labels: map[string]string{
							"app":  "web",
							"tier": "frontend",
						},
						Location: config.LocationConfig{
							Region: "region-east",
							Zone:   "zone-2",
							Site:   "edge-site-1",
						},
					},
					{
						Name:    "web-edge-2",
						Address: "10.0.3.10",
						Port:    8080,
						Weight:  1,
						Labels: map[string]string{
							"app":  "web",
							"tier": "frontend",
						},
						Location: config.LocationConfig{
							Region: "region-east",
							Zone:   "zone-3",
							Site:   "edge-site-2",
						},
					},
				},
			},
			{
				Name:      "api-service",
				Namespace: "default",
				Type:      "ClusterIP",
				Selector: map[string]string{
					"app": "api",
				},
				Ports: []config.ServicePort{
					{
						Name:       "api",
						Port:       8080,
						TargetPort: 8080,
						Protocol:   "stcp",
					},
				},
				ProxyMode:    "stcp",
				LoadBalancer: "health_aware",
				Backends: []config.BackendConfig{
					{
						Name:    "api-datacenter",
						Address: "10.0.1.20",
						Port:    8080,
						Weight:  3,
						Labels: map[string]string{
							"app":  "api",
							"tier": "backend",
						},
						Location: config.LocationConfig{
							Region: "region-east",
							Zone:   "zone-1",
							Site:   "datacenter-1",
						},
					},
					{
						Name:    "api-edge",
						Address: "10.0.2.20",
						Port:    8080,
						Weight:  2,
						Labels: map[string]string{
							"app":  "api",
							"tier": "backend",
						},
						Location: config.LocationConfig{
							Region: "region-east",
							Zone:   "zone-2",
							Site:   "edge-site-1",
						},
					},
				},
			},
			{
				Name:      "db-service",
				Namespace: "default",
				Type:      "ClusterIP",
				Selector: map[string]string{
					"app": "database",
				},
				Ports: []config.ServicePort{
					{
						Name:       "mysql",
						Port:       3306,
						TargetPort: 3306,
						Protocol:   "stcp",
					},
				},
				ProxyMode:    "stcp",
				LoadBalancer: "latency_aware",
				Backends: []config.BackendConfig{
					{
						Name:    "db-primary",
						Address: "10.0.1.30",
						Port:    3306,
						Weight:  5,
						Labels: map[string]string{
							"app":  "database",
							"role": "primary",
						},
						Location: config.LocationConfig{
							Region: "region-east",
							Zone:   "zone-1",
							Site:   "datacenter-1",
						},
					},
					{
						Name:    "db-replica",
						Address: "10.0.2.30",
						Port:    3306,
						Weight:  3,
						Labels: map[string]string{
							"app":  "database",
							"role": "replica",
						},
						Location: config.LocationConfig{
							Region: "region-east",
							Zone:   "zone-2",
							Site:   "edge-site-1",
						},
					},
				},
			},
		},
		
		Topology: config.TopologyConfig{
			Connections: []config.ConnectionConfig{
				{
					From: "node-primary",
					To:   "node-edge-1",
					Type: "direct",
				},
				{
					From: "node-primary",
					To:   "node-edge-2",
					Type: "direct",
				},
				{
					From: "node-edge-1",
					To:   "node-edge-2",
					Type: "tunnel",
				},
			},
			Routing: config.RoutingConfig{
				Strategy: "geographic",
				Fallback: "regional",
			},
		},
		
		Security: config.SecurityConfig{
			Auth: config.AuthConfig{
				Method: "token",
				Token:  "region-mesh-production-token",
			},
			TLS: config.GlobalTLSConfig{
				Enabled: true,
				CA: config.CAConfig{
					CertFile: "/etc/ssl/certs/ca.crt",
					KeyFile:  "/etc/ssl/private/ca.key",
				},
				Certs: []config.CertConfig{
					{
						Name:     "server",
						CertFile: "/etc/ssl/certs/server.crt",
						KeyFile:  "/etc/ssl/private/server.key",
					},
					{
						Name:     "client",
						CertFile: "/etc/ssl/certs/client.crt",
						KeyFile:  "/etc/ssl/private/client.key",
					},
				},
			},
		},
		
		Monitoring: config.MonitoringConfig{
			Enabled: true,
			Type:    "prometheus",
			Config: map[string]string{
				"scrape_interval": "15s",
				"metrics_path":    "/metrics",
			},
		},
	}, nil
}

// createProductionTemplate 创建生产环境模板
func (g *ConfigGenerator) createProductionTemplate() (*config.Deployment, error) {
	return &config.Deployment{
		Name:        "outpostnet-production",
		Version:     "2.1.0",
		Environment: "production",
		
		Cluster: config.ClusterConfig{
			Type: "multi-cluster",
			Nodes: []config.NodeConfig{
				{
					NodeID: "node-dc-1",
					Name:   "datacenter-primary",
					Role:   "primary",
					Network: config.NetworkConfig{
						ListenAddr:   "0.0.0.0",
						ExternalAddr: "10.0.1.100",
						Ports: config.PortConfig{
							Control:    7000,
							RegionMesh: 8000,
							Metrics:    9000,
							Web:        10000,
						},
						TLS: config.NodeTLSConfig{
							Enabled:  true,
							CertFile: "/etc/ssl/certs/node-dc-1.crt",
							KeyFile:  "/etc/ssl/private/node-dc-1.key",
							CAFile:   "/etc/ssl/certs/ca.crt",
						},
					},
					Location: config.LocationConfig{
						Region: "region-central",
						Zone:   "zone-dc-1",
						Site:   "datacenter-primary",
					},
					Resources: config.ResourceConfig{
						CPU:    "4",
						Memory: "8Gi",
						Disk:   "100Gi",
					},
				},
				{
					NodeID: "node-dc-2",
					Name:   "datacenter-secondary",
					Role:   "secondary",
					Network: config.NetworkConfig{
						ListenAddr:   "0.0.0.0",
						ExternalAddr: "10.0.2.100",
						Ports: config.PortConfig{
							Control:    7001,
							RegionMesh: 8001,
							Metrics:    9001,
							Web:        10001,
						},
						TLS: config.NodeTLSConfig{
							Enabled:  true,
							CertFile: "/etc/ssl/certs/node-dc-2.crt",
							KeyFile:  "/etc/ssl/private/node-dc-2.key",
							CAFile:   "/etc/ssl/certs/ca.crt",
						},
					},
					Location: config.LocationConfig{
						Region: "region-central",
						Zone:   "zone-dc-2",
						Site:   "datacenter-secondary",
					},
					Resources: config.ResourceConfig{
						CPU:    "4",
						Memory: "8Gi",
						Disk:   "100Gi",
					},
				},
				{
					NodeID: "node-edge-east",
					Name:   "edge-east",
					Role:   "edge",
					Network: config.NetworkConfig{
						ListenAddr:   "0.0.0.0",
						ExternalAddr: "10.10.1.100",
						Ports: config.PortConfig{
							Control:    7002,
							RegionMesh: 8002,
							Metrics:    9002,
							Web:        10002,
						},
						TLS: config.NodeTLSConfig{
							Enabled:  true,
							CertFile: "/etc/ssl/certs/node-edge-east.crt",
							KeyFile:  "/etc/ssl/private/node-edge-east.key",
							CAFile:   "/etc/ssl/certs/ca.crt",
						},
					},
					Location: config.LocationConfig{
						Region: "region-east",
						Zone:   "zone-edge-1",
						Site:   "edge-east",
					},
					Resources: config.ResourceConfig{
						CPU:    "2",
						Memory: "4Gi",
						Disk:   "50Gi",
					},
				},
				{
					NodeID: "node-edge-west",
					Name:   "edge-west",
					Role:   "edge",
					Network: config.NetworkConfig{
						ListenAddr:   "0.0.0.0",
						ExternalAddr: "10.20.1.100",
						Ports: config.PortConfig{
							Control:    7003,
							RegionMesh: 8003,
							Metrics:    9003,
							Web:        10003,
						},
						TLS: config.NodeTLSConfig{
							Enabled:  true,
							CertFile: "/etc/ssl/certs/node-edge-west.crt",
							KeyFile:  "/etc/ssl/private/node-edge-west.key",
							CAFile:   "/etc/ssl/certs/ca.crt",
						},
					},
					Location: config.LocationConfig{
						Region: "region-west",
						Zone:   "zone-edge-2",
						Site:   "edge-west",
					},
					Resources: config.ResourceConfig{
						CPU:    "2",
						Memory: "4Gi",
						Disk:   "50Gi",
					},
				},
			},
			
			Region: config.RegionConfig{
				Enabled:  true,
				RegionID: "production-mesh",
				
				ServiceSync: config.ServiceSyncConfig{
					Interval: "30s",
					Timeout:  "120s",
				},
				
				LoadBalancer: config.LoadBalancerConfig{
					Strategy: "geographic_aware",
					Enabled:  true,
				},
				
				HealthCheck: config.HealthCheckConfig{
					Enabled:      true,
					Interval:     "20s",
					Timeout:      "5s",
					MaxFailures:  5,
					PhiThreshold: 10.0,
				},
			},
		},
		
		Services: []config.ServiceConfig{
			{
				Name:      "frontend-service",
				Namespace: "frontend",
				Type:      "NodePort",
				Selector: map[string]string{
					"app":  "frontend",
					"tier": "web",
				},
				Ports: []config.ServicePort{
					{
						Name:       "http",
						Port:       80,
						TargetPort: 8080,
						NodePort:   30080,
						Protocol:   "tcp",
					},
					{
						Name:       "https",
						Port:       443,
						TargetPort: 8443,
						NodePort:   30443,
						Protocol:   "tcp",
					},
				},
				ProxyMode:    "tcp",
				LoadBalancer: "geographic_aware",
			},
			{
				Name:      "backend-service",
				Namespace: "backend",
				Type:      "ClusterIP",
				Selector: map[string]string{
					"app":  "backend",
					"tier": "api",
				},
				Ports: []config.ServicePort{
					{
						Name:       "api",
						Port:       8080,
						TargetPort: 8080,
						Protocol:   "stcp",
					},
				},
				ProxyMode:    "stcp",
				LoadBalancer: "health_aware",
			},
			{
				Name:      "database-service",
				Namespace: "database",
				Type:      "ClusterIP",
				Selector: map[string]string{
					"app":  "database",
					"tier": "data",
				},
				Ports: []config.ServicePort{
					{
						Name:       "mysql",
						Port:       3306,
						TargetPort: 3306,
						Protocol:   "stcp",
					},
				},
				ProxyMode:    "stcp",
				LoadBalancer: "latency_aware",
			},
			{
				Name:      "cache-service",
				Namespace: "cache",
				Type:      "ClusterIP",
				Selector: map[string]string{
					"app":  "redis",
					"tier": "cache",
				},
				Ports: []config.ServicePort{
					{
						Name:       "redis",
						Port:       6379,
						TargetPort: 6379,
						Protocol:   "stcp",
					},
				},
				ProxyMode:    "stcp",
				LoadBalancer: "consistent_hash",
			},
			{
				Name:      "monitoring-service",
				Namespace: "monitoring",
				Type:      "NodePort",
				Selector: map[string]string{
					"app":  "prometheus",
					"tier": "monitoring",
				},
				Ports: []config.ServicePort{
					{
						Name:       "prometheus",
						Port:       9090,
						TargetPort: 9090,
						NodePort:   30090,
						Protocol:   "tcp",
					},
				},
				ProxyMode:    "tcp",
				LoadBalancer: "round_robin",
			},
		},
		
		Topology: config.TopologyConfig{
			Connections: []config.ConnectionConfig{
				{
					From: "node-dc-1",
					To:   "node-dc-2",
					Type: "direct",
				},
				{
					From: "node-dc-1",
					To:   "node-edge-east",
					Type: "tunnel",
				},
				{
					From: "node-dc-1",
					To:   "node-edge-west",
					Type: "tunnel",
				},
				{
					From: "node-dc-2",
					To:   "node-edge-east",
					Type: "tunnel",
				},
				{
					From: "node-dc-2",
					To:   "node-edge-west",
					Type: "tunnel",
				},
				{
					From: "node-edge-east",
					To:   "node-edge-west",
					Type: "gateway",
				},
			},
			Routing: config.RoutingConfig{
				Strategy: "latency-based",
				Fallback: "cloud",
			},
		},
		
		Security: config.SecurityConfig{
			Auth: config.AuthConfig{
				Method: "certificate",
			},
			TLS: config.GlobalTLSConfig{
				Enabled: true,
				CA: config.CAConfig{
					CertFile: "/etc/ssl/certs/production-ca.crt",
					KeyFile:  "/etc/ssl/private/production-ca.key",
				},
				Certs: []config.CertConfig{
					{
						Name:     "server",
						CertFile: "/etc/ssl/certs/server.crt",
						KeyFile:  "/etc/ssl/private/server.key",
					},
					{
						Name:     "client",
						CertFile: "/etc/ssl/certs/client.crt",
						KeyFile:  "/etc/ssl/private/client.key",
					},
					{
						Name:     "edge",
						CertFile: "/etc/ssl/certs/edge.crt",
						KeyFile:  "/etc/ssl/private/edge.key",
					},
				},
			},
		},
		
		Monitoring: config.MonitoringConfig{
			Enabled: true,
			Type:    "prometheus",
			Config: map[string]string{
				"scrape_interval":   "10s",
				"metrics_path":      "/metrics",
				"retention_time":    "30d",
				"external_labels":   "environment=production",
				"alert_manager_url": "http://alertmanager:9093",
			},
		},
	}, nil
}