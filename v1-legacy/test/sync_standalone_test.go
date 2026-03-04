package test

import (
	"errors"
	"testing"
	"time"

	"github.com/outpostos/OutpostNet/pkg/mesh"
	"github.com/outpostos/OutpostNet/pkg/types"
)

var ErrServiceNotFound = errors.New("service not found")

// 复用discovery_test.go中的MockServiceRegistry
type MockServiceRegistry struct {
	services  map[string]*types.Service
	endpoints map[string][]*types.Endpoint
}

func NewMockServiceRegistry() *MockServiceRegistry {
	return &MockServiceRegistry{
		services:  make(map[string]*types.Service),
		endpoints: make(map[string][]*types.Endpoint),
	}
}

func (m *MockServiceRegistry) RegisterService(service *types.Service) error {
	key := service.Metadata.Namespace + "/" + service.Metadata.Name
	m.services[key] = service
	return nil
}

func (m *MockServiceRegistry) UnregisterService(name, namespace string) error {
	key := namespace + "/" + name
	delete(m.services, key)
	return nil
}

func (m *MockServiceRegistry) GetService(name, namespace string) (*types.Service, error) {
	key := namespace + "/" + name
	if service, exists := m.services[key]; exists {
		return service, nil
	}
	return nil, ErrServiceNotFound
}

func (m *MockServiceRegistry) ListServices(namespace string) ([]*types.Service, error) {
	var result []*types.Service
	for _, service := range m.services {
		if namespace == "" || service.Metadata.Namespace == namespace {
			result = append(result, service)
		}
	}
	return result, nil
}

func (m *MockServiceRegistry) RegisterWorkload(workload *types.Workload) error {
	return nil
}

func (m *MockServiceRegistry) UnregisterWorkload(workloadID string) error {
	return nil
}

func (m *MockServiceRegistry) GetWorkload(workloadID string) (*types.Workload, error) {
	return nil, ErrServiceNotFound
}

func (m *MockServiceRegistry) ListWorkloads(selector map[string]string) ([]*types.Workload, error) {
	return []*types.Workload{}, nil
}

func (m *MockServiceRegistry) RegisterEndpoint(serviceName, namespace string, endpoint *types.Endpoint) error {
	key := namespace + "/" + serviceName
	if _, exists := m.endpoints[key]; !exists {
		m.endpoints[key] = make([]*types.Endpoint, 0)
	}
	
	found := false
	for i, ep := range m.endpoints[key] {
		if ep.ID == endpoint.ID {
			m.endpoints[key][i] = endpoint
			found = true
			break
		}
	}
	if !found {
		m.endpoints[key] = append(m.endpoints[key], endpoint)
	}
	
	return nil
}

func (m *MockServiceRegistry) UnregisterEndpoint(serviceName, serviceNamespace, id string) error {
	key := serviceNamespace + "/" + serviceName
	if endpoints, exists := m.endpoints[key]; exists {
		for i, ep := range endpoints {
			if ep.ID == id {
				m.endpoints[key] = append(endpoints[:i], endpoints[i+1:]...)
				break
			}
		}
	}
	return nil
}

func (m *MockServiceRegistry) GetEndpoints(serviceName, serviceNamespace string, selector map[string]string) ([]*types.Endpoint, error) {
	key := serviceNamespace + "/" + serviceName
	if endpoints, exists := m.endpoints[key]; exists {
		return endpoints, nil
	}
	return []*types.Endpoint{}, nil
}

func (m *MockServiceRegistry) ListEndpoints(namespace string, selector map[string]string) ([]*types.Endpoint, error) {
	var result []*types.Endpoint
	for _, endpoints := range m.endpoints {
		for _, endpoint := range endpoints {
			result = append(result, endpoint)
		}
	}
	return result, nil
}

func (m *MockServiceRegistry) SyncEndpoints() error {
	return nil
}

func TestDefaultSyncConfig(t *testing.T) {
	config := mesh.DefaultSyncConfig()
	
	// 验证默认配置
	if config.SyncInterval != 30*time.Second {
		t.Errorf("Expected sync interval 30s, got %v", config.SyncInterval)
	}
	
	if config.GossipInterval != 10*time.Second {
		t.Errorf("Expected gossip interval 10s, got %v", config.GossipInterval)
	}
	
	if config.GossipFanout != 3 {
		t.Errorf("Expected gossip fanout 3, got %d", config.GossipFanout)
	}
	
	if config.MaxSyncEntries != 10000 {
		t.Errorf("Expected max sync entries 10000, got %d", config.MaxSyncEntries)
	}
	
	if config.ConflictResolution != "timestamp_wins" {
		t.Errorf("Expected conflict resolution 'timestamp_wins', got %s", config.ConflictResolution)
	}
}

func TestServiceSyncManager_Creation(t *testing.T) {
	// 创建模拟组件
	registry := NewMockServiceRegistry()
	localNode := &mesh.LocalNode{
		Node: &mesh.Node{
			ID:   "test-node",
			Addr: "127.0.0.1:7629",
		},
	}
	
	// 创建模拟集群
	cluster := &mesh.MeshCluster{}
	
	// 创建同步管理器
	config := mesh.DefaultSyncConfig()
	sm, err := mesh.NewServiceSyncManager(config, localNode, cluster, registry)
	if err != nil {
		t.Fatalf("Failed to create service sync manager: %v", err)
	}
	
	if sm == nil {
		t.Fatal("ServiceSyncManager should not be nil")
	}
}

func TestServiceSerialization(t *testing.T) {
	// 创建测试服务
	service := &types.Service{
		Metadata: types.ServiceMetadata{
			Name:      "test-service",
			Namespace: "default",
			Labels: map[string]string{
				"app": "test",
				"env": "dev",
			},
		},
		Spec: types.ServiceSpec{
			Selector: map[string]string{
				"app": "test",
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
	
	// 创建同步管理器用于测试序列化
	registry := NewMockServiceRegistry()
	localNode := &mesh.LocalNode{
		Node: &mesh.Node{ID: "test-node"},
	}
	cluster := &mesh.MeshCluster{}
	
	sm, err := mesh.NewServiceSyncManager(nil, localNode, cluster, registry)
	if err != nil {
		t.Fatalf("Failed to create sync manager: %v", err)
	}
	
	// 测试服务序列化和反序列化
	serviceData, err := sm.SerializeService(service)
	if err != nil {
		t.Fatalf("Failed to serialize service: %v", err)
	}
	
	deserializedService, err := sm.DeserializeService(serviceData)
	if err != nil {
		t.Fatalf("Failed to deserialize service: %v", err)
	}
	
	// 验证反序列化结果
	if deserializedService.Metadata.Name != service.Metadata.Name {
		t.Errorf("Service name mismatch: expected %s, got %s", 
			service.Metadata.Name, deserializedService.Metadata.Name)
	}
	
	if deserializedService.Metadata.Namespace != service.Metadata.Namespace {
		t.Errorf("Service namespace mismatch: expected %s, got %s", 
			service.Metadata.Namespace, deserializedService.Metadata.Namespace)
	}
}

func TestEndpointSerialization(t *testing.T) {
	// 创建测试端点
	endpoint := &types.Endpoint{
		ID:       "endpoint-1",
		IP:       "192.168.1.100",
		Port:     8080,
		Protocol: "tcp",
		Labels: map[string]string{
			"version": "v1",
			"zone":    "east",
		},
		Status: types.EndpointStatus{
			Ready:   true,
			Reason:  "available",
			Message: "endpoint is healthy",
		},
	}
	
	// 创建同步管理器用于测试序列化
	registry := NewMockServiceRegistry()
	localNode := &mesh.LocalNode{
		Node: &mesh.Node{ID: "test-node"},
	}
	cluster := &mesh.MeshCluster{}
	
	sm, err := mesh.NewServiceSyncManager(nil, localNode, cluster, registry)
	if err != nil {
		t.Fatalf("Failed to create sync manager: %v", err)
	}
	
	// 测试端点序列化和反序列化
	endpointData, err := sm.SerializeEndpoint(endpoint)
	if err != nil {
		t.Fatalf("Failed to serialize endpoint: %v", err)
	}
	
	deserializedEndpoint, err := sm.DeserializeEndpoint(endpointData)
	if err != nil {
		t.Fatalf("Failed to deserialize endpoint: %v", err)
	}
	
	// 验证反序列化结果
	if deserializedEndpoint.ID != endpoint.ID {
		t.Errorf("Endpoint ID mismatch: expected %s, got %s", 
			endpoint.ID, deserializedEndpoint.ID)
	}
	
	if deserializedEndpoint.IP != endpoint.IP {
		t.Errorf("Endpoint IP mismatch: expected %s, got %s", 
			endpoint.IP, deserializedEndpoint.IP)
	}
}