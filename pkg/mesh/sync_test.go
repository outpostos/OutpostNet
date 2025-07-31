package mesh

import (
	"testing"
	"time"

	"github.com/outpostos/OutpostNet/pkg/types"
)

// MockServiceRegistry 用于测试的模拟服务注册表
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
	return nil, types.ErrServiceNotFound
}

func (m *MockServiceRegistry) ListServices(namespace string, selector map[string]string) ([]*types.Service, error) {
	var result []*types.Service
	for _, service := range m.services {
		if namespace == "" || service.Metadata.Namespace == namespace {
			result = append(result, service)
		}
	}
	return result, nil
}

func (m *MockServiceRegistry) RegisterEndpoint(serviceName, namespace string, endpoint *types.Endpoint) error {
	key := namespace + "/" + serviceName
	if _, exists := m.endpoints[key]; !exists {
		m.endpoints[key] = make([]*types.Endpoint, 0)
	}
	
	// 替换或添加端点
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
			if namespace == "" || endpoint.ServiceNamespace == namespace {
				result = append(result, endpoint)
			}
		}
	}
	return result, nil
}

func TestDefaultSyncConfig(t *testing.T) {
	config := DefaultSyncConfig()
	
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
	localNode := &LocalNode{
		Node: &Node{
			ID:   "test-node",
			Addr: "127.0.0.1:7629",
		},
	}
	
	// 创建模拟集群
	cluster := &MeshCluster{
		localNode: localNode,
		nodes:     make(map[string]*Node),
	}
	
	// 创建同步管理器
	config := DefaultSyncConfig()
	sm, err := NewServiceSyncManager(config, localNode, cluster, registry)
	if err != nil {
		t.Fatalf("Failed to create service sync manager: %v", err)
	}
	
	if sm == nil {
		t.Fatal("ServiceSyncManager should not be nil")
	}
	
	if sm.config != config {
		t.Errorf("Config not set correctly")
	}
	
	if sm.localNode != localNode {
		t.Errorf("LocalNode not set correctly")
	}
	
	if sm.cluster != cluster {
		t.Errorf("Cluster not set correctly")
	}
	
	if sm.registry != registry {
		t.Errorf("Registry not set correctly")
	}
}

func TestSyncEntry_Serialization(t *testing.T) {
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
	
	// 创建同步管理器
	registry := NewMockServiceRegistry()
	localNode := &LocalNode{
		Node: &Node{ID: "test-node"},
	}
	cluster := &MeshCluster{localNode: localNode}
	
	sm, err := NewServiceSyncManager(nil, localNode, cluster, registry)
	if err != nil {
		t.Fatalf("Failed to create sync manager: %v", err)
	}
	
	// 测试服务序列化和反序列化
	serviceData, err := sm.serializeService(service)
	if err != nil {
		t.Fatalf("Failed to serialize service: %v", err)
	}
	
	deserializedService, err := sm.deserializeService(serviceData)
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
	
	if len(deserializedService.Spec.Ports) != len(service.Spec.Ports) {
		t.Errorf("Service ports count mismatch: expected %d, got %d", 
			len(service.Spec.Ports), len(deserializedService.Spec.Ports))
	}
}

func TestSyncEntry_EndpointSerialization(t *testing.T) {
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
	
	// 创建同步管理器
	registry := NewMockServiceRegistry()
	localNode := &LocalNode{
		Node: &Node{ID: "test-node"},
	}
	cluster := &MeshCluster{localNode: localNode}
	
	sm, err := NewServiceSyncManager(nil, localNode, cluster, registry)
	if err != nil {
		t.Fatalf("Failed to create sync manager: %v", err)
	}
	
	// 测试端点序列化和反序列化
	endpointData, err := sm.serializeEndpoint(endpoint)
	if err != nil {
		t.Fatalf("Failed to serialize endpoint: %v", err)
	}
	
	deserializedEndpoint, err := sm.deserializeEndpoint(endpointData)
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
	
	if deserializedEndpoint.Port != endpoint.Port {
		t.Errorf("Endpoint port mismatch: expected %d, got %d", 
			endpoint.Port, deserializedEndpoint.Port)
	}
}

func TestSyncManager_ConflictResolution(t *testing.T) {
	registry := NewMockServiceRegistry()
	localNode := &LocalNode{
		Node: &Node{ID: "test-node"},
	}
	cluster := &MeshCluster{localNode: localNode}
	
	// 测试不同的冲突解决策略
	testCases := []struct {
		name               string
		conflictResolution string
		localTime          time.Time
		remoteTime         time.Time
		localVersion       uint64
		remoteVersion      uint64
		expectedUpdate     bool
	}{
		{
			name:               "timestamp_wins_remote_newer",
			conflictResolution: "timestamp_wins",
			localTime:          time.Now().Add(-1 * time.Minute),
			remoteTime:         time.Now(),
			expectedUpdate:     true,
		},
		{
			name:               "timestamp_wins_local_newer",
			conflictResolution: "timestamp_wins", 
			localTime:          time.Now(),
			remoteTime:         time.Now().Add(-1 * time.Minute),
			expectedUpdate:     false,
		},
		{
			name:               "version_wins_remote_higher",
			conflictResolution: "version_wins",
			localVersion:       1,
			remoteVersion:      2,
			expectedUpdate:     true,
		},
		{
			name:               "version_wins_local_higher",
			conflictResolution: "version_wins",
			localVersion:       2,
			remoteVersion:      1,
			expectedUpdate:     false,
		},
	}
	
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			config := DefaultSyncConfig()
			config.ConflictResolution = tc.conflictResolution
			
			sm, err := NewServiceSyncManager(config, localNode, cluster, registry)
			if err != nil {
				t.Fatalf("Failed to create sync manager: %v", err)
			}
			
			localEntry := &SyncEntry{
				ID:        "test-entry",
				Timestamp: tc.localTime,
				Version:   tc.localVersion,
			}
			
			remoteEntry := &SyncEntry{
				ID:        "test-entry",
				Timestamp: tc.remoteTime,
				Version:   tc.remoteVersion,
			}
			
			shouldUpdate := sm.shouldUpdateEntry(localEntry, remoteEntry)
			if shouldUpdate != tc.expectedUpdate {
				t.Errorf("Expected shouldUpdate=%v, got %v", tc.expectedUpdate, shouldUpdate)
			}
		})
	}
}

func TestGossipMessage_Creation(t *testing.T) {
	registry := NewMockServiceRegistry()
	localNode := &LocalNode{
		Node: &Node{ID: "test-node"},
	}
	cluster := &MeshCluster{localNode: localNode}
	
	sm, err := NewServiceSyncManager(nil, localNode, cluster, registry)
	if err != nil {
		t.Fatalf("Failed to create sync manager: %v", err)
	}
	
	// 添加一些测试数据
	sm.syncData["test-1"] = &SyncEntry{
		ID:        "test-1",
		Type:      SyncEntryService,
		NodeID:    "test-node",
		Timestamp: time.Now(),
	}
	
	sm.syncData["test-2"] = &SyncEntry{
		ID:        "test-2",
		Type:      SyncEntryEndpoint,
		NodeID:    "test-node",
		Timestamp: time.Now(),
	}
	
	// 创建Gossip消息
	message := sm.createGossipMessage()
	
	// 验证消息
	if message.Type != GossipMessageSync {
		t.Errorf("Expected message type %s, got %s", GossipMessageSync, message.Type)
	}
	
	if message.SenderID != "test-node" {
		t.Errorf("Expected sender ID 'test-node', got %s", message.SenderID)
	}
	
	if len(message.Entries) == 0 {
		t.Errorf("Expected entries in gossip message, got none")
	}
	
	if message.MessageID == "" {
		t.Errorf("Expected non-empty message ID")
	}
}

// TestServiceSyncListener 测试用的同步监听器
type TestServiceSyncListener struct {
	serviceSynced   []*types.Service
	endpointSynced  []*types.Endpoint
	conflicts       [][]*SyncEntry
	errors          []error
}

func (l *TestServiceSyncListener) OnServiceSynced(service *types.Service) {
	l.serviceSynced = append(l.serviceSynced, service)
}

func (l *TestServiceSyncListener) OnEndpointSynced(endpoint *types.Endpoint) {
	l.endpointSynced = append(l.endpointSynced, endpoint)
}

func (l *TestServiceSyncListener) OnSyncConflict(local, remote *SyncEntry) {
	l.conflicts = append(l.conflicts, []*SyncEntry{local, remote})
}

func (l *TestServiceSyncListener) OnSyncError(err error) {
	l.errors = append(l.errors, err)
}

func TestSyncManager_Listeners(t *testing.T) {
	registry := NewMockServiceRegistry()
	localNode := &LocalNode{
		Node: &Node{ID: "test-node"},
	}
	cluster := &MeshCluster{localNode: localNode}
	
	sm, err := NewServiceSyncManager(nil, localNode, cluster, registry)
	if err != nil {
		t.Fatalf("Failed to create sync manager: %v", err)
	}
	
	// 创建测试监听器
	listener := &TestServiceSyncListener{}
	sm.AddListener(listener)
	
	// 测试服务同步通知
	service := &types.Service{
		Metadata: types.ServiceMetadata{
			Name:      "test-service",
			Namespace: "default",
		},
	}
	
	sm.notifyServiceSynced(service)
	
	// 给异步通知一些时间
	time.Sleep(10 * time.Millisecond)
	
	if len(listener.serviceSynced) != 1 {
		t.Errorf("Expected 1 service sync notification, got %d", len(listener.serviceSynced))
	}
	
	if listener.serviceSynced[0].Metadata.Name != "test-service" {
		t.Errorf("Expected service name 'test-service', got %s", listener.serviceSynced[0].Metadata.Name)
	}
}

func TestSyncStats(t *testing.T) {
	registry := NewMockServiceRegistry()
	localNode := &LocalNode{
		Node: &Node{ID: "test-node"},
	}
	cluster := &MeshCluster{localNode: localNode}
	
	sm, err := NewServiceSyncManager(nil, localNode, cluster, registry)
	if err != nil {
		t.Fatalf("Failed to create sync manager: %v", err)
	}
	
	// 添加测试条目
	sm.syncData["service-1"] = &SyncEntry{
		Type: SyncEntryService,
	}
	sm.syncData["service-2"] = &SyncEntry{
		Type: SyncEntryService,
	}
	sm.syncData["endpoint-1"] = &SyncEntry{
		Type: SyncEntryEndpoint,
	}
	sm.syncData["node-1"] = &SyncEntry{
		Type: SyncEntryNode,
	}
	
	// 获取统计信息
	stats := sm.GetSyncStats()
	
	if stats.TotalEntries != 4 {
		t.Errorf("Expected 4 total entries, got %d", stats.TotalEntries)
	}
	
	if stats.ServiceEntries != 2 {
		t.Errorf("Expected 2 service entries, got %d", stats.ServiceEntries)
	}
	
	if stats.EndpointEntries != 1 {
		t.Errorf("Expected 1 endpoint entry, got %d", stats.EndpointEntries)
	}
	
	if stats.NodeEntries != 1 {
		t.Errorf("Expected 1 node entry, got %d", stats.NodeEntries)
	}
}