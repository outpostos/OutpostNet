package types

import "time"

// Phase 2 Region Configuration Types

// RegionConfig 区域配置 (Phase 2 新增)
type RegionConfig struct {
	Enabled     bool               `yaml:"enabled" json:"enabled"`                       // 是否启用区域模式
	RegionID    string             `yaml:"region_id" json:"region_id"`                   // 区域标识
	NodeID      string             `yaml:"node_id" json:"node_id"`                       // 节点标识
	Neighbors   []NeighborNode     `yaml:"neighbors" json:"neighbors"`                   // 邻居节点列表
	ServiceSync ServiceSyncConfig  `yaml:"service_sync" json:"service_sync"`             // 服务同步配置
}

// NeighborNode 邻居节点信息
type NeighborNode struct {
	NodeID      string `yaml:"node_id" json:"node_id"`                     // 邻居节点ID
	Address     string `yaml:"address" json:"address"`                     // 邻居server地址
	Weight      int    `yaml:"weight,omitempty" json:"weight,omitempty"`   // 路由权重
	Zone        string `yaml:"zone,omitempty" json:"zone,omitempty"`       // 可用区
	LastSeen    time.Time `yaml:"last_seen,omitempty" json:"last_seen,omitempty"` // 最后可见时间
	Status      string `yaml:"status,omitempty" json:"status,omitempty"`   // 节点状态: online/offline/unknown
}

// ServiceSyncConfig 服务同步配置
type ServiceSyncConfig struct {
	SyncInterval    time.Duration `yaml:"sync_interval" json:"sync_interval"`       // 同步间隔
	RetryCount      int          `yaml:"retry_count" json:"retry_count"`           // 重试次数
	HealthCheck     bool         `yaml:"health_check" json:"health_check"`         // 是否启用健康检查
	CacheTimeout    time.Duration `yaml:"cache_timeout" json:"cache_timeout"`      // 缓存超时
}

// RemoteServiceInfo 远程服务信息 (缓存使用)
type RemoteServiceInfo struct {
	Service     *Service    `yaml:"service" json:"service"`           // 服务定义
	Endpoints   []*Endpoint `yaml:"endpoints" json:"endpoints"`       // 端点列表
	SourceNode  string      `yaml:"source_node" json:"source_node"`   // 来源节点
	CachedAt    time.Time   `yaml:"cached_at" json:"cached_at"`       // 缓存时间
	TTL         time.Duration `yaml:"ttl" json:"ttl"`                 // 生存时间
}

// Service 定义服务的逻辑抽象，类似 K8s Service
type Service struct {
	Metadata ServiceMetadata `yaml:"metadata" json:"metadata"`
	Spec     ServiceSpec     `yaml:"spec" json:"spec"`
}

// ServiceMetadata 服务元数据
type ServiceMetadata struct {
	Name      string            `yaml:"name" json:"name"`
	Namespace string            `yaml:"namespace" json:"namespace"`
	Labels    map[string]string `yaml:"labels" json:"labels"`
	
	// Phase 2 新增：丰富的地理和应用元数据
	Annotations map[string]string `yaml:"annotations,omitempty" json:"annotations,omitempty"`
	
	// 应用信息
	Application string `yaml:"application,omitempty" json:"application,omitempty"`
	Cluster     string `yaml:"cluster,omitempty" json:"cluster,omitempty"`
	Version     string `yaml:"version,omitempty" json:"version,omitempty"`
	
	// 地理位置信息
	Site   string `yaml:"site,omitempty" json:"site,omitempty"`     // 站点标识
	Zone   string `yaml:"zone,omitempty" json:"zone,omitempty"`     // 可用区
	Region string `yaml:"region,omitempty" json:"region,omitempty"` // 区域
	
	// TTL 和清理相关元数据
	CreatedAt time.Time     `yaml:"created_at,omitempty" json:"created_at,omitempty"`
	TTL       time.Duration `yaml:"ttl,omitempty" json:"ttl,omitempty"`           // 生存时间
	LastSeen  time.Time     `yaml:"last_seen,omitempty" json:"last_seen,omitempty"` // 最后可见时间
}

// ServiceSpec 服务规格定义
type ServiceSpec struct {
	Selector     map[string]string  `yaml:"selector" json:"selector"`
	Ports        []ServicePort      `yaml:"ports" json:"ports"`
	ProxyMode    string             `yaml:"proxy_mode" json:"proxy_mode"` // tcp/udp/stcp/sudp
	LoadBalancer string             `yaml:"load_balancer" json:"load_balancer"`
	Constraints  *ServiceConstraints `yaml:"constraints,omitempty" json:"constraints,omitempty"`
}

// ServiceConstraints 服务约束条件定义 (简化版本)
type ServiceConstraints struct {
	// 端点健康状态约束 - 最基础的约束
	HealthyOnly bool `yaml:"healthy_only" json:"healthy_only"`
	
	// 标签表达式约束：支持K8s风格的标签选择器
	LabelSelector *LabelSelector `yaml:"label_selector,omitempty" json:"label_selector,omitempty"`
	
	// TODO: 后期可添加的高级功能
	// - RequiredLabels map[string]string     // 简单标签匹配（可用LabelSelector.MatchLabels替代）
	// - MinEndpoints int                     // 最小端点数约束
	// - MaxEndpoints int                     // 最大端点数约束  
	// - AllowedStatuses []string             // 端点状态约束
	// - ExcludeEndpoints []string            // 排除特定端点
	// - PreferredEndpoints []string          // 首选端点
}

// LabelSelector 标签选择器，支持复杂的标签匹配逻辑
type LabelSelector struct {
	// 等值匹配：key=value
	MatchLabels map[string]string `yaml:"match_labels,omitempty" json:"match_labels,omitempty"`
	
	// 表达式匹配：支持In、NotIn、Exists、DoesNotExist操作
	MatchExpressions []LabelSelectorRequirement `yaml:"match_expressions,omitempty" json:"match_expressions,omitempty"`
}

// LabelSelectorRequirement 标签选择器表达式
type LabelSelectorRequirement struct {
	Key      string                      `yaml:"key" json:"key"`
	Operator LabelSelectorOperator       `yaml:"operator" json:"operator"`
	Values   []string                    `yaml:"values,omitempty" json:"values,omitempty"`
}

// LabelSelectorOperator 标签选择器操作符
type LabelSelectorOperator string

const (
	LabelSelectorOpIn           LabelSelectorOperator = "In"           // key的值在给定列表中
	LabelSelectorOpNotIn        LabelSelectorOperator = "NotIn"        // key的值不在给定列表中
	LabelSelectorOpExists       LabelSelectorOperator = "Exists"       // key存在（忽略值）
	LabelSelectorOpDoesNotExist LabelSelectorOperator = "DoesNotExist"  // key不存在
)

// ServicePort 服务端口定义
type ServicePort struct {
	Name       string `yaml:"name" json:"name"`
	Port       int    `yaml:"port" json:"port"`
	TargetPort int    `yaml:"targetPort" json:"targetPort"`
	Protocol   string `yaml:"protocol" json:"protocol"`
}

// Endpoint 定义具体的后端实例，类似 K8s Endpoint
type Endpoint struct {
	ID       string            `yaml:"id" json:"id"`
	IP       string            `yaml:"ip" json:"ip"`
	Port     int               `yaml:"port" json:"port"`
	Protocol string            `yaml:"protocol" json:"protocol"`
	Labels   map[string]string `yaml:"labels" json:"labels"`
	Status   EndpointStatus    `yaml:"status" json:"status"`
	LastSeen time.Time         `yaml:"last_seen" json:"last_seen"`
	
	// Phase 2 新增：丰富的端点元数据
	Metadata EndpointMetadata `yaml:"metadata,omitempty" json:"metadata,omitempty"`
}

// EndpointMetadata 端点元数据 (Phase 2 新增)
type EndpointMetadata struct {
	// 应用信息
	Application string `yaml:"application,omitempty" json:"application,omitempty"`
	Version     string `yaml:"version,omitempty" json:"version,omitempty"`
	
	// 地理位置信息
	NodeID  string `yaml:"node_id,omitempty" json:"node_id,omitempty"`       // 所属节点
	Site    string `yaml:"site,omitempty" json:"site,omitempty"`             // 站点标识
	Zone    string `yaml:"zone,omitempty" json:"zone,omitempty"`             // 可用区
	Region  string `yaml:"region,omitempty" json:"region,omitempty"`         // 区域
	
	// 网络信息
	PublicIP    string `yaml:"public_ip,omitempty" json:"public_ip,omitempty"`       // 公网IP
	InternalIP  string `yaml:"internal_ip,omitempty" json:"internal_ip,omitempty"`   // 内网IP
	
	// 性能信息
	Weight      int    `yaml:"weight,omitempty" json:"weight,omitempty"`             // 负载均衡权重
	Priority    int    `yaml:"priority,omitempty" json:"priority,omitempty"`         // 优先级
	MaxConn     int    `yaml:"max_conn,omitempty" json:"max_conn,omitempty"`         // 最大连接数
	
	// 运行时信息
	Uptime      time.Duration `yaml:"uptime,omitempty" json:"uptime,omitempty"`       // 运行时间
	CPU         float64       `yaml:"cpu,omitempty" json:"cpu,omitempty"`             // CPU使用率
	Memory      float64       `yaml:"memory,omitempty" json:"memory,omitempty"`       // 内存使用率
	
	// 源信息 (用于跨节点服务发现)
	SourceNode  string        `yaml:"source_node,omitempty" json:"source_node,omitempty"` // 注册源节点
	IsRemote    bool          `yaml:"is_remote,omitempty" json:"is_remote,omitempty"`     // 是否为远程端点
}

// EndpointStatus 端点状态
type EndpointStatus struct {
	Ready   bool   `yaml:"ready" json:"ready"`
	Reason  string `yaml:"reason,omitempty" json:"reason,omitempty"`
	Message string `yaml:"message,omitempty" json:"message,omitempty"`
}

// Proxy 定义代理配置，客户端如何访问服务
type Proxy struct {
	Name             string            `yaml:"name" json:"name"`
	ServiceName      string            `yaml:"service_name" json:"service_name"`
	ServiceNamespace string            `yaml:"service_namespace" json:"service_namespace"`
	LocalPort        int               `yaml:"local_port" json:"local_port"`
	ProxyMode        string            `yaml:"proxy_mode" json:"proxy_mode"`
	Selector         map[string]string `yaml:"selector,omitempty" json:"selector,omitempty"`
	LoadBalancer     string            `yaml:"load_balancer,omitempty" json:"load_balancer,omitempty"`
}

// Workload 定义工作负载实例，类似 K8s Pod
type Workload struct {
	Metadata WorkloadMetadata `yaml:"metadata" json:"metadata"`
	Spec     WorkloadSpec     `yaml:"spec" json:"spec"`
	Status   WorkloadStatus   `yaml:"status" json:"status"`
}

// WorkloadMetadata 工作负载元数据
type WorkloadMetadata struct {
	ID        string            `yaml:"id" json:"id"`
	Labels    map[string]string `yaml:"labels" json:"labels"`
	CreatedAt time.Time         `yaml:"created_at" json:"created_at"`
	
	// TTL 和清理相关元数据
	TTL       time.Duration     `yaml:"ttl,omitempty" json:"ttl,omitempty"`           // 生存时间
	LastSeen  time.Time         `yaml:"last_seen,omitempty" json:"last_seen,omitempty"` // 最后可见时间
}

// WorkloadSpec 工作负载规格定义
type WorkloadSpec struct {
	LocalAddr string `yaml:"local_addr" json:"local_addr"`
	LocalPort int    `yaml:"local_port" json:"local_port"`
	Protocol  string `yaml:"protocol" json:"protocol"`
}

// WorkloadStatus 工作负载状态
type WorkloadStatus struct {
	Ready      bool      `yaml:"ready" json:"ready"`
	Reason     string    `yaml:"reason,omitempty" json:"reason,omitempty"`
	Message    string    `yaml:"message,omitempty" json:"message,omitempty"`
	LastSeen   time.Time `yaml:"last_seen" json:"last_seen"`
	RegisterIP string    `yaml:"register_ip" json:"register_ip"` // 注册时的客户端IP
}

// ServiceRegistry 服务注册表接口
type ServiceRegistry interface {
	// 服务管理
	RegisterService(service *Service) error
	GetService(name, namespace string) (*Service, error)
	ListServices(namespace string) ([]*Service, error)
	
	// 工作负载管理 (K8s Pod 风格)
	RegisterWorkload(workload *Workload) error
	UnregisterWorkload(workloadID string) error
	GetWorkload(workloadID string) (*Workload, error)
	ListWorkloads(selector map[string]string) ([]*Workload, error)
	
	// 端点管理 (由 Controller 自动管理)
	GetEndpoints(serviceName, namespace string, selector map[string]string) ([]*Endpoint, error)
	RegisterEndpoint(serviceName, namespace string, endpoint *Endpoint) error
	UnregisterEndpoint(serviceName, namespace, endpointID string) error
	
	// Controller 使用的内部方法
	SyncEndpoints() error // 同步服务和工作负载，自动创建端点
}

// RegionServiceRegistry 区域服务注册表接口 (Phase 2 新增)
type RegionServiceRegistry interface {
	// 继承基础服务注册表功能
	ServiceRegistry
	
	// Phase 2: 区域服务发现
	GetEndpointsInRegion(serviceName, namespace, regionID string, selector map[string]string) ([]*Endpoint, error)
	GetEndpointsInSite(serviceName, namespace, siteID string, selector map[string]string) ([]*Endpoint, error)
	GetEndpointsInZone(serviceName, namespace, zone string, selector map[string]string) ([]*Endpoint, error)
	
	// Phase 2: 跨节点服务同步
	SyncServiceFromNode(nodeID string, service *Service) error
	SyncEndpointsFromNode(nodeID string, serviceName, namespace string, endpoints []*Endpoint) error
	BroadcastServiceUpdate(service *Service) error
	BroadcastEndpointUpdate(serviceName, namespace string, endpoints []*Endpoint) error
	
	// Phase 2: 远程服务缓存管理
	CacheRemoteService(nodeID string, service *Service, endpoints []*Endpoint) error
	GetCachedRemoteServices(nodeID string) ([]*RemoteServiceInfo, error)
	InvalidateRemoteServiceCache(nodeID string, serviceName, namespace string) error
	CleanupExpiredCache() error
	
	// Phase 2: 节点管理
	RegisterNeighborNode(node *NeighborNode) error
	UpdateNeighborStatus(nodeID, status string) error
	GetNeighborNodes() ([]*NeighborNode, error)
	GetActiveNeighbors() ([]*NeighborNode, error)
	
	// Phase 2: 服务查询增强 (本地+远程)
	QueryServiceMultiLevel(serviceName, namespace string, selector map[string]string, scope ServiceScope) ([]*Endpoint, error)
	
	// Phase 2: 健康检查和故障处理
	MarkEndpointUnhealthy(endpointID string, reason string) error
	RecoverEndpoint(endpointID string) error
	GetUnhealthyEndpoints() ([]*Endpoint, error)
}

// ServiceScope 服务查询范围 (Phase 2 新增)
type ServiceScope string

const (
	ScopeLocal    ServiceScope = "local"    // 仅本地节点
	ScopeRegion   ServiceScope = "region"   // 区域内所有节点  
	ScopeSite     ServiceScope = "site"     // 站点内所有节点
	ScopeZone     ServiceScope = "zone"     // 可用区内所有节点
	ScopeGlobal   ServiceScope = "global"   // 全局范围 (Phase 3)
)

// LoadBalancer 负载均衡器接口
type LoadBalancer interface {
	// 从端点列表中选择一个端点
	Select(endpoints []*Endpoint) (*Endpoint, error)
	
	// 获取负载均衡算法名称
	Name() string
}