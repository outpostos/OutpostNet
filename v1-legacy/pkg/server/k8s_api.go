package server

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/outpostos/OutpostNet/pkg/types"
)

// K8sAPI K8s 兼容的 REST API 服务器
type K8sAPI struct {
	server *Server
}

// NewK8sAPI 创建 K8s API 服务器
func NewK8sAPI(server *Server) *K8sAPI {
	return &K8sAPI{server: server}
}

// Start 启动 K8s API 服务器
func (k *K8sAPI) Start(port int) error {
	mux := http.NewServeMux()
	
	// K8s API 核心路由
	mux.HandleFunc("/api/v1/namespaces/", k.handleNamespacedResources)
	mux.HandleFunc("/api/v1/services", k.handleServices)
	mux.HandleFunc("/api/v1/endpoints", k.handleEndpoints)
	
	// 健康检查
	mux.HandleFunc("/healthz", k.handleHealthz)
	mux.HandleFunc("/readyz", k.handleReadyz)
	
	// API 发现 - 顺序很重要，更具体的路径要在前面
	mux.HandleFunc("/apis", k.handleAPIGroups)
	mux.HandleFunc("/api/v1", k.handleV1Resources)
	mux.HandleFunc("/api", k.handleAPIVersions)
	mux.HandleFunc("/version", k.handleVersion)
	mux.HandleFunc("/", k.handleRoot)
	
	addr := fmt.Sprintf(":%d", port)
	server := &http.Server{
		Addr:    addr,
		Handler: k.addCORSHeaders(mux),
	}
	
	return server.ListenAndServe()
}

// addCORSHeaders 添加 CORS 头部
func (k *K8sAPI) addCORSHeaders(handler http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
		
		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}
		
		handler.ServeHTTP(w, r)
	})
}

// handleNamespacedResources 处理命名空间下的资源
func (k *K8sAPI) handleNamespacedResources(w http.ResponseWriter, r *http.Request) {
	// 解析路径：/api/v1/namespaces/{namespace}/{resource}
	path := strings.TrimPrefix(r.URL.Path, "/api/v1/namespaces/")
	parts := strings.Split(path, "/")
	
	if len(parts) < 2 {
		k.sendError(w, http.StatusBadRequest, "Invalid namespace resource path")
		return
	}
	
	namespace := parts[0]
	resource := parts[1]
	
	switch resource {
	case "services":
		k.handleNamespacedServices(w, r, namespace)
	case "endpoints":
		k.handleNamespacedEndpoints(w, r, namespace)
	default:
		k.sendError(w, http.StatusNotFound, fmt.Sprintf("Resource %s not found", resource))
	}
}

// handleServices 处理服务列表
func (k *K8sAPI) handleServices(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case "GET":
		k.listAllServices(w, r)
	default:
		k.sendError(w, http.StatusMethodNotAllowed, "Method not allowed")
	}
}

// handleNamespacedServices 处理命名空间下的服务
func (k *K8sAPI) handleNamespacedServices(w http.ResponseWriter, r *http.Request, namespace string) {
	// 解析路径，检查是否是单个服务
	path := strings.TrimPrefix(r.URL.Path, "/api/v1/namespaces/"+namespace+"/services")
	if path != "" && path != "/" {
		// 单个服务处理：/api/v1/namespaces/{namespace}/services/{name}
		serviceName := strings.Trim(path, "/")
		k.handleSingleService(w, r, namespace, serviceName)
		return
	}
	
	switch r.Method {
	case "GET":
		k.listServices(w, r, namespace)
	case "POST":
		k.createService(w, r, namespace)
	default:
		k.sendError(w, http.StatusMethodNotAllowed, "Method not allowed")
	}
}

// handleEndpoints 处理端点列表
func (k *K8sAPI) handleEndpoints(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case "GET":
		k.listAllEndpoints(w, r)
	default:
		k.sendError(w, http.StatusMethodNotAllowed, "Method not allowed")
	}
}

// handleNamespacedEndpoints 处理命名空间下的端点
func (k *K8sAPI) handleNamespacedEndpoints(w http.ResponseWriter, r *http.Request, namespace string) {
	switch r.Method {
	case "GET":
		k.listEndpoints(w, r, namespace)
	default:
		k.sendError(w, http.StatusMethodNotAllowed, "Method not allowed")
	}
}

// listAllServices 列出所有服务
func (k *K8sAPI) listAllServices(w http.ResponseWriter, r *http.Request) {
	services, err := k.server.registry.ListServices("")
	if err != nil {
		k.sendError(w, http.StatusInternalServerError, err.Error())
		return
	}
	
	// 转换为 K8s Service List 格式
	serviceList := k8sServiceList{
		APIVersion: "v1",
		Kind:       "ServiceList",
		Items:      make([]k8sService, len(services)),
	}
	
	for i, service := range services {
		serviceList.Items[i] = k.convertToK8sService(service)
	}
	
	k.sendJSON(w, serviceList)
}

// listServices 列出指定命名空间的服务
func (k *K8sAPI) listServices(w http.ResponseWriter, r *http.Request, namespace string) {
	services, err := k.server.registry.ListServices(namespace)
	if err != nil {
		k.sendError(w, http.StatusInternalServerError, err.Error())
		return
	}
	
	// 转换为 K8s Service List 格式
	serviceList := k8sServiceList{
		APIVersion: "v1",
		Kind:       "ServiceList",
		Items:      make([]k8sService, len(services)),
	}
	
	for i, service := range services {
		serviceList.Items[i] = k.convertToK8sService(service)
	}
	
	k.sendJSON(w, serviceList)
}

// handleSingleService 处理单个服务
func (k *K8sAPI) handleSingleService(w http.ResponseWriter, r *http.Request, namespace, serviceName string) {
	switch r.Method {
	case "GET":
		k.getService(w, r, namespace, serviceName)
	case "PUT":
		k.updateService(w, r, namespace, serviceName)
	case "DELETE":
		k.deleteService(w, r, namespace, serviceName)
	default:
		k.sendError(w, http.StatusMethodNotAllowed, "Method not allowed")
	}
}

// getService 获取单个服务
func (k *K8sAPI) getService(w http.ResponseWriter, r *http.Request, namespace, serviceName string) {
	service, err := k.server.registry.GetService(serviceName, namespace)
	if err != nil {
		k.sendError(w, http.StatusNotFound, fmt.Sprintf("Service %s not found in namespace %s", serviceName, namespace))
		return
	}
	
	k8sService := k.convertToK8sService(service)
	k.sendJSON(w, k8sService)
}

// updateService 更新服务
func (k *K8sAPI) updateService(w http.ResponseWriter, r *http.Request, namespace, serviceName string) {
	var k8sService k8sService
	if err := json.NewDecoder(r.Body).Decode(&k8sService); err != nil {
		k.sendError(w, http.StatusBadRequest, "Invalid JSON")
		return
	}
	
	// 转换为 outpostnet Service 格式
	service := k.convertFromK8sService(&k8sService, namespace)
	service.Metadata.Name = serviceName
	
	// 更新服务
	if err := k.server.registry.RegisterService(service); err != nil {
		k.sendError(w, http.StatusInternalServerError, err.Error())
		return
	}
	
	// 返回更新的服务
	k.sendJSON(w, k.convertToK8sService(service))
}

// deleteService 删除服务
func (k *K8sAPI) deleteService(w http.ResponseWriter, r *http.Request, namespace, serviceName string) {
	// TODO: 实现删除功能（需要在 registry 中添加 DeleteService 方法）
	k.sendError(w, http.StatusNotImplemented, "Delete service not implemented yet")
}

// createService 创建服务
func (k *K8sAPI) createService(w http.ResponseWriter, r *http.Request, namespace string) {
	var k8sService k8sService
	if err := json.NewDecoder(r.Body).Decode(&k8sService); err != nil {
		k.sendError(w, http.StatusBadRequest, "Invalid JSON")
		return
	}
	
	// 转换为 outpostnet Service 格式
	service := k.convertFromK8sService(&k8sService, namespace)
	
	// 注册服务
	if err := k.server.registry.RegisterService(service); err != nil {
		k.sendError(w, http.StatusConflict, err.Error())
		return
	}
	
	// 返回创建的服务
	w.WriteHeader(http.StatusCreated)
	k.sendJSON(w, k.convertToK8sService(service))
}

// listAllEndpoints 列出所有端点
func (k *K8sAPI) listAllEndpoints(w http.ResponseWriter, r *http.Request) {
	services, err := k.server.registry.ListServices("")
	if err != nil {
		k.sendError(w, http.StatusInternalServerError, err.Error())
		return
	}
	
	var allEndpoints []k8sEndpoints
	for _, service := range services {
		endpoints, err := k.server.registry.GetEndpoints(service.Metadata.Name, service.Metadata.Namespace, nil)
		if err != nil {
			continue
		}
		
		k8sEp := k.convertToK8sEndpoints(service, endpoints)
		allEndpoints = append(allEndpoints, k8sEp)
	}
	
	endpointsList := k8sEndpointsList{
		APIVersion: "v1",
		Kind:       "EndpointsList",
		Items:      allEndpoints,
	}
	
	k.sendJSON(w, endpointsList)
}

// listEndpoints 列出指定命名空间的端点
func (k *K8sAPI) listEndpoints(w http.ResponseWriter, r *http.Request, namespace string) {
	services, err := k.server.registry.ListServices(namespace)
	if err != nil {
		k.sendError(w, http.StatusInternalServerError, err.Error())
		return
	}
	
	var allEndpoints []k8sEndpoints
	for _, service := range services {
		endpoints, err := k.server.registry.GetEndpoints(service.Metadata.Name, service.Metadata.Namespace, nil)
		if err != nil {
			continue
		}
		
		k8sEp := k.convertToK8sEndpoints(service, endpoints)
		allEndpoints = append(allEndpoints, k8sEp)
	}
	
	endpointsList := k8sEndpointsList{
		APIVersion: "v1",
		Kind:       "EndpointsList",
		Items:      allEndpoints,
	}
	
	k.sendJSON(w, endpointsList)
}

// handleHealthz 健康检查
func (k *K8sAPI) handleHealthz(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("ok"))
}

// handleReadyz 就绪检查
func (k *K8sAPI) handleReadyz(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("ok"))
}

// handleAPIVersions API 版本发现
func (k *K8sAPI) handleAPIVersions(w http.ResponseWriter, r *http.Request) {
	versions := map[string]interface{}{
		"kind":       "APIVersions",
		"apiVersion": "v1",
		"versions":   []string{"v1"},
	}
	k.sendJSON(w, versions)
}

// handleV1Resources v1 资源发现
func (k *K8sAPI) handleV1Resources(w http.ResponseWriter, r *http.Request) {
	resources := map[string]interface{}{
		"kind":       "APIResourceList",
		"apiVersion": "v1",
		"groupVersion": "v1",
		"resources": []map[string]interface{}{
			{
				"name":       "services",
				"singularName": "service",
				"namespaced": true,
				"kind":       "Service",
				"verbs":      []string{"get", "list", "create", "update", "patch", "delete"},
			},
			{
				"name":       "endpoints",
				"singularName": "endpoints",
				"namespaced": true,
				"kind":       "Endpoints",
				"verbs":      []string{"get", "list"},
			},
		},
	}
	k.sendJSON(w, resources)
}

// handleAPIGroups 处理 API 组发现
func (k *K8sAPI) handleAPIGroups(w http.ResponseWriter, r *http.Request) {
	groups := map[string]interface{}{
		"kind":       "APIGroupList",
		"apiVersion": "v1",
		"groups":     []interface{}{}, // outpostnet 只支持核心 API 组
	}
	k.sendJSON(w, groups)
}

// handleVersion 处理版本信息
func (k *K8sAPI) handleVersion(w http.ResponseWriter, r *http.Request) {
	version := map[string]interface{}{
		"major":        "1",
		"minor":        "28",
		"gitVersion":   "v1.28.0-outpostnet",
		"gitCommit":    "outpostnet-proxy",
		"buildDate":    "2025-07-15T12:00:00Z",
		"goVersion":    "go1.21.0",
		"compiler":     "gc",
		"platform":     "linux/amd64",
	}
	k.sendJSON(w, version)
}

// handleRoot 处理根路径
func (k *K8sAPI) handleRoot(w http.ResponseWriter, r *http.Request) {
	root := map[string]interface{}{
		"paths": []string{
			"/api",
			"/api/v1",
			"/apis",
			"/healthz",
			"/readyz",
			"/version",
		},
	}
	k.sendJSON(w, root)
}

// convertToK8sService 转换为 K8s Service 格式
func (k *K8sAPI) convertToK8sService(service *types.Service) k8sService {
	k8sSvc := k8sService{
		APIVersion: "v1",
		Kind:       "Service",
		Metadata: k8sObjectMeta{
			Name:      service.Metadata.Name,
			Namespace: service.Metadata.Namespace,
			Labels:    service.Metadata.Labels,
		},
		Spec: k8sServiceSpec{
			Selector: service.Spec.Selector,
			Ports:    make([]k8sServicePort, len(service.Spec.Ports)),
		},
	}
	
	for i, port := range service.Spec.Ports {
		k8sSvc.Spec.Ports[i] = k8sServicePort{
			Name:       port.Name,
			Port:       port.Port,
			TargetPort: port.TargetPort,
			Protocol:   strings.ToUpper(port.Protocol),
		}
	}
	
	return k8sSvc
}

// convertFromK8sService 从 K8s Service 格式转换
func (k *K8sAPI) convertFromK8sService(k8sSvc *k8sService, namespace string) *types.Service {
	service := &types.Service{
		Metadata: types.ServiceMetadata{
			Name:      k8sSvc.Metadata.Name,
			Namespace: namespace,
			Labels:    k8sSvc.Metadata.Labels,
		},
		Spec: types.ServiceSpec{
			Selector: k8sSvc.Spec.Selector,
			Ports:    make([]types.ServicePort, len(k8sSvc.Spec.Ports)),
		},
	}
	
	for i, port := range k8sSvc.Spec.Ports {
		service.Spec.Ports[i] = types.ServicePort{
			Name:       port.Name,
			Port:       port.Port,
			TargetPort: port.TargetPort,
			Protocol:   strings.ToLower(port.Protocol),
		}
	}
	
	return service
}

// convertToK8sEndpoints 转换为 K8s Endpoints 格式
func (k *K8sAPI) convertToK8sEndpoints(service *types.Service, endpoints []*types.Endpoint) k8sEndpoints {
	k8sEp := k8sEndpoints{
		APIVersion: "v1",
		Kind:       "Endpoints",
		Metadata: k8sObjectMeta{
			Name:      service.Metadata.Name,
			Namespace: service.Metadata.Namespace,
		},
		Subsets: []k8sEndpointSubset{},
	}
	
	if len(endpoints) > 0 {
		// 按端口分组端点
		portMap := make(map[int][]k8sEndpointAddress)
		
		for _, ep := range endpoints {
			address := k8sEndpointAddress{
				IP: ep.IP,
			}
			portMap[ep.Port] = append(portMap[ep.Port], address)
		}
		
		// 为每个端口创建子集
		for port, addresses := range portMap {
			subset := k8sEndpointSubset{
				Addresses: addresses,
				Ports: []k8sEndpointPort{
					{
						Port:     port,
						Protocol: "TCP",
					},
				},
			}
			k8sEp.Subsets = append(k8sEp.Subsets, subset)
		}
	}
	
	return k8sEp
}

// sendJSON 发送 JSON 响应
func (k *K8sAPI) sendJSON(w http.ResponseWriter, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(data)
}

// sendError 发送错误响应
func (k *K8sAPI) sendError(w http.ResponseWriter, code int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	
	errorResponse := map[string]interface{}{
		"kind":       "Status",
		"apiVersion": "v1",
		"status":     "Failure",
		"message":    message,
		"code":       code,
	}
	
	json.NewEncoder(w).Encode(errorResponse)
}

// K8s 兼容的数据结构

type k8sObjectMeta struct {
	Name      string            `json:"name"`
	Namespace string            `json:"namespace,omitempty"`
	Labels    map[string]string `json:"labels,omitempty"`
}

type k8sService struct {
	APIVersion string           `json:"apiVersion"`
	Kind       string           `json:"kind"`
	Metadata   k8sObjectMeta    `json:"metadata"`
	Spec       k8sServiceSpec   `json:"spec"`
}

type k8sServiceSpec struct {
	Selector map[string]string `json:"selector,omitempty"`
	Ports    []k8sServicePort  `json:"ports"`
}

type k8sServicePort struct {
	Name       string `json:"name,omitempty"`
	Port       int    `json:"port"`
	TargetPort int    `json:"targetPort,omitempty"`
	Protocol   string `json:"protocol,omitempty"`
}

type k8sServiceList struct {
	APIVersion string      `json:"apiVersion"`
	Kind       string      `json:"kind"`
	Items      []k8sService `json:"items"`
}

type k8sEndpoints struct {
	APIVersion string              `json:"apiVersion"`
	Kind       string              `json:"kind"`
	Metadata   k8sObjectMeta       `json:"metadata"`
	Subsets    []k8sEndpointSubset `json:"subsets"`
}

type k8sEndpointSubset struct {
	Addresses []k8sEndpointAddress `json:"addresses"`
	Ports     []k8sEndpointPort    `json:"ports"`
}

type k8sEndpointAddress struct {
	IP string `json:"ip"`
}

type k8sEndpointPort struct {
	Port     int    `json:"port"`
	Protocol string `json:"protocol,omitempty"`
}

type k8sEndpointsList struct {
	APIVersion string         `json:"apiVersion"`
	Kind       string         `json:"kind"`
	Items      []k8sEndpoints `json:"items"`
}