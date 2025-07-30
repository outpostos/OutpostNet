package loadbalancer

import (
	"crypto/md5"
	"fmt"
	"hash/crc32"
	"sort"
	"sync"
	"time"

	"github.com/outpostos/OutpostNet/pkg/types"
)

// ConsistentHashLoadBalancer 一致性哈希负载均衡器
type ConsistentHashLoadBalancer struct {
	hashRing     map[uint32]string // hash -> endpoint ID
	sortedHashes []uint32          // 排序的哈希值
	virtualNodes int               // 每个端点的虚拟节点数
	hashFunc     HashFunction      // 哈希函数
	mu           sync.RWMutex
}

// HashFunction 哈希函数类型
type HashFunction func(data []byte) uint32

// 预定义的哈希函数
var (
	CRC32HashFunc = func(data []byte) uint32 {
		return crc32.ChecksumIEEE(data)
	}
	
	MD5HashFunc = func(data []byte) uint32 {
		hash := md5.Sum(data)
		return uint32(hash[0])<<24 | uint32(hash[1])<<16 | uint32(hash[2])<<8 | uint32(hash[3])
	}
)

// NewConsistentHashLoadBalancer 创建一致性哈希负载均衡器
func NewConsistentHashLoadBalancer() *ConsistentHashLoadBalancer {
	return &ConsistentHashLoadBalancer{
		hashRing:     make(map[uint32]string),
		sortedHashes: make([]uint32, 0),
		virtualNodes: 150, // 默认虚拟节点数
		hashFunc:     CRC32HashFunc,
	}
}

// NewConsistentHashLoadBalancerWithOptions 创建带选项的一致性哈希负载均衡器
func NewConsistentHashLoadBalancerWithOptions(virtualNodes int, hashFunc HashFunction) *ConsistentHashLoadBalancer {
	if virtualNodes <= 0 {
		virtualNodes = 150
	}
	if hashFunc == nil {
		hashFunc = CRC32HashFunc
	}
	
	return &ConsistentHashLoadBalancer{
		hashRing:     make(map[uint32]string),
		sortedHashes: make([]uint32, 0),
		virtualNodes: virtualNodes,
		hashFunc:     hashFunc,
	}
}

// Name 返回负载均衡器名称
func (lb *ConsistentHashLoadBalancer) Name() string {
	return "consistent_hash"
}

// Select 实现 types.LoadBalancer 接口，从端点列表中选择一个端点
func (lb *ConsistentHashLoadBalancer) Select(endpoints []*types.Endpoint) (*types.Endpoint, error) {
	if len(endpoints) == 0 {
		return nil, fmt.Errorf("no endpoints available")
	}
	
	// 对于一致性哈希，我们使用第一个端点的ID作为默认key
	// 在实际使用中，调用方应该提供具体的key
	key := "default"
	if len(endpoints) > 0 {
		key = endpoints[0].ID
	}
	
	return lb.SelectEndpoint(endpoints, key), nil
}

// SelectEndpoint 使用一致性哈希算法选择端点
func (lb *ConsistentHashLoadBalancer) SelectEndpoint(endpoints []*types.Endpoint, key string) *types.Endpoint {
	if len(endpoints) == 0 {
		return nil
	}

	// 如果没有提供key，使用默认key
	if key == "" {
		key = "default"
	}

	lb.mu.Lock()
	defer lb.mu.Unlock()

	// 重建哈希环（如果端点发生变化）
	lb.rebuildHashRing(endpoints)

	// 计算键的哈希值
	keyHash := lb.hashFunc([]byte(key))

	// 在哈希环中查找
	selectedID := lb.findInHashRing(keyHash)
	
	// 查找对应的端点
	for _, ep := range endpoints {
		if ep.ID == selectedID {
			return ep
		}
	}

	// 如果没找到，返回第一个
	return endpoints[0]
}

// UpdateStats 更新端点统计信息
func (lb *ConsistentHashLoadBalancer) UpdateStats(endpoint *types.Endpoint, latency time.Duration, success bool) {
	// 一致性哈希负载均衡器通常不需要统计信息
	// 但可以在这里实现基于性能的虚拟节点调整逻辑
}

// SetVirtualNodes 设置虚拟节点数
func (lb *ConsistentHashLoadBalancer) SetVirtualNodes(count int) {
	if count <= 0 {
		count = 150
	}
	
	lb.mu.Lock()
	defer lb.mu.Unlock()
	
	lb.virtualNodes = count
	// 清空现有哈希环，下次选择时会重建
	lb.hashRing = make(map[uint32]string)
	lb.sortedHashes = lb.sortedHashes[:0]
}

// GetVirtualNodes 获取虚拟节点数
func (lb *ConsistentHashLoadBalancer) GetVirtualNodes() int {
	lb.mu.RLock()
	defer lb.mu.RUnlock()
	return lb.virtualNodes
}

// SetHashFunction 设置哈希函数
func (lb *ConsistentHashLoadBalancer) SetHashFunction(hashFunc HashFunction) {
	if hashFunc == nil {
		return
	}
	
	lb.mu.Lock()
	defer lb.mu.Unlock()
	
	lb.hashFunc = hashFunc
	// 清空现有哈希环，下次选择时会重建
	lb.hashRing = make(map[uint32]string)
	lb.sortedHashes = lb.sortedHashes[:0]
}

// GetHashRingInfo 获取哈希环信息（用于监控和调试）
func (lb *ConsistentHashLoadBalancer) GetHashRingInfo() map[string]interface{} {
	lb.mu.RLock()
	defer lb.mu.RUnlock()
	
	info := make(map[string]interface{})
	info["virtual_nodes"] = lb.virtualNodes
	info["ring_size"] = len(lb.hashRing)
	info["endpoints_count"] = len(lb.hashRing) / lb.virtualNodes
	
	// 统计每个端点的虚拟节点分布
	endpointCounts := make(map[string]int)
	for _, endpointID := range lb.hashRing {
		endpointCounts[endpointID]++
	}
	info["endpoint_distribution"] = endpointCounts
	
	return info
}

// rebuildHashRing 重建哈希环
func (lb *ConsistentHashLoadBalancer) rebuildHashRing(endpoints []*types.Endpoint) {
	// 检查是否需要重建
	if lb.needsRebuild(endpoints) {
		lb.doRebuildHashRing(endpoints)
	}
}

// needsRebuild 检查是否需要重建哈希环
func (lb *ConsistentHashLoadBalancer) needsRebuild(endpoints []*types.Endpoint) bool {
	expectedSize := len(endpoints) * lb.virtualNodes
	if len(lb.hashRing) != expectedSize {
		return true
	}
	
	// 检查端点是否发生变化
	endpointIDs := make(map[string]bool)
	for _, ep := range endpoints {
		endpointIDs[ep.ID] = true
	}
	
	for _, endpointID := range lb.hashRing {
		if !endpointIDs[endpointID] {
			return true
		}
	}
	
	return false
}

// doRebuildHashRing 执行哈希环重建
func (lb *ConsistentHashLoadBalancer) doRebuildHashRing(endpoints []*types.Endpoint) {
	// 清空现有的哈希环
	lb.hashRing = make(map[uint32]string)
	lb.sortedHashes = lb.sortedHashes[:0]

	// 为每个端点添加多个虚拟节点
	for _, ep := range endpoints {
		lb.addEndpointToRing(ep)
	}

	// 排序哈希值
	sort.Slice(lb.sortedHashes, func(i, j int) bool {
		return lb.sortedHashes[i] < lb.sortedHashes[j]
	})
}

// addEndpointToRing 将端点添加到哈希环
func (lb *ConsistentHashLoadBalancer) addEndpointToRing(endpoint *types.Endpoint) {
	for i := 0; i < lb.virtualNodes; i++ {
		virtualKey := lb.generateVirtualKey(endpoint, i)
		hash := lb.hashFunc([]byte(virtualKey))
		lb.hashRing[hash] = endpoint.ID
		lb.sortedHashes = append(lb.sortedHashes, hash)
	}
}

// generateVirtualKey 生成虚拟节点的键
func (lb *ConsistentHashLoadBalancer) generateVirtualKey(endpoint *types.Endpoint, index int) string {
	// 使用端点ID和索引生成虚拟键
	// 也可以考虑端点的其他属性，如IP地址等
	return fmt.Sprintf("%s#%d", endpoint.ID, index)
}

// findInHashRing 在哈希环中查找端点
func (lb *ConsistentHashLoadBalancer) findInHashRing(keyHash uint32) string {
	if len(lb.sortedHashes) == 0 {
		return ""
	}

	// 二分查找第一个大于等于 keyHash 的值
	idx := sort.Search(len(lb.sortedHashes), func(i int) bool {
		return lb.sortedHashes[i] >= keyHash
	})

	// 如果没找到，返回第一个（环形）
	if idx == len(lb.sortedHashes) {
		idx = 0
	}

	hash := lb.sortedHashes[idx]
	return lb.hashRing[hash]
}

// GetEndpointForKey 获取指定key对应的端点ID（用于测试和调试）
func (lb *ConsistentHashLoadBalancer) GetEndpointForKey(endpoints []*types.Endpoint, key string) string {
	if len(endpoints) == 0 || key == "" {
		return ""
	}

	lb.mu.Lock()
	defer lb.mu.Unlock()

	lb.rebuildHashRing(endpoints)
	keyHash := lb.hashFunc([]byte(key))
	return lb.findInHashRing(keyHash)
}

// RemoveEndpoint 从哈希环中移除端点
func (lb *ConsistentHashLoadBalancer) RemoveEndpoint(endpointID string) {
	lb.mu.Lock()
	defer lb.mu.Unlock()

	// 移除该端点的所有虚拟节点
	newHashRing := make(map[uint32]string)
	newSortedHashes := make([]uint32, 0)

	for hash, id := range lb.hashRing {
		if id != endpointID {
			newHashRing[hash] = id
			newSortedHashes = append(newSortedHashes, hash)
		}
	}

	lb.hashRing = newHashRing
	lb.sortedHashes = newSortedHashes

	// 重新排序
	sort.Slice(lb.sortedHashes, func(i, j int) bool {
		return lb.sortedHashes[i] < lb.sortedHashes[j]
	})
}

// AddEndpoint 向哈希环中添加端点
func (lb *ConsistentHashLoadBalancer) AddEndpoint(endpoint *types.Endpoint) {
	lb.mu.Lock()
	defer lb.mu.Unlock()

	// 检查端点是否已存在
	for _, id := range lb.hashRing {
		if id == endpoint.ID {
			return // 端点已存在
		}
	}

	// 添加端点到哈希环
	lb.addEndpointToRing(endpoint)

	// 重新排序
	sort.Slice(lb.sortedHashes, func(i, j int) bool {
		return lb.sortedHashes[i] < lb.sortedHashes[j]
	})
}