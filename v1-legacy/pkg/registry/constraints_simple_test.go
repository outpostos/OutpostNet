package registry

import (
	"testing"
	"time"

	"github.com/outpostos/OutpostNet/pkg/types"
)

func TestConstraintFilter_SimpleFiltering(t *testing.T) {
	filter := NewConstraintFilter()

	// 创建测试端点
	endpoints := []*types.Endpoint{
		{
			ID:       "ep1",
			IP:       "10.0.0.1",
			Port:     8080,
			Labels:   map[string]string{"app": "web", "version": "v1", "env": "prod"},
			Status:   types.EndpointStatus{Ready: true},
			LastSeen: time.Now(),
		},
		{
			ID:       "ep2", 
			IP:       "10.0.0.2",
			Port:     8080,
			Labels:   map[string]string{"app": "web", "version": "v2", "env": "prod"},
			Status:   types.EndpointStatus{Ready: true},
			LastSeen: time.Now(),
		},
		{
			ID:       "ep3",
			IP:       "10.0.0.3", 
			Port:     8080,
			Labels:   map[string]string{"app": "api", "version": "v1", "env": "staging"},
			Status:   types.EndpointStatus{Ready: false},
			LastSeen: time.Now(),
		},
		{
			ID:       "ep4",
			IP:       "10.0.0.4",
			Port:     8080,
			Labels:   map[string]string{"app": "web", "version": "v1", "env": "staging"}, 
			Status:   types.EndpointStatus{Ready: true},
			LastSeen: time.Now(),
		},
	}

	tests := []struct {
		name        string
		constraints *types.ServiceConstraints
		expected    []string // 期望的端点ID列表
		expectError bool
	}{
		{
			name:        "无约束条件",
			constraints: nil,
			expected:    []string{"ep1", "ep2", "ep3", "ep4"},
		},
		{
			name: "只要求健康端点",
			constraints: &types.ServiceConstraints{
				HealthyOnly: true,
			},
			expected: []string{"ep1", "ep2", "ep4"},
		},
		{
			name: "标签选择器-匹配标签",
			constraints: &types.ServiceConstraints{
				LabelSelector: &types.LabelSelector{
					MatchLabels: map[string]string{
						"app": "web",
					},
				},
			},
			expected: []string{"ep1", "ep2", "ep4"},
		},
		{
			name: "标签选择器-In操作",
			constraints: &types.ServiceConstraints{
				LabelSelector: &types.LabelSelector{
					MatchExpressions: []types.LabelSelectorRequirement{
						{
							Key:      "version",
							Operator: types.LabelSelectorOpIn,
							Values:   []string{"v1", "v2"},
						},
					},
				},
			},
			expected: []string{"ep1", "ep2", "ep3", "ep4"},
		},
		{
			name: "标签选择器-NotIn操作",
			constraints: &types.ServiceConstraints{
				LabelSelector: &types.LabelSelector{
					MatchExpressions: []types.LabelSelectorRequirement{
						{
							Key:      "env",
							Operator: types.LabelSelectorOpNotIn,
							Values:   []string{"staging"},
						},
					},
				},
			},
			expected: []string{"ep1", "ep2"},
		},
		{
			name: "标签选择器-Exists操作",
			constraints: &types.ServiceConstraints{
				LabelSelector: &types.LabelSelector{
					MatchExpressions: []types.LabelSelectorRequirement{
						{
							Key:      "app",
							Operator: types.LabelSelectorOpExists,
						},
					},
				},
			},
			expected: []string{"ep1", "ep2", "ep3", "ep4"},
		},
		{
			name: "组合约束条件-健康+标签选择器",
			constraints: &types.ServiceConstraints{
				HealthyOnly: true,
				LabelSelector: &types.LabelSelector{
					MatchLabels: map[string]string{
						"app": "web",
					},
					MatchExpressions: []types.LabelSelectorRequirement{
						{
							Key:      "env",
							Operator: types.LabelSelectorOpIn,
							Values:   []string{"prod"},
						},
					},
				},
			},
			expected: []string{"ep1", "ep2"}, // 健康+app=web+env=prod
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := filter.FilterEndpoints(endpoints, tt.constraints)

			if tt.expectError {
				if err == nil {
					t.Errorf("期望出现错误，但没有错误")
				}
				return
			}

			if err != nil {
				t.Fatalf("意外的错误: %v", err)
			}

			if len(result) != len(tt.expected) {
				t.Errorf("期望 %d 个端点，实际得到 %d 个", len(tt.expected), len(result))
			}

			// 检查结果端点ID是否匹配期望
			resultIDs := make(map[string]bool)
			for _, ep := range result {
				resultIDs[ep.ID] = true
			}

			for _, expectedID := range tt.expected {
				if !resultIDs[expectedID] {
					t.Errorf("缺少期望的端点 %s", expectedID)
				}
			}
		})
	}
}

func TestConstraintFilter_SimpleValidation(t *testing.T) {
	filter := NewConstraintFilter()

	tests := []struct {
		name        string
		constraints *types.ServiceConstraints
		expectError bool
	}{
		{
			name:        "nil约束条件",
			constraints: nil,
			expectError: false,
		},
		{
			name: "只有健康约束",
			constraints: &types.ServiceConstraints{
				HealthyOnly: true,
			},
			expectError: false,
		},
		{
			name: "有效的标签选择器",
			constraints: &types.ServiceConstraints{
				LabelSelector: &types.LabelSelector{
					MatchLabels: map[string]string{"app": "web"},
					MatchExpressions: []types.LabelSelectorRequirement{
						{
							Key:      "version",
							Operator: types.LabelSelectorOpIn,
							Values:   []string{"v1", "v2"},
						},
					},
				},
			},
			expectError: false,
		},
		{
			name: "无效的标签选择器-空key",
			constraints: &types.ServiceConstraints{
				LabelSelector: &types.LabelSelector{
					MatchExpressions: []types.LabelSelectorRequirement{
						{
							Key:      "",
							Operator: types.LabelSelectorOpExists,
						},
					},
				},
			},
			expectError: true,
		},
		{
			name: "无效的标签选择器-In操作无值",
			constraints: &types.ServiceConstraints{
				LabelSelector: &types.LabelSelector{
					MatchExpressions: []types.LabelSelectorRequirement{
						{
							Key:      "version",
							Operator: types.LabelSelectorOpIn,
							Values:   []string{},
						},
					},
				},
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := filter.ValidateConstraints(tt.constraints)

			if tt.expectError && err == nil {
				t.Error("期望出现错误，但没有错误")
			}

			if !tt.expectError && err != nil {
				t.Errorf("意外的错误: %v", err)
			}
		})
	}
}

func TestK8sMemoryRegistry_SimpleConstraints(t *testing.T) {
	registry := NewK8sMemoryRegistry()

	// 注册测试服务-只使用基础约束
	service := &types.Service{
		Metadata: types.ServiceMetadata{
			Name:      "test-service",
			Namespace: "default",
		},
		Spec: types.ServiceSpec{
			Selector: map[string]string{"app": "web"},
			Constraints: &types.ServiceConstraints{
				HealthyOnly: true,
				LabelSelector: &types.LabelSelector{
					MatchLabels: map[string]string{
						"env": "prod",
					},
				},
			},
		},
	}

	err := registry.RegisterService(service)
	if err != nil {
		t.Fatalf("注册服务失败: %v", err)
	}

	// 注册测试端点
	endpoints := []*types.Endpoint{
		{
			ID:     "ep1",
			IP:     "10.0.0.1",
			Port:   8080,
			Labels: map[string]string{"app": "web", "env": "prod"},
			Status: types.EndpointStatus{Ready: true},
		},
		{
			ID:     "ep2",
			IP:     "10.0.0.2", 
			Port:   8080,
			Labels: map[string]string{"app": "web", "env": "prod"},
			Status: types.EndpointStatus{Ready: true},
		},
		{
			ID:     "ep3",
			IP:     "10.0.0.3",
			Port:   8080,
			Labels: map[string]string{"app": "web", "env": "staging"},
			Status: types.EndpointStatus{Ready: true},
		},
		{
			ID:     "ep4",
			IP:     "10.0.0.4",
			Port:   8080,
			Labels: map[string]string{"app": "web", "env": "prod"},
			Status: types.EndpointStatus{Ready: false},
		},
	}

	for _, ep := range endpoints {
		err := registry.RegisterEndpoint("test-service", "default", ep)
		if err != nil {
			t.Fatalf("注册端点失败: %v", err)
		}
	}

	// 获取带约束条件的端点
	result, err := registry.GetEndpointsWithConstraints("test-service", "default")
	if err != nil {
		t.Fatalf("获取端点失败: %v", err)
	}

	// 期望结果：只有ep1和ep2符合条件 (健康+app=web+env=prod)
	expectedIDs := []string{"ep1", "ep2"}
	if len(result) != len(expectedIDs) {
		t.Errorf("期望 %d 个端点，实际得到 %d 个", len(expectedIDs), len(result))
	}

	resultIDs := make(map[string]bool)
	for _, ep := range result {
		resultIDs[ep.ID] = true
	}

	for _, expectedID := range expectedIDs {
		if !resultIDs[expectedID] {
			t.Errorf("缺少期望的端点 %s", expectedID)
		}
	}
}