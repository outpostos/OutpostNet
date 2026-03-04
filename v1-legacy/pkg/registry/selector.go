package registry

import (
	"fmt"
	"strings"

	"github.com/outpostos/OutpostNet/pkg/types"
)

// EndpointSelector 端点选择器，用于根据约束条件过滤端点
type EndpointSelector struct{}

// NewEndpointSelector 创建新的端点选择器
func NewEndpointSelector() *EndpointSelector {
	return &EndpointSelector{}
}

// SelectEndpoints 根据约束条件选择端点 (简化版本)
func (s *EndpointSelector) SelectEndpoints(endpoints []*types.Endpoint, constraints *types.ServiceConstraints) ([]*types.Endpoint, error) {
	if constraints == nil {
		return endpoints, nil
	}

	var filtered []*types.Endpoint

	for _, endpoint := range endpoints {
		if s.matchesConstraints(endpoint, constraints) {
			filtered = append(filtered, endpoint)
		}
	}

	return filtered, nil
}

// matchesConstraints 检查端点是否匹配约束条件 (简化版本)
func (s *EndpointSelector) matchesConstraints(endpoint *types.Endpoint, constraints *types.ServiceConstraints) bool {
	// 1. 检查健康状态约束
	if constraints.HealthyOnly && !endpoint.Status.Ready {
		return false
	}

	// 2. 检查标签选择器
	if constraints.LabelSelector != nil {
		if !s.matchesLabelSelector(endpoint, constraints.LabelSelector) {
			return false
		}
	}

	return true
}

// matchesRequiredLabels 检查端点是否匹配必需标签
func (s *EndpointSelector) matchesRequiredLabels(endpoint *types.Endpoint, requiredLabels map[string]string) bool {
	for key, value := range requiredLabels {
		endpointValue, exists := endpoint.Labels[key]
		if !exists || endpointValue != value {
			return false
		}
	}
	return true
}

// matchesLabelSelector 检查端点是否匹配标签选择器
func (s *EndpointSelector) matchesLabelSelector(endpoint *types.Endpoint, selector *types.LabelSelector) bool {
	// 检查等值匹配
	if len(selector.MatchLabels) > 0 {
		for key, expectedValue := range selector.MatchLabels {
			actualValue, exists := endpoint.Labels[key]
			if !exists || actualValue != expectedValue {
				return false
			}
		}
	}

	// 检查表达式匹配
	if len(selector.MatchExpressions) > 0 {
		for _, expr := range selector.MatchExpressions {
			if !s.matchesLabelExpression(endpoint, &expr) {
				return false
			}
		}
	}

	return true
}

// matchesLabelExpression 检查端点是否匹配标签表达式
func (s *EndpointSelector) matchesLabelExpression(endpoint *types.Endpoint, expr *types.LabelSelectorRequirement) bool {
	endpointValue, exists := endpoint.Labels[expr.Key]

	switch expr.Operator {
	case types.LabelSelectorOpIn:
		if !exists {
			return false
		}
		return s.valueInList(endpointValue, expr.Values)

	case types.LabelSelectorOpNotIn:
		if !exists {
			return true // 如果标签不存在，则认为不在列表中
		}
		return !s.valueInList(endpointValue, expr.Values)

	case types.LabelSelectorOpExists:
		return exists

	case types.LabelSelectorOpDoesNotExist:
		return !exists

	default:
		return false
	}
}

// valueInList 检查值是否在列表中
func (s *EndpointSelector) valueInList(value string, list []string) bool {
	for _, item := range list {
		if value == item {
			return true
		}
	}
	return false
}

// TODO: 后期可恢复的已删除方法 (如果需要扩展功能)
// - getEndpointStatusString() - 状态约束
// - applyQuantityConstraints() - 数量约束  
// - prioritizePreferredEndpoints() - 首选端点

// ValidateConstraints 验证约束条件的有效性 (简化版本)
func (s *EndpointSelector) ValidateConstraints(constraints *types.ServiceConstraints) error {
	if constraints == nil {
		return nil
	}

	// 验证标签选择器
	if constraints.LabelSelector != nil {
		if err := s.validateLabelSelector(constraints.LabelSelector); err != nil {
			return fmt.Errorf("invalid label selector: %w", err)
		}
	}

	// TODO: 后期可添加的验证项目
	// - 数量约束验证 (MinEndpoints, MaxEndpoints)
	// - 状态约束验证 (AllowedStatuses)
	// - 端点ID约束验证 (ExcludeEndpoints, PreferredEndpoints)

	return nil
}

// validateLabelSelector 验证标签选择器
func (s *EndpointSelector) validateLabelSelector(selector *types.LabelSelector) error {
	// 验证表达式
	for i, expr := range selector.MatchExpressions {
		if err := s.validateLabelExpression(&expr); err != nil {
			return fmt.Errorf("invalid expression at index %d: %w", i, err)
		}
	}

	return nil
}

// validateLabelExpression 验证标签表达式
func (s *EndpointSelector) validateLabelExpression(expr *types.LabelSelectorRequirement) error {
	// 验证键
	if strings.TrimSpace(expr.Key) == "" {
		return fmt.Errorf("key cannot be empty")
	}

	// 验证操作符
	validOperators := map[types.LabelSelectorOperator]bool{
		types.LabelSelectorOpIn:           true,
		types.LabelSelectorOpNotIn:        true,
		types.LabelSelectorOpExists:       true,
		types.LabelSelectorOpDoesNotExist: true,
	}

	if !validOperators[expr.Operator] {
		return fmt.Errorf("invalid operator: %s", expr.Operator)
	}

	// 验证值列表
	switch expr.Operator {
	case types.LabelSelectorOpIn, types.LabelSelectorOpNotIn:
		if len(expr.Values) == 0 {
			return fmt.Errorf("values cannot be empty for operator %s", expr.Operator)
		}
	case types.LabelSelectorOpExists, types.LabelSelectorOpDoesNotExist:
		if len(expr.Values) > 0 {
			return fmt.Errorf("values must be empty for operator %s", expr.Operator)
		}
	}

	return nil
}

// ParseLabelSelector 解析标签选择器字符串（简化版本）
func (s *EndpointSelector) ParseLabelSelector(selectorStr string) (*types.LabelSelector, error) {
	if strings.TrimSpace(selectorStr) == "" {
		return nil, nil
	}

	selector := &types.LabelSelector{
		MatchLabels: make(map[string]string),
	}

	// 简单解析：支持 key=value,key2=value2 格式
	pairs := strings.Split(selectorStr, ",")
	for _, pair := range pairs {
		pair = strings.TrimSpace(pair)
		if pair == "" {
			continue
		}

		parts := strings.SplitN(pair, "=", 2)
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid label selector format: %s", pair)
		}

		key := strings.TrimSpace(parts[0])
		value := strings.TrimSpace(parts[1])

		if key == "" {
			return nil, fmt.Errorf("empty key in label selector: %s", pair)
		}

		selector.MatchLabels[key] = value
	}

	return selector, nil
}