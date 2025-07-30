package registry

import (
	"fmt"
	"slices"

	"github.com/outpostos/OutpostNet/pkg/types"
)

// ConstraintFilter 约束条件过滤器
type ConstraintFilter struct{}

// NewConstraintFilter 创建新的约束条件过滤器
func NewConstraintFilter() *ConstraintFilter {
	return &ConstraintFilter{}
}

// FilterEndpoints 根据约束条件过滤端点
func (cf *ConstraintFilter) FilterEndpoints(endpoints []*types.Endpoint, constraints *types.ServiceConstraints) ([]*types.Endpoint, error) {
	if constraints == nil {
		return endpoints, nil
	}

	var filtered []*types.Endpoint

	for _, ep := range endpoints {
		if cf.matchesConstraints(ep, constraints) {
			filtered = append(filtered, ep)
		}
	}

	// 应用数量约束
	filtered = cf.applyQuantityConstraints(filtered, constraints)

	// 简化版本：无需最小端点数验证
	// TODO: 后期如需要可添加 validateMinEndpoints

	return filtered, nil
}

// matchesConstraints 检查端点是否满足所有约束条件 (简化版本)
func (cf *ConstraintFilter) matchesConstraints(endpoint *types.Endpoint, constraints *types.ServiceConstraints) bool {
	// 健康状态约束 - 最基础的约束
	if constraints.HealthyOnly && !endpoint.Status.Ready {
		return false
	}

	// 标签选择器约束
	if constraints.LabelSelector != nil && !cf.matchesLabelSelector(endpoint, constraints.LabelSelector) {
		return false
	}

	return true
}

// TODO: 后期可添加的辅助方法
// - matchesRequiredLabels() - 简单标签匹配
// - validateMinEndpoints() - 最小端点数验证  
// - validateQuantityConstraints() - 数量约束验证

// matchesLabelSelector 检查端点是否匹配标签选择器
func (cf *ConstraintFilter) matchesLabelSelector(endpoint *types.Endpoint, selector *types.LabelSelector) bool {
	// 检查 MatchLabels (内联实现)
	for key, expectedValue := range selector.MatchLabels {
		actualValue, exists := endpoint.Labels[key]
		if !exists || actualValue != expectedValue {
			return false
		}
	}

	// 检查 MatchExpressions
	for _, expr := range selector.MatchExpressions {
		if !cf.matchesLabelExpression(endpoint, expr) {
			return false
		}
	}

	return true
}

// matchesLabelExpression 检查端点是否匹配标签表达式
func (cf *ConstraintFilter) matchesLabelExpression(endpoint *types.Endpoint, expr types.LabelSelectorRequirement) bool {
	value, exists := endpoint.Labels[expr.Key]

	switch expr.Operator {
	case types.LabelSelectorOpExists:
		return exists

	case types.LabelSelectorOpDoesNotExist:
		return !exists

	case types.LabelSelectorOpIn:
		if !exists {
			return false
		}
		return slices.Contains(expr.Values, value)

	case types.LabelSelectorOpNotIn:
		if !exists {
			return true // 如果key不存在，则认为值不在列表中
		}
		return !slices.Contains(expr.Values, value)

	default:
		return false
	}
}

// applyQuantityConstraints 应用数量约束 (简化版本 - 当前无数量约束)
func (cf *ConstraintFilter) applyQuantityConstraints(endpoints []*types.Endpoint, constraints *types.ServiceConstraints) []*types.Endpoint {
	// 简化版本：直接返回过滤后的端点
	// TODO: 后期可在此处添加MinEndpoints、MaxEndpoints、PreferredEndpoints等功能
	return endpoints
}

// TODO: 后期可恢复的已删除方法
//
// validateMinEndpoints() - 最小端点数约束验证
// applyQuantityConstraints() - 数量约束和首选端点处理

// ValidateConstraints 验证约束条件的有效性 (简化版本)
func (cf *ConstraintFilter) ValidateConstraints(constraints *types.ServiceConstraints) error {
	if constraints == nil {
		return nil
	}

	// 验证标签选择器
	if constraints.LabelSelector != nil {
		if err := cf.validateLabelSelector(constraints.LabelSelector); err != nil {
			return fmt.Errorf("invalid label selector: %w", err)
		}
	}

	// 简化版本：只验证核心功能
	// TODO: 后期可添加数量约束、状态约束等验证

	return nil
}

// validateLabelSelector 验证标签选择器
func (cf *ConstraintFilter) validateLabelSelector(selector *types.LabelSelector) error {
	// 验证 MatchExpressions
	for _, expr := range selector.MatchExpressions {
		if expr.Key == "" {
			return fmt.Errorf("label selector expression key cannot be empty")
		}

		switch expr.Operator {
		case types.LabelSelectorOpIn, types.LabelSelectorOpNotIn:
			if len(expr.Values) == 0 {
				return fmt.Errorf("label selector expression with operator %s must have values", expr.Operator)
			}

		case types.LabelSelectorOpExists, types.LabelSelectorOpDoesNotExist:
			if len(expr.Values) > 0 {
				return fmt.Errorf("label selector expression with operator %s must not have values", expr.Operator)
			}

		default:
			return fmt.Errorf("invalid label selector operator: %s", expr.Operator)
		}
	}

	return nil
}