package registry

import (
	"testing"
	"time"

	"github.com/outpostos/OutpostNet/pkg/types"
)

func TestEndpointSelector_SelectEndpoints(t *testing.T) {
	selector := NewEndpointSelector()

	// Create test endpoints
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
		expected    []string // Expected endpoint IDs
		expectError bool
	}{
		{
			name:        "no constraints",
			constraints: nil,
			expected:    []string{"ep1", "ep2", "ep3", "ep4"},
		},
		{
			name: "healthy only",
			constraints: &types.ServiceConstraints{
				HealthyOnly: true,
			},
			expected: []string{"ep1", "ep2", "ep4"},
		},
		{
			name: "label selector match labels",
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
			name: "label selector In operation",
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
			name: "label selector NotIn operation",
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
			name: "label selector Exists operation",
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
			name: "combined constraints",
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
			expected: []string{"ep1", "ep2"}, // healthy + app=web + env=prod
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := selector.SelectEndpoints(endpoints, tt.constraints)

			if tt.expectError {
				if err == nil {
					t.Errorf("expected error but got none")
				}
				return
			}

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if len(result) != len(tt.expected) {
				t.Errorf("expected %d endpoints, got %d", len(tt.expected), len(result))
			}

			// Check if result endpoint IDs match expected
			resultIDs := make(map[string]bool)
			for _, ep := range result {
				resultIDs[ep.ID] = true
			}

			for _, expectedID := range tt.expected {
				if !resultIDs[expectedID] {
					t.Errorf("missing expected endpoint %s", expectedID)
				}
			}
		})
	}
}

func TestEndpointSelector_ValidateConstraints(t *testing.T) {
	selector := NewEndpointSelector()

	tests := []struct {
		name        string
		constraints *types.ServiceConstraints
		expectError bool
	}{
		{
			name:        "nil constraints",
			constraints: nil,
			expectError: false,
		},
		{
			name: "valid label selector",
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
			name: "invalid label selector empty key",
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
			name: "invalid label selector In operation no values",
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
			err := selector.ValidateConstraints(tt.constraints)

			if tt.expectError && err == nil {
				t.Error("expected error but got none")
			}

			if !tt.expectError && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}

func TestEndpointSelector_ParseLabelSelector(t *testing.T) {
	selector := NewEndpointSelector()

	tests := []struct {
		name         string
		selectorStr  string
		expectedKeys []string
		expectError  bool
	}{
		{
			name:         "empty selector",
			selectorStr:  "",
			expectedKeys: nil,
			expectError:  false,
		},
		{
			name:         "single label",
			selectorStr:  "app=web",
			expectedKeys: []string{"app"},
			expectError:  false,
		},
		{
			name:         "multiple labels",
			selectorStr:  "app=web,version=v1",
			expectedKeys: []string{"app", "version"},
			expectError:  false,
		},
		{
			name:        "invalid format",
			selectorStr: "invalid-format",
			expectError: true,
		},
		{
			name:        "empty key",
			selectorStr: "=value",
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := selector.ParseLabelSelector(tt.selectorStr)

			if tt.expectError {
				if err == nil {
					t.Error("expected error but got none")
				}
				return
			}

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if tt.expectedKeys == nil {
				if result != nil {
					t.Error("expected nil result")
				}
				return
			}

			if result == nil {
				t.Fatal("expected result but got nil")
			}

			if len(result.MatchLabels) != len(tt.expectedKeys) {
				t.Errorf("expected %d labels, got %d", len(tt.expectedKeys), len(result.MatchLabels))
			}

			for _, key := range tt.expectedKeys {
				if _, exists := result.MatchLabels[key]; !exists {
					t.Errorf("missing expected label key: %s", key)
				}
			}
		})
	}
}