package loadbalancer

import (
	"testing"
)

func TestNewLoadBalancer(t *testing.T) {
	tests := []struct {
		algorithm   string
		expectError bool
		expectedType string
	}{
		{"round_robin", false, "round_robin"},
		{"", false, "round_robin"}, // 默认算法
		{"random", false, "random"},
		{"least_conn", false, "least_conn"},
		{"invalid", true, ""},
	}

	for _, tt := range tests {
		t.Run(tt.algorithm, func(t *testing.T) {
			lb, err := NewLoadBalancer(tt.algorithm)
			
			if tt.expectError {
				if err == nil {
					t.Error("expected error but got none")
				}
				return
			}
			
			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}
			
			if lb.Name() != tt.expectedType {
				t.Errorf("expected type %s, got %s", tt.expectedType, lb.Name())
			}
		})
	}
}

func TestGetSupportedAlgorithms(t *testing.T) {
	algorithms := GetSupportedAlgorithms()
	expected := []string{"round_robin", "random", "least_conn"}
	
	if len(algorithms) != len(expected) {
		t.Fatalf("expected %d algorithms, got %d", len(expected), len(algorithms))
	}
	
	for i, alg := range expected {
		if algorithms[i] != alg {
			t.Errorf("expected algorithm %s at index %d, got %s", alg, i, algorithms[i])
		}
	}
}

func TestIsValidAlgorithm(t *testing.T) {
	tests := []struct {
		algorithm string
		valid     bool
	}{
		{"round_robin", true},
		{"random", true},
		{"least_conn", true},
		{"", true}, // 空字符串是有效的，默认为 round_robin
		{"invalid", false},
		{"ROUND_ROBIN", false}, // 大小写敏感
	}
	
	for _, tt := range tests {
		t.Run(tt.algorithm, func(t *testing.T) {
			result := IsValidAlgorithm(tt.algorithm)
			if result != tt.valid {
				t.Errorf("expected %v for algorithm '%s', got %v", tt.valid, tt.algorithm, result)
			}
		})
	}
}