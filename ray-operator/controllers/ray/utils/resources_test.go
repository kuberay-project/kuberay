package utils

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIsGPUResourceKey(t *testing.T) {
	tests := []struct {
		name        string
		resourceKey string
		expected    bool
	}{
		{
			name:        "nvidia gpu",
			resourceKey: "nvidia.com/gpu",
			expected:    true,
		},
		{
			name:        "amd gpu",
			resourceKey: "amd.com/gpu",
			expected:    true,
		},
		{
			name:        "nvidia MIG",
			resourceKey: "nvidia.com/mig-12g.128gb",
			expected:    true,
		},
		{
			name:        "nvidia MIG bad format",
			resourceKey: "nvidia.com/gpu-mig-12g.128gb",
			expected:    false,
		},
		{
			name:        "cpu",
			resourceKey: "cpu",
			expected:    false,
		},
		{
			name:        "memory",
			resourceKey: "memory",
			expected:    false,
		},
		{
			name:        "huawei Ascend910B",
			resourceKey: "huawei.com/Ascend910B",
			expected:    false,
		},
		{
			name:        "huawei npu",
			resourceKey: "huawei.com/npu",
			expected:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := IsGPUResourceKey(tt.resourceKey)
			assert.Equal(t, tt.expected, result)
		})
	}
}
