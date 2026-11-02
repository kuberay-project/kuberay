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
		{
			name:        "DRA implicit extended resource name, NVIDIA DeviceClass",
			resourceKey: "deviceclass.resource.kubernetes.io/gpu.nvidia.com",
			expected:    true,
		},
		{
			name:        "DRA implicit extended resource name, AMD DeviceClass",
			resourceKey: "deviceclass.resource.kubernetes.io/gpu.amd.com",
			expected:    true,
		},
		{
			name:        "DRA implicit extended resource name, example driver DeviceClass",
			resourceKey: "deviceclass.resource.kubernetes.io/gpu.example.com",
			expected:    true,
		},
		{
			name:        "DRA implicit extended resource name, DeviceClass named plainly gpu",
			resourceKey: "deviceclass.resource.kubernetes.io/gpu",
			expected:    true,
		},
		{
			name:        "DRA implicit extended resource name, NVIDIA MIG DeviceClass is not counted",
			resourceKey: "deviceclass.resource.kubernetes.io/mig.nvidia.com",
			expected:    false,
		},
		{
			name:        "DRA implicit extended resource name, gpu not at the start of the class name",
			resourceKey: "deviceclass.resource.kubernetes.io/nvidia-gpu.example.com",
			expected:    false,
		},
		{
			name:        "DRA implicit extended resource name, TPU DeviceClass",
			resourceKey: "deviceclass.resource.kubernetes.io/tpu.google.com",
			expected:    false,
		},
		{
			name:        "DRA implicit extended resource name, network DeviceClass",
			resourceKey: "deviceclass.resource.kubernetes.io/net.example.com",
			expected:    false,
		},
		{
			name:        "custom vendor gpu name matched by suffix",
			resourceKey: "example.com/gpu",
			expected:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := IsGPUResourceKey(tt.resourceKey)
			assert.Equal(t, tt.expected, result)
		})
	}
}
