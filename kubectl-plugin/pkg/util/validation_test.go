package util

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValidateResourceQuantity(t *testing.T) {
	tests := []struct {
		value   string
		name    string
		wantErr bool
	}{
		{"500m", "cpu", false},
		{"-500m", "cpu", true},
		{"aaa", "cpu", true},
		{"10Gi", "memory", false},
		{"bbb", "memory", true},
		{"", "memory", true},
		{"", "ephemeral-storage", true},
		{"100Gi", "head-ephemeral-storage", false},
		{"-100Gi", "worker-ephemeral-storage", true},
	}

	for _, tt := range tests {
		t.Run(tt.value, func(t *testing.T) {
			err := ValidateResourceQuantity(tt.value, tt.name)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateResourceQuantity() = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestValidateTPU(t *testing.T) {
	tests := map[string]struct {
		nodeSelector map[string]string
		tpu          string
		numOfHosts   *int32
		wantErr      string
	}{
		"empty TPU without node selectors is valid": {
			tpu:          "",
			numOfHosts:   new(int32(1)),
			nodeSelector: map[string]string{},
		},
		"0 TPU without node selectors is valid": {
			tpu:          "0",
			numOfHosts:   new(int32(1)),
			nodeSelector: map[string]string{},
		},
		"1 TPU without node selectors is invalid": {
			tpu:          "1",
			numOfHosts:   new(int32(1)),
			nodeSelector: map[string]string{},
			wantErr:      NodeSelectorGKETPUAccelerator + " is not set",
		},
		"1 TPU without TPU topology node selector is invalid": {
			tpu:          "1",
			numOfHosts:   new(int32(1)),
			nodeSelector: map[string]string{NodeSelectorGKETPUAccelerator: "tpu-v5-lite-podslice"},
			wantErr:      NodeSelectorGKETPUTopology + " is not set",
		},
		"1 TPU without TPU accelerator node selector is invalid": {
			tpu:          "1",
			numOfHosts:   new(int32(1)),
			nodeSelector: map[string]string{NodeSelectorGKETPUTopology: "1x1"},
			wantErr:      NodeSelectorGKETPUAccelerator + " is not set",
		},
		"1 TPU with 0 numOfHosts is invalid": {
			tpu:          "1",
			numOfHosts:   new(int32(0)),
			nodeSelector: map[string]string{NodeSelectorGKETPUAccelerator: "tpu-v5-lite-podslice", NodeSelectorGKETPUTopology: "1x1"},
			wantErr:      "--num-of-hosts cannot be 0 when using TPU",
		},
		"fractional millivalue TPU is invalid": {
			tpu:          "500m",
			numOfHosts:   new(int32(1)),
			nodeSelector: map[string]string{NodeSelectorGKETPUAccelerator: "tpu-v5-lite-podslice", NodeSelectorGKETPUTopology: "1x1"},
			wantErr:      "--worker-tpu must be a whole integer",
		},
		"decimal TPU is invalid": {
			tpu:          "1.5",
			numOfHosts:   new(int32(1)),
			nodeSelector: map[string]string{NodeSelectorGKETPUAccelerator: "tpu-v5-lite-podslice", NodeSelectorGKETPUTopology: "1x1"},
			wantErr:      "--worker-tpu must be a whole integer",
		},
		"unsupported accelerator is invalid": {
			tpu:          "1",
			numOfHosts:   new(int32(1)),
			nodeSelector: map[string]string{NodeSelectorGKETPUAccelerator: "tpu-v2", NodeSelectorGKETPUTopology: "1x1"},
			wantErr:      `unsupported TPU accelerator "tpu-v2"`,
		},
		"unsupported 2D topology is invalid": {
			tpu:          "4",
			numOfHosts:   new(int32(1)),
			nodeSelector: map[string]string{NodeSelectorGKETPUAccelerator: "tpu-v6e-slice", NodeSelectorGKETPUTopology: "3x3"},
			wantErr:      `unsupported TPU topology "3x3"`,
		},
		"2D topology on 3D accelerator is invalid": {
			tpu:          "4",
			numOfHosts:   new(int32(1)),
			nodeSelector: map[string]string{NodeSelectorGKETPUAccelerator: "tpu-v4-podslice", NodeSelectorGKETPUTopology: "2x2"},
			wantErr:      "requires a 3D topology",
		},
		"v5e 1x1 with 1 TPU and 1 host is valid": {
			tpu:          "1",
			numOfHosts:   new(int32(1)),
			nodeSelector: map[string]string{NodeSelectorGKETPUAccelerator: "tpu-v5-lite-podslice", NodeSelectorGKETPUTopology: "1x1"},
		},
		"v5e 2x4 single-host 8 TPU is valid": {
			tpu:          "8",
			numOfHosts:   new(int32(1)),
			nodeSelector: map[string]string{NodeSelectorGKETPUAccelerator: "tpu-v5-lite-podslice", NodeSelectorGKETPUTopology: "2x4"},
		},
		"v5e 2x4 multi-host 4 TPU is valid": {
			tpu:          "4",
			numOfHosts:   new(int32(2)),
			nodeSelector: map[string]string{NodeSelectorGKETPUAccelerator: "tpu-v5-lite-podslice", NodeSelectorGKETPUTopology: "2x4"},
		},
		"v6e 4x4 with wrong numOfHosts is invalid": {
			tpu:          "4",
			numOfHosts:   new(int32(1)),
			nodeSelector: map[string]string{NodeSelectorGKETPUAccelerator: "tpu-v6e-slice", NodeSelectorGKETPUTopology: "4x4"},
			wantErr:      "--num-of-hosts must be 4",
		},
		"v6e 4x4 with 4 TPU and 4 hosts is valid": {
			tpu:          "4",
			numOfHosts:   new(int32(4)),
			nodeSelector: map[string]string{NodeSelectorGKETPUAccelerator: "tpu-v6e-slice", NodeSelectorGKETPUTopology: "4x4"},
		},
		"v6e 16x16 with 4 TPU and 64 hosts is valid": {
			tpu:          "4",
			numOfHosts:   new(int32(64)),
			nodeSelector: map[string]string{NodeSelectorGKETPUAccelerator: "tpu-v6e-slice", NodeSelectorGKETPUTopology: "16x16"},
		},
		"v5e 1x1 with 4 TPUs per host is invalid": {
			tpu:          "4",
			numOfHosts:   new(int32(1)),
			nodeSelector: map[string]string{NodeSelectorGKETPUAccelerator: "tpu-v5-lite-podslice", NodeSelectorGKETPUTopology: "1x1"},
			wantErr:      "4 TPUs per host is not valid",
		},
		"v4 2x2x1 single-host is valid": {
			tpu:          "4",
			numOfHosts:   new(int32(1)),
			nodeSelector: map[string]string{NodeSelectorGKETPUAccelerator: "tpu-v4-podslice", NodeSelectorGKETPUTopology: "2x2x1"},
		},
		"v4 2x2x2 multi-host is valid": {
			tpu:          "4",
			numOfHosts:   new(int32(2)),
			nodeSelector: map[string]string{NodeSelectorGKETPUAccelerator: "tpu-v4-podslice", NodeSelectorGKETPUTopology: "2x2x2"},
		},
		"v4 2x2x6 is not a supported small topology": {
			tpu:          "4",
			numOfHosts:   new(int32(6)),
			nodeSelector: map[string]string{NodeSelectorGKETPUAccelerator: "tpu-v4-podslice", NodeSelectorGKETPUTopology: "2x2x6"},
			wantErr:      `unsupported TPU topology "2x2x6"`,
		},
		"v4 16x16x16 superpod is valid": {
			tpu:          "4",
			numOfHosts:   new(int32(1024)),
			nodeSelector: map[string]string{NodeSelectorGKETPUAccelerator: "tpu-v4-podslice", NodeSelectorGKETPUTopology: "16x16x16"},
		},
		"v5e device 1x1 is valid": {
			tpu:          "1",
			numOfHosts:   new(int32(1)),
			nodeSelector: map[string]string{NodeSelectorGKETPUAccelerator: "tpu-v5-lite-device", NodeSelectorGKETPUTopology: "1x1"},
		},
		"v5e device 2x4 is valid": {
			tpu:          "8",
			numOfHosts:   new(int32(1)),
			nodeSelector: map[string]string{NodeSelectorGKETPUAccelerator: "tpu-v5-lite-device", NodeSelectorGKETPUTopology: "2x4"},
		},
		"v4 device 2x2 is valid": {
			tpu:          "4",
			numOfHosts:   new(int32(1)),
			nodeSelector: map[string]string{NodeSelectorGKETPUAccelerator: "tpu-v4-lite-device", NodeSelectorGKETPUTopology: "2x2"},
		},
		"tpu7x 1x1x1 single-chip is valid": {
			tpu:          "1",
			numOfHosts:   new(int32(1)),
			nodeSelector: map[string]string{NodeSelectorGKETPUAccelerator: "tpu7x", NodeSelectorGKETPUTopology: "1x1x1"},
		},
		"tpu7x 1x1x1 with 4 TPUs per host is invalid": {
			tpu:          "4",
			numOfHosts:   new(int32(1)),
			nodeSelector: map[string]string{NodeSelectorGKETPUAccelerator: "tpu7x", NodeSelectorGKETPUTopology: "1x1x1"},
			wantErr:      "4 TPUs per host is not valid",
		},
		"tpu7x 4x4x4 is valid": {
			tpu:          "4",
			numOfHosts:   new(int32(16)),
			nodeSelector: map[string]string{NodeSelectorGKETPUAccelerator: "tpu7x", NodeSelectorGKETPUTopology: "4x4x4"},
		},
		"tpu7x 4x4x8 custom topology is valid": {
			tpu:          "4",
			numOfHosts:   new(int32(32)),
			nodeSelector: map[string]string{NodeSelectorGKETPUAccelerator: "tpu7x", NodeSelectorGKETPUTopology: "4x4x8"},
		},
		"tpu7x 8x4x4 violates A <= B <= C for large topologies": {
			tpu:          "4",
			numOfHosts:   new(int32(32)),
			nodeSelector: map[string]string{NodeSelectorGKETPUAccelerator: "tpu7x", NodeSelectorGKETPUTopology: "8x4x4"},
			wantErr:      `unsupported TPU topology "8x4x4"`,
		},
		"tpu7x 16x16x16 is valid": {
			tpu:          "4",
			numOfHosts:   new(int32(1024)),
			nodeSelector: map[string]string{NodeSelectorGKETPUAccelerator: "tpu7x", NodeSelectorGKETPUTopology: "16x16x16"},
		},
		"tpu7x 16x16x36 max topology is valid": {
			tpu:          "4",
			numOfHosts:   new(int32(2304)),
			nodeSelector: map[string]string{NodeSelectorGKETPUAccelerator: "tpu7x", NodeSelectorGKETPUTopology: "16x16x36"},
		},
		"tpu7x 16x16x40 exceeds max topology": {
			tpu:          "4",
			numOfHosts:   new(int32(2560)),
			nodeSelector: map[string]string{NodeSelectorGKETPUAccelerator: "tpu7x", NodeSelectorGKETPUTopology: "16x16x40"},
			wantErr:      `unsupported TPU topology "16x16x40"`,
		},
		"v3-device 2x2 is valid": {
			tpu:          "4",
			numOfHosts:   new(int32(1)),
			nodeSelector: map[string]string{NodeSelectorGKETPUAccelerator: "tpu-v3-device", NodeSelectorGKETPUTopology: "2x2"},
		},
		"v3-device 2x4 is valid": {
			tpu:          "8",
			numOfHosts:   new(int32(1)),
			nodeSelector: map[string]string{NodeSelectorGKETPUAccelerator: "tpu-v3-device", NodeSelectorGKETPUTopology: "2x4"},
		},
		"nil numOfHosts defaults to 1 and is valid for single-host": {
			tpu:          "1",
			nodeSelector: map[string]string{NodeSelectorGKETPUAccelerator: "tpu-v5-lite-podslice", NodeSelectorGKETPUTopology: "1x1"},
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			err := ValidateTPU(&tt.tpu, tt.numOfHosts, tt.nodeSelector)
			if tt.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, tt.wantErr)
			if strings.Contains(tt.wantErr, "unsupported") || strings.Contains(tt.wantErr, "is not set") || strings.Contains(tt.wantErr, "--num-of-hosts must be") {
				require.ErrorContains(t, err, tpuDocURL)
			}
		})
	}
}

func TestParseTPUTopology(t *testing.T) {
	tests := map[string]struct {
		topology string
		want     []int
		wantErr  bool
	}{
		"2D":          {topology: "4x4", want: []int{4, 4}},
		"3D":          {topology: "2x2x1", want: []int{2, 2, 1}},
		"empty":       {topology: "", wantErr: true},
		"one dim":     {topology: "4", wantErr: true},
		"four dims":   {topology: "2x2x2x2", wantErr: true},
		"non-numeric": {topology: "2xN", wantErr: true},
		"zero dim":    {topology: "2x0", wantErr: true},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			got, err := parseTPUTopology(tt.topology)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestIsValid3DTopology(t *testing.T) {
	const maxTPUs = 16 * 16 * 24
	tests := map[string]struct {
		dims []int
		want bool
	}{
		"2x2x1 single-host": {dims: []int{2, 2, 1}, want: true},
		"2x2x2":             {dims: []int{2, 2, 2}, want: true},
		"2x2x4":             {dims: []int{2, 2, 4}, want: true},
		"2x4x4":             {dims: []int{2, 4, 4}, want: true},
		"4x4x4":             {dims: []int{4, 4, 4}, want: true},
		"4x4x8":             {dims: []int{4, 4, 8}, want: true},
		"8x8x8":             {dims: []int{8, 8, 8}, want: true},
		"1x1x1":             {dims: []int{1, 1, 1}, want: false},
		"2x2x3":             {dims: []int{2, 2, 3}, want: false},
		"2x2x6":             {dims: []int{2, 2, 6}, want: false},
		"4x2x2 unordered":   {dims: []int{4, 2, 2}, want: false},
		"8x4x4 unordered":   {dims: []int{8, 4, 4}, want: false},
		"too large":         {dims: []int{16, 16, 32}, want: false},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			require.Equal(t, tt.want, isValid3DTopology(tt.dims, maxTPUs))
		})
	}
}
