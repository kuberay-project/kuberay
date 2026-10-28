package util

import (
	"fmt"
	"testing"
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

func TestValidateTPUNodeSelector(t *testing.T) {
	tests := map[string]struct {
		nodeSelector map[string]string
		tpu          string
		numOfHosts   int32
		wantErr      bool
	}{
		"empty TPU without node selectors is valid": {
			tpu:          "",
			numOfHosts:   1,
			nodeSelector: map[string]string{},
			wantErr:      false,
		},
		"0 TPU without node selectors is valid": {
			tpu:          "0",
			numOfHosts:   1,
			nodeSelector: map[string]string{},
			wantErr:      false,
		},
		"1 TPU without node selectors is invalid": {
			tpu:          "1",
			numOfHosts:   1,
			nodeSelector: map[string]string{},
			wantErr:      true,
		},
		"1 TPU without TPU topology node selector is invalid": {
			tpu:          "1",
			numOfHosts:   1,
			nodeSelector: map[string]string{NodeSelectorGKETPUAccelerator: "v2"},
			wantErr:      true,
		},
		"1 TPU without TPU accelerator node selector is invalid": {
			tpu:          "1",
			numOfHosts:   1,
			nodeSelector: map[string]string{NodeSelectorGKETPUTopology: "topology-1"},
			wantErr:      true,
		},
		"1 TPU with 0 numOfHosts is invalid": {
			tpu:          "1",
			numOfHosts:   0,
			nodeSelector: map[string]string{NodeSelectorGKETPUAccelerator: "v2", NodeSelectorGKETPUTopology: "topology-1"},
			wantErr:      true,
		},
		"1 TPU with required node selectors and numOfHosts >= 1 is valid": {
			tpu:          "1",
			numOfHosts:   1,
			nodeSelector: map[string]string{NodeSelectorGKETPUAccelerator: "v2", NodeSelectorGKETPUTopology: "topology-1"},
			wantErr:      false,
		},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("%v", tt.nodeSelector), func(t *testing.T) {
			err := ValidateTPU(&tt.tpu, &tt.numOfHosts, tt.nodeSelector)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateTPUNodeSelector() = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
