package historyserver

import (
	"strings"
	"testing"
	"time"

	"github.com/ray-project/kuberay/historyserver/pkg/eventserver/types"
)

func TestFormatStatus(t *testing.T) {
	builder := NewClusterStatusBuilder()
	builder.Timestamp = time.Date(2026, 1, 20, 22, 55, 56, 762825000, time.UTC)

	debugState := &NodeDebugState{
		NodeID:    "abc123",
		NodeGroup: "headgroup",
		IsIdle:    true,
		Total: map[string]float64{
			"memory":              10000000000, // 10GB
			"object_store_memory": 1450000000,  // ~1.35GB
		},
		Available: map[string]float64{
			"memory":              10000000000, // All available (0 used)
			"object_store_memory": 1450000000,
		},
	}
	builder.AddNodeFromDebugState(debugState)

	nodes := map[string]types.Node{
		"node1": {
			NodeID:        "node1",
			NodeIPAddress: "10.0.0.5",
			Labels:        map[string]string{"ray.io/node-group": "worker-group"},
			StateTransitions: []types.NodeStateTransition{
				{State: types.NODE_ALIVE, Timestamp: time.Date(2026, 1, 20, 22, 0, 0, 0, time.UTC)},
				{
					State:     types.NODE_DEAD,
					Timestamp: time.Date(2026, 1, 20, 22, 50, 0, 0, time.UTC),
					DeathInfo: &types.NodeDeathInfo{
						Reason:        types.UNEXPECTED_TERMINATION,
						ReasonMessage: "raylet died",
					},
				},
			},
		},
	}
	builder.AddFailedNodesFromNodes(nodes)

	status := builder.FormatStatus()

	if !strings.Contains(status, "======== Autoscaler status: 2026-01-20 22:55:56.762825") {
		t.Errorf("Expected status to contain timestamp header")
	}

	if !strings.Contains(status, "Idle:\n 1 headgroup") {
		t.Errorf("Expected status to show 1 idle headgroup node, got:\n%s", status)
	}

	if !strings.Contains(status, "(no active nodes)") {
		t.Errorf("Expected status to show no active nodes")
	}

	if !strings.Contains(status, "memory") {
		t.Errorf("Expected status to contain memory resources")
	}

	expectedV1 := " worker-group: NodeTerminated (ip: 10.0.0.5)" // Matches Ray v1 format: " {node_type}: NodeTerminated (ip: {ip})"
	if !strings.Contains(status, expectedV1) {
		t.Errorf("Expected status to contain %q, got:\n%s", expectedV1, status)
	}

	if strings.Contains(status, "(no failures)") {
		t.Errorf("Expected failed nodes, but got '(no failures)'")
	}

	t.Logf("Generated status:\n%s", status)
}
