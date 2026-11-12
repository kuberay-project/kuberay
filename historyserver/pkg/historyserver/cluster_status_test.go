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

// A placement group with two 1-CPU bundles is reserved on a 4-CPU node. One task runs in the
// placement group and one task runs outside of it.
const debugStateWithPlacementGroup = `cluster_resource_scheduler state:
Local id: 1234567890 Local resources: {"total":{CPU: [10000, 10000, 10000, 10000], CPU_group_a1b2c3: [20000], CPU_group_0_a1b2c3: [10000], CPU_group_1_a1b2c3: [10000], bundle_group_a1b2c3: [20000000], bundle_group_0_a1b2c3: [10000000], bundle_group_1_a1b2c3: [10000000], memory: [107374182400000], node:10.244.0.10: [10000], object_store_memory: [50000000000000]}}, "available": {CPU: [0, 0, 10000, 0], CPU_group_a1b2c3: [10000], CPU_group_0_a1b2c3: [0], CPU_group_1_a1b2c3: [10000], bundle_group_a1b2c3: [20000000], bundle_group_0_a1b2c3: [10000000], bundle_group_1_a1b2c3: [10000000], memory: [85899345920000], node:10.244.0.10: [10000], object_store_memory: [50000000000000]}}, "labels":{"ray.io/node-group":"worker-group",} is_draining: 0 is_idle: 0 Cluster resources
`

func TestFormatStatusPlacementGroupResources(t *testing.T) {
	state, err := ParseDebugState(strings.NewReader(debugStateWithPlacementGroup))
	if err != nil {
		t.Fatalf("ParseDebugState failed: %v", err)
	}
	builder := NewClusterStatusBuilder()
	builder.AddNodeFromDebugState(state)

	status := builder.FormatStatus()

	// Expected lines come from Ray's parse_usage for the same resources, which is what
	// `ray status` prints: bundle resources are hidden and CPU reports the placement group
	// reservation instead of counting it as used.
	expected := "Total Usage:\n" +
		" 2.0/4.0 CPU (1.0 used of 2.0 reserved in placement groups)\n" +
		" 2.00GiB/10.00GiB memory\n" +
		" 0B/4.66GiB object_store_memory\n"
	if !strings.Contains(status, expected) {
		t.Errorf("expected status to contain:\n%s\ngot:\n%s", expected, status)
	}
	if strings.Contains(status, "_group_") {
		t.Errorf("expected placement group bundle resources to be hidden, got:\n%s", status)
	}
}

func TestParsePlacementGroupResource(t *testing.T) {
	tests := []struct {
		resource  string
		name      string
		pgName    string
		countable bool
	}{
		{resource: "CPU", name: "CPU", pgName: "", countable: true},
		{resource: "CPU_group_a1b2c3", name: "CPU", pgName: "a1b2c3", countable: true},
		{resource: "CPU_group_0_a1b2c3", name: "CPU", pgName: "a1b2c3", countable: false},
		{resource: "custom_res_group_12_a1b2c3", name: "custom_res", pgName: "a1b2c3", countable: false},
		{resource: "bundle_group_a1b2c3", name: "bundle", pgName: "a1b2c3", countable: true},
	}
	for _, tc := range tests {
		t.Run(tc.resource, func(t *testing.T) {
			name, pgName, countable := parsePlacementGroupResource(tc.resource)
			if name != tc.name || pgName != tc.pgName || countable != tc.countable {
				t.Errorf("parsePlacementGroupResource(%q) = (%q, %q, %v), want (%q, %q, %v)",
					tc.resource, name, pgName, countable, tc.name, tc.pgName, tc.countable)
			}
		})
	}
}
