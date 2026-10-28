package historyserver

import (
	"math"
	"strings"
	"testing"
)

const (
	shortenDebugStateTxt1 = `NodeManager:
Node ID: 32bbb200d7f0b1f13fc37160bbab6b0f39e53b2e4de044d2ed885cbb
Node name: 10.244.0.9
InitialConfigResources: {node:__internal_head__: 1, node:10.244.0.9: 1, object_store_memory: 1.43968e+09, memory: 1e+10}
ClusterLeaseManager:
========== Node: 32bbb200d7f0b1f13fc37160bbab6b0f39e53b2e4de044d2ed885cbb =================
Infeasible queue length: 0
Schedule queue length: 0
cluster_resource_scheduler state:
Local id: -2795944114624162604 Local resources: {"total":{memory: [100000000000000], node:__internal_head__: [10000], object_store_memory: [14396805120000], node:10.244.0.9: [10000]}}, "available": {memory: [100000000000000], node:__internal_head__: [10000], object_store_memory: [14396805120000], node:10.244.0.9: [10000]}}, "labels":{"ray.io/node-group":"headgroup","ray.io/node-id":"32bbb200d7f0b1f13fc37160bbab6b0f39e53b2e4de044d2ed885cbb",} is_draining: 0 is_idle: 1 Cluster resources`

	// Worker node with 4 CPU cores and 2 GPUs (multi-instance resources).
	// CPU: [10000, 10000, 10000, 10000] represents 4 cores × 1.0 each = 4.0 total.
	// GPU: [10000, 10000] represents 2 GPUs × 1.0 each = 2.0 total.
	workerDebugStateTxt = `NodeManager:
Node ID: aaaa1111bbbb2222cccc3333dddd4444eeee5555ffff6666
Node name: 10.244.0.10
InitialConfigResources: {CPU: 4, GPU: 2, memory: 1e+10, object_store_memory: 5e+09}
ClusterLeaseManager:
========== Node: aaaa1111bbbb2222cccc3333dddd4444eeee5555ffff6666 =================
Infeasible queue length: 0
Schedule queue length: 0
cluster_resource_scheduler state:
Local id: 1234567890 Local resources: {"total":{CPU: [10000, 10000, 10000, 10000], GPU: [10000, 10000], memory: [100000000000000], node:10.244.0.10: [10000], object_store_memory: [50000000000000]}}, "available": {CPU: [10000, 10000, 5000, 0], GPU: [10000, 0], memory: [80000000000000], node:10.244.0.10: [10000], object_store_memory: [50000000000000]}}, "labels":{"ray.io/node-group":"worker-group","ray.io/node-id":"aaaa1111bbbb2222cccc3333dddd4444eeee5555ffff6666",} is_draining: 0 is_idle: 0 Cluster resources`

	shortenDebugStateTxt2 = `NodeManager:
Node ID: 5a21dad564fb61db93fa736a04d75eb306d57cf817091308f39ab30a
Node name: 10.244.0.4
InitialConfigResources: {object_store_memory: 1.45457e+09, node:__internal_head__: 1, node:10.244.0.4: 1, memory: 1e+10}
ClusterLeaseManager:
========== Node: 5a21dad564fb61db93fa736a04d75eb306d57cf817091308f39ab30a =================
Infeasible queue length: 0
Schedule queue length: 0
Grant queue length: 0
num_waiting_for_resource: 0
num_waiting_for_plasma_memory: 0
num_waiting_for_remote_node_resources: 0
num_worker_not_started_by_job_config_not_exist: 0
num_worker_not_started_by_registration_timeout: 0
num_tasks_waiting_for_workers: 0
num_cancelled_leases: 0
cluster_resource_scheduler state:
Local id: 4655599106509980361 Local resources: {"total":{node:10.244.0.4: [10000], memory: [100000000000000], node:__internal_head__: [10000], object_store_memory: [14545711100000]}}, "available": {node:10.244.0.4: [10000], memory: [100000000000000], node:__internal_head__: [10000], object_store_memory: [14545711100000]}}, "labels":{"ray.io/node-group":"headgroup","ray.io/node-id":"5a21dad564fb61db93fa736a04d75eb306d57cf817091308f39ab30a",} is_draining: 0 is_idle: 1 Cluster resources (at most 20 nodes are shown): node id: 4655599106509980361{"total":{memory: 100000000000000, node:__internal_head__: 10000, object_store_memory: 14545711100000, node:10.244.0.4: 10000}}, "available": {object_store_memory: 14545711100000, node:10.244.0.4: 10000, memory: 100000000000000, node:__internal_head__: 10000}}, "labels":{"ray.io/node-group":"headgroup","ray.io/node-id":"5a21dad564fb61db93fa736a04d75eb306d57cf817091308f39ab30a",}, "is_draining": 0, "draining_deadline_timestamp_ms": -1} { "placement group locations": [], "node to bundles": []}`
)

func almostEqual(a, b, eps float64) bool {
	return math.Abs(a-b) <= eps
}

func TestParseDebugState(t *testing.T) {
	testcases := []struct {
		name              string
		content           string
		expectedID        string
		expectedNodeName  string
		expectedNodeGroup string
		expectedIsIdle    bool
		expectTotal       map[string]float64
		expectUsed        map[string]float64 // nil means expect all zeros
	}{
		{
			name:              "headgroup-short",
			content:           shortenDebugStateTxt1,
			expectedID:        "32bbb200d7f0b1f13fc37160bbab6b0f39e53b2e4de044d2ed885cbb",
			expectedNodeName:  "10.244.0.9",
			expectedNodeGroup: "headgroup",
			expectedIsIdle:    true,
			expectTotal: map[string]float64{
				"memory":              10000000000,
				"object_store_memory": 1439680512,
			},
		},
		{
			name:              "worker-with-multi-instance-cpu-gpu",
			content:           workerDebugStateTxt,
			expectedID:        "aaaa1111bbbb2222cccc3333dddd4444eeee5555ffff6666",
			expectedNodeName:  "10.244.0.10",
			expectedNodeGroup: "worker-group",
			expectedIsIdle:    false,
			expectTotal: map[string]float64{
				"CPU":                 4.0, // [10000, 10000, 10000, 10000] → 40000 / 10000
				"GPU":                 2.0, // [10000, 10000] → 20000 / 10000
				"memory":              10000000000,
				"object_store_memory": 5000000000,
			},
			expectUsed: map[string]float64{
				"CPU":    1.5,          // total 4.0 - available 2.5
				"GPU":    1.0,          // total 2.0 - available 1.0
				"memory": 2000000000.0, // total 10G - available 8G
			},
		},
		{
			name:              "headgroup-long-with-cluster-resources",
			content:           shortenDebugStateTxt2,
			expectedID:        "5a21dad564fb61db93fa736a04d75eb306d57cf817091308f39ab30a",
			expectedNodeName:  "10.244.0.4",
			expectedNodeGroup: "headgroup",
			expectedIsIdle:    true,
			expectTotal: map[string]float64{
				"memory":              10000000000,
				"object_store_memory": 1454571110,
			},
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			state, err := ParseDebugState(strings.NewReader(tc.content))
			if err != nil {
				t.Fatalf("ParseDebugState() error = %v", err)
			}

			if state.NodeID != tc.expectedID {
				t.Errorf("NodeID = %q, want %q", state.NodeID, tc.expectedID)
			}
			if state.NodeName != tc.expectedNodeName {
				t.Errorf("NodeName = %q, want %q", state.NodeName, tc.expectedNodeName)
			}
			if state.NodeGroup != tc.expectedNodeGroup {
				t.Errorf("NodeGroup = %q, want %q", state.NodeGroup, tc.expectedNodeGroup)
			}
			if state.IsIdle != tc.expectedIsIdle {
				t.Errorf("IsIdle = %v, want %v", state.IsIdle, tc.expectedIsIdle)
			}

			// totals
			for k, want := range tc.expectTotal {
				got, ok := state.Total[k]
				if !ok {
					t.Fatalf("Total[%q] missing", k)
				}
				if !almostEqual(got, want, 1e-6) {
					t.Errorf("Total[%q] = %f, want %f", k, got, want)
				}
			}

			used := state.GetUsedResources()
			if tc.expectUsed != nil {
				for k, want := range tc.expectUsed {
					got := used[k]
					if !almostEqual(got, want, 1e-6) {
						t.Errorf("Used[%q] = %f, want %f", k, got, want)
					}
				}
			} else {
				// When expectUsed is nil, all used values should be 0
				for k := range tc.expectTotal {
					if got := used[k]; got != 0 {
						t.Errorf("Used[%q] = %f, want 0", k, got)
					}
				}
			}
		})
	}
}
