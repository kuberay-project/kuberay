package historyserver

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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

func TestParseSessionTimestamp(t *testing.T) {
	want := time.Date(2026, 1, 20, 22, 55, 56, 0, time.UTC)
	for _, tc := range []struct {
		name    string
		session string
		want    time.Time
	}{
		{"standard", "session_2026-01-20_22-55-56_123456", want},
		{"with suffix", "session_2026-01-20_22-55-56_123456_1", want},
		{"timestamp only", "session_2026-01-20_22-55-56", want},
		{"empty", "", time.Time{}},
		{"missing prefix", "2026-01-20_22-55-56_123456", time.Time{}},
		{"missing time", "session_2026-01-20", time.Time{}},
		{"invalid date", "session_2026-02-30_22-55-56_123456", time.Time{}},
		{"invalid time", "session_2026-01-20_25-55-56_123456", time.Time{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, ParseSessionTimestamp(tc.session))
		})
	}
}

func TestGetLastTimestamp(t *testing.T) {
	early := time.Date(2026, 1, 20, 12, 0, 0, 0, time.UTC)
	late := early.Add(time.Hour)
	for _, tc := range []struct {
		name   string
		tasks  []types.Task
		actors []types.Actor
		want   time.Time
	}{
		{name: "no timestamps"},
		{name: "zero end times", tasks: []types.Task{{}}, actors: []types.Actor{{}}},
		{name: "tasks only unsorted", tasks: []types.Task{{EndTime: late}, {EndTime: early}, {}}, want: late},
		{name: "actors only unsorted", actors: []types.Actor{{EndTime: late}, {EndTime: early}, {}}, want: late},
		{name: "task latest", tasks: []types.Task{{EndTime: late}}, actors: []types.Actor{{EndTime: early}}, want: late},
		{name: "actor latest", tasks: []types.Task{{EndTime: early}}, actors: []types.Actor{{EndTime: late}}, want: late},
		{name: "equal timestamps", tasks: []types.Task{{EndTime: late}}, actors: []types.Actor{{EndTime: late}}, want: late},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, GetLastTimestamp(tc.tasks, tc.actors))
		})
	}
}

func TestIsPendingTaskState(t *testing.T) {
	for _, tc := range []struct {
		state types.TaskStatus
		want  bool
	}{
		{types.PENDING_ARGS_AVAIL, true},
		{types.PENDING_NODE_ASSIGNMENT, true},
		{types.PENDING_OBJ_STORE_MEM_AVAIL, true},
		{types.PENDING_ARGS_FETCH, true},
		{types.NIL, false},
		{types.SUBMITTED_TO_WORKER, false},
		{types.PENDING_ACTOR_TASK_ARGS_FETCH, false},
		{types.PENDING_ACTOR_TASK_ORDERING_OR_CONCURRENCY, false},
		{types.RUNNING, false},
		{types.RUNNING_IN_RAY_GET, false},
		{types.RUNNING_IN_RAY_WAIT, false},
		{types.FINISHED, false},
		{types.FAILED, false},
		{types.GETTING_AND_PINNING_ARGS, false},
		{"", false},
		{"UNKNOWN", false},
	} {
		t.Run(string(tc.state), func(t *testing.T) {
			assert.Equal(t, tc.want, isPendingTaskState(tc.state))
		})
	}
}

func TestIsPendingActorState(t *testing.T) {
	for _, tc := range []struct {
		state types.StateType
		want  bool
	}{
		{types.PENDING_CREATION, true},
		{types.DEPENDENCIES_UNREADY, true},
		{types.ALIVE, false},
		{types.RESTARTING, false},
		{types.DEAD, false},
		{"", false},
		{"UNKNOWN", false},
	} {
		t.Run(string(tc.state), func(t *testing.T) {
			assert.Equal(t, tc.want, isPendingActorState(tc.state))
		})
	}
}

func TestResourceKey(t *testing.T) {
	for _, tc := range []struct {
		name      string
		resources map[string]float64
		want      string
	}{
		{name: "nil"},
		{name: "empty", resources: map[string]float64{}},
		{"sorted keys", map[string]float64{"GPU": 0.5, "CPU": 2}, "CPU:2.0000,GPU:0.5000"},
		{"same shape", map[string]float64{"CPU": 2, "GPU": 0.5}, "CPU:2.0000,GPU:0.5000"},
		{"zero", map[string]float64{"CPU": 0}, "CPU:0.0000"},
		{"fractional precision", map[string]float64{"CPU": 0.12346}, "CPU:0.1235"},
		{"smallest fraction", map[string]float64{"GPU": 0.0001}, "GPU:0.0001"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, resourceKey(tc.resources))
		})
	}
}

func TestAddPendingDemands(t *testing.T) {
	cpu := map[string]float64{"CPU": 1}
	gpu := map[string]float64{"CPU": 2, "GPU": 0.5}
	for _, tc := range []struct {
		name   string
		tasks  []types.Task
		actors []types.Actor
		want   []ResourceDemand
	}{
		{name: "empty inputs"},
		{
			name: "ignore nonpending states and empty resources",
			tasks: []types.Task{
				{State: types.RUNNING, RequiredResources: cpu},
				{State: types.PENDING_NODE_ASSIGNMENT},
				{State: types.PENDING_ARGS_AVAIL, RequiredResources: map[string]float64{}},
			},
			actors: []types.Actor{
				{State: types.ALIVE, RequiredResources: cpu},
				{State: types.PENDING_CREATION},
				{State: types.DEPENDENCIES_UNREADY, RequiredResources: map[string]float64{}},
			},
		},
		{
			name: "group tasks by resource shape",
			tasks: []types.Task{
				{State: types.PENDING_ARGS_AVAIL, RequiredResources: cpu},
				{State: types.PENDING_NODE_ASSIGNMENT, RequiredResources: cpu},
				{State: types.PENDING_OBJ_STORE_MEM_AVAIL, RequiredResources: gpu},
				{State: types.PENDING_ARGS_FETCH, RequiredResources: map[string]float64{"CPU": 2}},
			},
			want: []ResourceDemand{{Resources: cpu, Count: 2}, {Resources: gpu, Count: 1}, {Resources: map[string]float64{"CPU": 2}, Count: 1}},
		},
		{
			name: "group actors by resource shape",
			actors: []types.Actor{
				{State: types.PENDING_CREATION, RequiredResources: cpu},
				{State: types.DEPENDENCIES_UNREADY, RequiredResources: cpu},
				{State: types.PENDING_CREATION, RequiredResources: gpu},
			},
			want: []ResourceDemand{{Resources: cpu, Count: 2}, {Resources: gpu, Count: 1}},
		},
		{
			name: "merge task and actor demands",
			tasks: []types.Task{
				{State: types.PENDING_NODE_ASSIGNMENT, RequiredResources: cpu},
				{State: types.PENDING_ARGS_AVAIL, RequiredResources: cpu},
			},
			actors: []types.Actor{
				{State: types.PENDING_CREATION, RequiredResources: cpu},
				{State: types.DEPENDENCIES_UNREADY, RequiredResources: gpu},
			},
			want: []ResourceDemand{{Resources: cpu, Count: 3}, {Resources: gpu, Count: 1}},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			builder := NewClusterStatusBuilder()
			builder.AddPendingDemandsFromTasks(tc.tasks)
			builder.AddPendingDemandsFromActors(tc.actors)
			assert.ElementsMatch(t, tc.want, builder.PendingDemands)
		})
	}
}

func TestAddNodeFromDebugState(t *testing.T) {
	for _, tc := range []struct {
		name         string
		states       []*NodeDebugState
		active, idle map[string]int
		total, used  map[string]float64
	}{
		{"nil state", []*NodeDebugState{nil}, map[string]int{}, map[string]int{}, map[string]float64{}, map[string]float64{}},
		{"default group", []*NodeDebugState{{}}, map[string]int{"default": 1}, map[string]int{}, map[string]float64{}, map[string]float64{}},
		{
			name: "accumulate nodes and resources",
			states: []*NodeDebugState{
				{NodeGroup: "workers", Total: map[string]float64{"CPU": 4}, Available: map[string]float64{"CPU": 1}},
				{NodeGroup: "workers", Total: map[string]float64{"CPU": 2, "GPU": 1}, Available: map[string]float64{"CPU": 1}},
				{NodeGroup: "head", IsIdle: true, Total: map[string]float64{"CPU": 1}, Available: map[string]float64{"CPU": 1}},
				nil,
			},
			active: map[string]int{"workers": 2}, idle: map[string]int{"head": 1},
			total: map[string]float64{"CPU": 7, "GPU": 1}, used: map[string]float64{"CPU": 4, "GPU": 1},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			builder := NewClusterStatusBuilder()
			for _, state := range tc.states {
				builder.AddNodeFromDebugState(state)
			}
			assert.Equal(t, tc.active, builder.ActiveNodes)
			assert.Equal(t, tc.idle, builder.IdleNodes)
			assert.Equal(t, tc.total, builder.TotalResources)
			assert.Equal(t, tc.used, builder.UsedResources)
		})
	}
}

func TestAddFailedNodesFromNodes(t *testing.T) {
	ts := time.Date(2026, 1, 20, 12, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name        string
		transitions []types.NodeStateTransition
		labels      map[string]string
		wantType    string
		wantTime    time.Time
	}{
		{name: "no transitions"},
		{name: "alive", transitions: []types.NodeStateTransition{{State: types.NODE_ALIVE}}},
		{name: "expected termination", transitions: []types.NodeStateTransition{{State: types.NODE_DEAD, DeathInfo: &types.NodeDeathInfo{Reason: types.EXPECTED_TERMINATION}}}},
		{name: "idle drain", transitions: []types.NodeStateTransition{{State: types.NODE_DEAD, DeathInfo: &types.NodeDeathInfo{Reason: types.AUTOSCALER_DRAIN_IDLE}}}},
		{name: "no death info or labels", transitions: []types.NodeStateTransition{{State: types.NODE_DEAD, Timestamp: ts}}, wantType: "node1", wantTime: ts},
		{name: "empty group", transitions: []types.NodeStateTransition{{State: types.NODE_DEAD, Timestamp: ts}}, labels: map[string]string{"ray.io/node-group": ""}, wantType: "node1", wantTime: ts},
		{name: "preempted", transitions: []types.NodeStateTransition{{State: types.NODE_DEAD, Timestamp: ts, DeathInfo: &types.NodeDeathInfo{Reason: types.AUTOSCALER_DRAIN_PREEMPTED}}}, wantType: "node1", wantTime: ts},
		{
			name: "latest failure per node",
			transitions: []types.NodeStateTransition{
				{State: types.NODE_DEAD, Timestamp: ts.Add(-time.Hour)},
				{State: types.NODE_ALIVE, Timestamp: ts.Add(-time.Minute)},
				{State: types.NODE_DEAD, Timestamp: ts, DeathInfo: &types.NodeDeathInfo{Reason: types.UNEXPECTED_TERMINATION}},
			},
			labels: map[string]string{"ray.io/node-group": "workers"}, wantType: "workers", wantTime: ts,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			builder := NewClusterStatusBuilder()
			builder.AddFailedNodesFromNodes(map[string]types.Node{
				"node1": {NodeID: "node1", NodeIPAddress: "10.0.0.1", InstanceID: "instance1", Labels: tc.labels, StateTransitions: tc.transitions},
			})
			if tc.wantType == "" {
				assert.Empty(t, builder.FailedNodes)
				return
			}
			assert.Equal(t, []FailedNode{{NodeID: "node1", NodeIPAddress: "10.0.0.1", InstanceID: "instance1", NodeType: tc.wantType, Timestamp: tc.wantTime}}, builder.FailedNodes)
		})
	}
}

func TestFailedNodesSortedAndCapped(t *testing.T) {
	ts := time.Date(2026, 1, 20, 12, 0, 0, 0, time.UTC)
	for _, count := range []int{0, 2, 20, 21} {
		t.Run(fmt.Sprintf("%d nodes", count), func(t *testing.T) {
			var nodes map[string]types.Node
			if count > 0 {
				nodes = make(map[string]types.Node)
			}
			for i := range count {
				id := fmt.Sprintf("node%d", i)
				nodes[id] = types.Node{NodeID: id, StateTransitions: []types.NodeStateTransition{{State: types.NODE_DEAD, Timestamp: ts.Add(time.Duration(i) * time.Minute)}}}
			}
			builder := NewClusterStatusBuilder()
			builder.AddFailedNodesFromNodes(nodes)
			require.Len(t, builder.FailedNodes, min(count, 20))
			for i, node := range builder.FailedNodes {
				assert.Equal(t, fmt.Sprintf("node%d", count-1-i), node.NodeID)
				assert.Equal(t, ts.Add(time.Duration(count-1-i)*time.Minute), node.Timestamp)
			}
		})
	}
}

func TestFormatPythonFloat(t *testing.T) {
	for _, tc := range []struct {
		value float64
		want  string
	}{
		{0, "0.0"},
		{1, "1.0"},
		{-2, "-2.0"},
		{0.1, "0.1"},
		{0.25, "0.25"},
		{0.0001, "0.0001"},
		{1073741824, "1073741824.0"},
	} {
		t.Run(tc.want, func(t *testing.T) {
			assert.Equal(t, tc.want, formatPythonFloat(tc.value))
		})
	}
}

func TestFormatResourceValue(t *testing.T) {
	for _, tc := range []struct {
		name     string
		resource string
		value    float64
		want     string
	}{
		{"integer", "CPU", 2, "2.0"},
		{"fraction", "GPU", 0.25, "0.25"},
		{"zero", "CPU", 0, "0.0"},
		{"memory", "memory", 1073741824, "1.00GiB"},
		{"object store", "object_store_memory", 1572864, "1.50MiB"},
		{"case insensitive", "MEMORY", 1024, "1.00KiB"},
		{"bytes", "memory", 512, "512B"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, formatResourceValue(tc.resource, tc.value))
		})
	}
}

func TestFormatResourceMapForDisplay(t *testing.T) {
	for _, tc := range []struct {
		name      string
		resources map[string]float64
		want      string
	}{
		{"nil", nil, "{}"},
		{"empty", map[string]float64{}, "{}"},
		{"sorted resources with raw memory", map[string]float64{"memory": 1073741824, "GPU": 0.5, "CPU": 1}, "{'CPU': 1.0, 'GPU': 0.5, 'memory': 1073741824.0}"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, formatResourceMapForDisplay(tc.resources))
		})
	}
}

func TestFormatStatusEmpty(t *testing.T) {
	const want = `======== Autoscaler status: time unknown ========
Node status
-------------------------------------------------
Active:
 (no active nodes)
Idle:
 (no idle nodes)
Pending:
 (unavailable in history server)
Recent failures:
 (no failures)

Resources
-------------------------------------------------
Total Usage:
 (no resources)

From request_resources:
 (unavailable in history server)
Pending Demands:
 (no resource demands)`
	assert.Equal(t, want, NewClusterStatusBuilder().FormatStatus())
}

func TestFormatStatusPopulated(t *testing.T) {
	builder := NewClusterStatusBuilder()
	builder.ActiveNodes = map[string]int{"z-workers": 2, "a-workers": 1}
	builder.IdleNodes = map[string]int{"z-idle": 1, "a-idle": 3}
	builder.FailedNodes = []FailedNode{
		{NodeType: "worker-v2", InstanceID: "instance1", NodeIPAddress: "10.0.0.1"},
		{NodeType: "worker-v1", NodeIPAddress: "10.0.0.2"},
	}
	builder.TotalResources = map[string]float64{"GPU": 1, "CPU": 4, "accelerator_type:A100": 1, "memory": 1073741824}
	builder.UsedResources = map[string]float64{"CPU": 1, "memory": 536870912}
	builder.PendingDemands = []ResourceDemand{{Resources: map[string]float64{"GPU": 0.5, "CPU": 1}, Count: 3}}
	status := builder.FormatStatus()
	for _, want := range []string{
		"Active:\n 1 a-workers\n 2 z-workers\n",
		"Idle:\n 3 a-idle\n 1 z-idle\n",
		" worker-v2: NodeTerminated (instance_id: instance1)\n",
		" worker-v1: NodeTerminated (ip: 10.0.0.2)\n",
		"Total Usage:\n 1.0/4.0 CPU\n 0.0/1.0 GPU\n 512.00MiB/1.00GiB memory\n",
		"Pending Demands:\n {'CPU': 1.0, 'GPU': 0.5}: 3+ pending tasks/actors\n",
	} {
		assert.Contains(t, status, want)
	}
	assert.NotContains(t, status, "accelerator_type:")
	assert.NotContains(t, status, "10.0.0.1")
}
