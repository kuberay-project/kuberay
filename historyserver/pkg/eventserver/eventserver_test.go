package eventserver

import (
	"bytes"
	"compress/gzip"
	"context"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ray-project/kuberay/historyserver/pkg/eventserver/types"
	"github.com/ray-project/kuberay/historyserver/pkg/utils"
)

func makeTaskEventMap(taskName, nodeID, taskID string, attempt int) map[string]any {
	return map[string]any{
		"eventType": string(types.TASK_DEFINITION_EVENT),
		"taskDefinitionEvent": map[string]any{
			"taskId":      taskID,
			"taskName":    taskName,
			"nodeId":      nodeID,
			"taskAttempt": attempt,
		},
	}
}

func TestStoreEvent(t *testing.T) {
	// IDs follow Ray's ID specification:
	// ref: https://github.com/ray-project/ray/blob/f229d5376eb87b09a3fa0b991323450de84df890/src/ray/design_docs/id_specification.md
	// Sizes: JobID=4B, ActorID=12B unique+JobID (16B), TaskID=8B unique+ActorID (24B), NodeID/WorkerID=28B
	const (
		testJobID        = "aaaabbbb"                                                 // 4B
		testActorID      = "aaaabbbb1234aaaabbbb1234" + testJobID                     // 12B unique + JobID
		testTaskID1      = "ccccdddd5678cccc" + testActorID                           // 8B unique + ActorID
		testTaskID2      = "ccccdddd9012dddd" + testActorID                           // 8B unique + ActorID
		testNodeID1      = "eeeeffff1234eeeeffff1234eeeeffff1234eeeeffff1234eeeeffff" // 28B
		testNodeID2      = "eeeeffff9012eeeeffff9012eeeeffff9012eeeeffff9012eeeeffff" // 28B
		testUpperNodeID1 = "AAAABBBB1234AAAABBBB1234AAAABBBB1234AAAABBBB1234AAAABBBB" // 28B uppercase
		testLowerNodeID1 = "aaaabbbb1234aaaabbbb1234aaaabbbb1234aaaabbbb1234aaaabbbb" // 28B lowercase
		testUpperTaskID1 = "AAAABBBB5678AAAABBBB5678AAAABBBB5678AAAABBBB5678"         // 24B uppercase
		testLowerTaskID1 = "aaaabbbb5678aaaabbbb5678aaaabbbb5678aaaabbbb5678"         // 24B lowercase

		// Base64 and Hex pairs to verify normalization of base64 to hex
		testBase64ID1 = "AgAAAA=="
		testBase64ID2 = "AwAAAA=="
		testHexID1    = "02000000" // hex for base64 "AgAAAA=="
		testHexID2    = "03000000" // hex for base64 "AwAAAA=="

		testClusterName = "cluster1"
		testTaskName1   = "Name_12345"
		testTaskName2   = "Name_54321"
	)

	initialTask := types.Task{
		TaskID:      testTaskID1,
		TaskName:    testTaskName1,
		NodeID:      testNodeID1,
		TaskAttempt: 0,
	}
	tests := []struct {
		name              string
		initialState      *types.ClusterTaskMap
		eventMap          map[string]any
		wantErr           bool
		wantClusterCount  int
		wantTaskInCluster string       // Cluster to check for the task
		wantTaskID        string       // TaskID to check
		wantTasks         []types.Task // Expected tasks (all attempts), nil if not applicable
	}{
		{
			name: "unsupported event type",
			initialState: &types.ClusterTaskMap{
				ClusterTaskMap: make(map[string]*types.TaskMap),
			},
			eventMap: map[string]any{
				"eventType": "UNKNOWN_TYPE",
			},
			wantErr:          false,
			wantClusterCount: 0,
		},
		{
			name: "task event - new cluster and new task",
			initialState: &types.ClusterTaskMap{
				ClusterTaskMap: make(map[string]*types.TaskMap),
			},
			eventMap:          makeTaskEventMap(testTaskName1, testNodeID1, testTaskID1, 0),
			wantErr:           false,
			wantClusterCount:  1,
			wantTaskInCluster: testClusterName,
			wantTaskID:        testTaskID1,
			wantTasks: []types.Task{
				{
					TaskID:      testTaskID1,
					TaskName:    testTaskName1,
					NodeID:      testNodeID1,
					TaskAttempt: 0,
				},
			},
		},
		{
			name: "task event - existing cluster, new task",
			initialState: &types.ClusterTaskMap{
				ClusterTaskMap: map[string]*types.TaskMap{
					testClusterName: types.NewTaskMap(),
				},
			},
			eventMap:          makeTaskEventMap(testTaskName2, testNodeID2, testTaskID2, 1),
			wantErr:           false,
			wantClusterCount:  1,
			wantTaskInCluster: testClusterName,
			wantTaskID:        testTaskID2,
			wantTasks: []types.Task{
				{
					TaskID:      testTaskID2,
					TaskName:    testTaskName2,
					NodeID:      testNodeID2,
					TaskAttempt: 1,
				},
			},
		},
		{
			name: "task event - existing cluster and existing task with new attempt",
			initialState: &types.ClusterTaskMap{
				ClusterTaskMap: map[string]*types.TaskMap{
					testClusterName: {
						TaskMap: map[string][]types.Task{
							testTaskID1: {initialTask},
						},
					},
				},
			},
			eventMap:          makeTaskEventMap(testTaskName1, testNodeID1, testTaskID1, 2),
			wantErr:           false,
			wantClusterCount:  1,
			wantTaskInCluster: testClusterName,
			wantTaskID:        testTaskID1,
			// Now expects BOTH attempts to be stored
			wantTasks: []types.Task{
				{
					TaskID:      testTaskID1,
					TaskName:    testTaskName1,
					NodeID:      testNodeID1,
					TaskAttempt: 0,
				},
				{
					TaskID:      testTaskID1,
					TaskName:    testTaskName1,
					NodeID:      testNodeID1,
					TaskAttempt: 2,
				},
			},
		},
		{
			name: "task event - base64 ID is normalized to hex",
			initialState: &types.ClusterTaskMap{
				ClusterTaskMap: make(map[string]*types.TaskMap),
			},
			eventMap:          makeTaskEventMap(testTaskName1, testBase64ID1, testBase64ID2, 0),
			wantErr:           false,
			wantClusterCount:  1,
			wantTaskInCluster: testClusterName,
			wantTaskID:        testHexID2,
			wantTasks: []types.Task{
				{
					TaskID:      testHexID2,
					TaskName:    testTaskName1,
					NodeID:      testHexID1,
					TaskAttempt: 0,
				},
			},
		},
		{
			name: "task event - uppercase hex IDs normalized to lowercase",
			initialState: &types.ClusterTaskMap{
				ClusterTaskMap: make(map[string]*types.TaskMap),
			},
			eventMap:          makeTaskEventMap(testTaskName1, testUpperNodeID1, testUpperTaskID1, 0),
			wantErr:           false,
			wantClusterCount:  1,
			wantTaskInCluster: testClusterName,
			wantTaskID:        testLowerTaskID1,
			wantTasks: []types.Task{
				{
					TaskID:      testLowerTaskID1,
					TaskName:    testTaskName1,
					NodeID:      testLowerNodeID1,
					TaskAttempt: 0,
				},
			},
		},
		{
			name: "task event - lowercase hex IDs are returned as-is",
			initialState: &types.ClusterTaskMap{
				ClusterTaskMap: make(map[string]*types.TaskMap),
			},
			eventMap:          makeTaskEventMap(testTaskName1, testLowerNodeID1, testLowerTaskID1, 0),
			wantErr:           false,
			wantClusterCount:  1,
			wantTaskInCluster: testClusterName,
			wantTaskID:        testLowerTaskID1,
			wantTasks: []types.Task{
				{
					TaskID:      testLowerTaskID1,
					TaskName:    testTaskName1,
					NodeID:      testLowerNodeID1,
					TaskAttempt: 0,
				},
			},
		},
		{
			name: "task event - missing taskDefinitionEvent",
			initialState: &types.ClusterTaskMap{
				ClusterTaskMap: make(map[string]*types.TaskMap),
			},
			eventMap: map[string]any{
				"eventType": string(types.TASK_DEFINITION_EVENT),
			},
			wantErr: true,
		},
		{
			name: "task event - taskDefinitionEvent wrong type",
			initialState: &types.ClusterTaskMap{
				ClusterTaskMap: make(map[string]*types.TaskMap),
			},
			eventMap: map[string]any{
				"eventType":           string(types.TASK_DEFINITION_EVENT),
				"taskDefinitionEvent": "not a map",
			},
			wantErr: true, // Marshal will fail
		},
		{
			name: "task event - invalid task structure",
			initialState: &types.ClusterTaskMap{
				ClusterTaskMap: make(map[string]*types.TaskMap),
			},
			eventMap: map[string]any{
				"eventType": string(types.TASK_DEFINITION_EVENT),
				"taskDefinitionEvent": map[string]any{
					"taskId":      123, // Should be string
					"taskAttempt": 0,
				},
			},
			wantErr: true, // Unmarshal will fail
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := &EventHandler{
				ClusterTaskMap: tt.initialState,
			}
			if h.ClusterTaskMap == nil {
				h.ClusterTaskMap = &types.ClusterTaskMap{
					ClusterTaskMap: make(map[string]*types.TaskMap),
				}
			}

			err := h.storeEvent(testClusterName, tt.eventMap)

			if (err != nil) != tt.wantErr {
				t.Fatalf("storeEvent() error = %v, wantErr %v", err, tt.wantErr)
			}
			if err != nil {
				return
			}

			gotClusterCount := len(h.ClusterTaskMap.ClusterTaskMap)

			if gotClusterCount != tt.wantClusterCount {
				t.Errorf("storeEvent() resulted in %d clusters, want %d", gotClusterCount, tt.wantClusterCount)
			}

			if tt.wantTasks != nil {
				clusterObj, clusterExists := h.ClusterTaskMap.ClusterTaskMap[tt.wantTaskInCluster]

				if !clusterExists {
					t.Fatalf("storeEvent() cluster %s not found", tt.wantTaskInCluster)
				}

				clusterObj.Lock()
				defer clusterObj.Unlock()
				gotTasks, taskExists := clusterObj.TaskMap[tt.wantTaskID]
				if !taskExists {
					t.Fatalf("storeEvent() task %s not found in cluster %s", tt.wantTaskID, tt.wantTaskInCluster)
				}

				if diff := cmp.Diff(tt.wantTasks, gotTasks); diff != "" {
					t.Errorf("storeEvent() tasks mismatch (-want +got):\n%s", diff)
				}
			}
		})
	}
}

// TestTaskLifecycleEventDeduplication verifies that duplicate events are correctly filtered
// and out-of-order events are properly sorted
func TestTaskLifecycleEventDeduplication(t *testing.T) {
	// IDs follow Ray's ID spec; see TestStoreEvent for rationale.
	const (
		testJobID       = "aaaabbbb"                                                 // 4B
		testActorID     = "aaaabbbb1234aaaabbbb1234" + testJobID                     // 12B unique + JobID
		testTaskID      = "ccccdddd5678cccc" + testActorID                           // 8B unique + ActorID
		testNodeID      = "eeeeffff1234eeeeffff1234eeeeffff1234eeeeffff1234eeeeffff" // 28B
		testWorkerID    = "eeeeffff0000eeeeffff0000eeeeffff0000eeeeffff0000eeeeffff" // 28B
		testClusterName = "cluster1"
	)

	// Helper to create a StateEvent
	makeStateEvent := func(state types.TaskStatus, timestampNano int64) types.TaskStateTransition {
		return types.TaskStateTransition{
			State:     state,
			Timestamp: time.Unix(0, timestampNano),
		}
	}

	// Helper to create a TASK_LIFECYCLE_EVENT map
	makeLifecycleEvent := func(transitions []map[string]any) map[string]any {
		// Convert []map[string]any to []any for proper type assertion in storeEvent
		transitionsAny := make([]any, len(transitions))
		for i, t := range transitions {
			transitionsAny[i] = t
		}
		return map[string]any{
			"eventType": string(types.TASK_LIFECYCLE_EVENT),
			"taskLifecycleEvent": map[string]any{
				"taskId":           testTaskID,
				"taskAttempt":      float64(0),
				"stateTransitions": transitionsAny,
				"nodeId":           testNodeID,
				"workerId":         testWorkerID,
			},
		}
	}

	// Helper to create state transition
	makeTransition := func(state string, timestampNano int64) map[string]any {
		return map[string]any{
			"state":     state,
			"timestamp": time.Unix(0, timestampNano).Format(time.RFC3339Nano),
		}
	}

	tests := []struct {
		name           string
		existingEvents []types.TaskStateTransition // Events already in the task
		newTransitions []map[string]any            // New transitions to process
		wantEvents     []types.TaskStateTransition // Expected final events (sorted by timestamp)
		wantState      types.TaskStatus            // Expected final state
	}{
		{
			name: "Scenario 1: Normal deduplication - same events processed twice",
			existingEvents: []types.TaskStateTransition{
				makeStateEvent(types.PENDING_NODE_ASSIGNMENT, 1000),
				makeStateEvent(types.RUNNING, 2000),
			},
			newTransitions: []map[string]any{
				makeTransition("PENDING_NODE_ASSIGNMENT", 1000), // Duplicate
				makeTransition("RUNNING", 2000),                 // Duplicate
			},
			wantEvents: []types.TaskStateTransition{
				makeStateEvent(types.PENDING_NODE_ASSIGNMENT, 1000),
				makeStateEvent(types.RUNNING, 2000),
			},
			wantState: types.RUNNING,
		},
		{
			name: "Scenario 2: Out-of-order events - B(t=2) arrives after A(t=1), C(t=3)",
			existingEvents: []types.TaskStateTransition{
				makeStateEvent(types.PENDING_NODE_ASSIGNMENT, 1000), // A
				makeStateEvent(types.FINISHED, 3000),                // C
			},
			newTransitions: []map[string]any{
				makeTransition("RUNNING", 2000), // B - should be inserted in the middle
			},
			wantEvents: []types.TaskStateTransition{
				makeStateEvent(types.PENDING_NODE_ASSIGNMENT, 1000), // A
				makeStateEvent(types.RUNNING, 2000),                 // B - now in correct position
				makeStateEvent(types.FINISHED, 3000),                // C
			},
			wantState: types.FINISHED,
		},
		{
			name: "Scenario 3: Same timestamp, different states - both should be kept",
			existingEvents: []types.TaskStateTransition{
				makeStateEvent(types.PENDING_NODE_ASSIGNMENT, 1000),
			},
			newTransitions: []map[string]any{
				makeTransition("RUNNING", 1000), // Same timestamp, different state
			},
			wantEvents: []types.TaskStateTransition{
				makeStateEvent(types.PENDING_NODE_ASSIGNMENT, 1000),
				makeStateEvent(types.RUNNING, 1000),
			},
			wantState: types.RUNNING, // Last after sort (order of same timestamp is stable)
		},
		{
			name: "Scenario 4: Exact duplicate event - only one should remain",
			existingEvents: []types.TaskStateTransition{
				makeStateEvent(types.RUNNING, 1000),
			},
			newTransitions: []map[string]any{
				makeTransition("RUNNING", 1000), // Exact duplicate
				makeTransition("RUNNING", 1000), // Another duplicate
			},
			wantEvents: []types.TaskStateTransition{
				makeStateEvent(types.RUNNING, 1000), // Only one
			},
			wantState: types.RUNNING,
		},
		{
			name: "Scenario 5: Partial overlap - existing [A,B], new [B,C] -> result [A,B,C]",
			existingEvents: []types.TaskStateTransition{
				makeStateEvent(types.PENDING_NODE_ASSIGNMENT, 1000), // A
				makeStateEvent(types.RUNNING, 2000),                 // B
			},
			newTransitions: []map[string]any{
				makeTransition("RUNNING", 2000),  // B - duplicate
				makeTransition("FINISHED", 3000), // C - new
			},
			wantEvents: []types.TaskStateTransition{
				makeStateEvent(types.PENDING_NODE_ASSIGNMENT, 1000), // A
				makeStateEvent(types.RUNNING, 2000),                 // B
				makeStateEvent(types.FINISHED, 3000),                // C
			},
			wantState: types.FINISHED,
		},
		{
			name:           "Scenario 6: Empty initial events - add new events",
			existingEvents: []types.TaskStateTransition{},
			newTransitions: []map[string]any{
				makeTransition("PENDING_NODE_ASSIGNMENT", 1000),
				makeTransition("RUNNING", 2000),
			},
			wantEvents: []types.TaskStateTransition{
				makeStateEvent(types.PENDING_NODE_ASSIGNMENT, 1000),
				makeStateEvent(types.RUNNING, 2000),
			},
			wantState: types.RUNNING,
		},
		{
			name: "Scenario 7: Multiple reprocessing cycles - events should not grow",
			existingEvents: []types.TaskStateTransition{
				makeStateEvent(types.PENDING_NODE_ASSIGNMENT, 1000),
				makeStateEvent(types.RUNNING, 2000),
				makeStateEvent(types.FINISHED, 3000),
			},
			newTransitions: []map[string]any{
				// Simulating reprocess of same file
				makeTransition("PENDING_NODE_ASSIGNMENT", 1000),
				makeTransition("RUNNING", 2000),
				makeTransition("FINISHED", 3000),
			},
			wantEvents: []types.TaskStateTransition{
				makeStateEvent(types.PENDING_NODE_ASSIGNMENT, 1000),
				makeStateEvent(types.RUNNING, 2000),
				makeStateEvent(types.FINISHED, 3000),
			},
			wantState: types.FINISHED,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := NewEventHandler(nil)

			// Pre-populate existing events if any
			if len(tt.existingEvents) > 0 {
				taskMap := h.ClusterTaskMap.GetOrCreateTaskMap(testClusterName)
				taskMap.CreateOrMergeAttempt(testTaskID, 0, func(task *types.Task) {
					task.TaskID = testTaskID
					task.StateTransitions = tt.existingEvents
					if len(tt.existingEvents) > 0 {
						task.State = tt.existingEvents[len(tt.existingEvents)-1].State
					}
				})
			}

			// Process the lifecycle event
			eventMap := makeLifecycleEvent(tt.newTransitions)
			err := h.storeEvent(testClusterName, eventMap)
			if err != nil {
				t.Fatalf("storeEvent() unexpected error: %v", err)
			}

			// Get the task and verify
			taskMap := h.ClusterTaskMap.GetOrCreateTaskMap(testClusterName)
			taskMap.Lock()
			defer taskMap.Unlock()

			tasks, exists := taskMap.TaskMap[testTaskID]
			if !exists || len(tasks) == 0 {
				t.Fatal("Task not found after processing")
			}

			task := tasks[0]

			// Verify events
			if diff := cmp.Diff(tt.wantEvents, task.StateTransitions); diff != "" {
				t.Errorf("Events mismatch (-want +got):\n%s", diff)
			}

			// Verify final state
			if task.State != tt.wantState {
				t.Errorf("State = %v, want %v", task.State, tt.wantState)
			}

			// Verify event count (important for deduplication)
			if len(task.StateTransitions) != len(tt.wantEvents) {
				t.Errorf("Event count = %d, want %d", len(task.StateTransitions), len(tt.wantEvents))
			}
		})
	}
}

func TestTaskLogInfoLifecycleReplay(t *testing.T) {
	const (
		clusterName = "cluster1"
		taskID      = "ccccdddd5678ccccaaaabbbb1234aaaabbbb1234aaaabbbb"
	)

	makeLogEvent := func(attempt int, taskLogInfo map[string]any) map[string]any {
		return map[string]any{
			"eventType": string(types.TASK_LIFECYCLE_EVENT),
			"taskLifecycleEvent": map[string]any{
				"taskId":           taskID,
				"taskAttempt":      attempt,
				"stateTransitions": []any{},
				"taskLogInfo":      taskLogInfo,
			},
		}
	}
	// Ray reports a task's log boundaries through two separate calls:
	// - RecordTaskLogStart sets only the file paths and start offsets
	// - RecordTaskLogEnd sets only the end offsets.
	// Ref: https://github.com/ray-project/ray/blob/d53df23caf6210f6bca58855607951d89c52d345/src/ray/core_worker/core_worker.cc#L4933-L4976
	start := makeLogEvent(0, map[string]any{
		"stdoutFile": "worker.out", "stderrFile": "worker.err",
		"stdoutStart": "0", "stderrStart": "7",
	})
	end := makeLogEvent(0, map[string]any{
		"stdoutEnd": "20", "stderrEnd": "30",
	})
	definition := makeTaskEventMap("bench_task", "", taskID, 0)
	stateWithoutLogInfo := map[string]any{
		"eventType": string(types.TASK_LIFECYCLE_EVENT),
		"taskLifecycleEvent": map[string]any{
			"taskId":      taskID,
			"taskAttempt": 0,
			"stateTransitions": []any{map[string]any{
				"state": "RUNNING", "timestamp": "2026-08-08T01:45:58.211147463Z",
			}},
		},
	}
	want := &types.TaskLogInfo{
		StdoutFile: "worker.out", StderrFile: "worker.err",
		StdoutStart: 0, StdoutEnd: 20, StderrStart: 7, StderrEnd: 30,
	}

	for _, tt := range []struct {
		name   string
		events []map[string]any
	}{
		{name: "start then end", events: []map[string]any{start, end, definition, stateWithoutLogInfo}},
		{name: "end then start", events: []map[string]any{end, start, definition, stateWithoutLogInfo}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			h := NewEventHandler(nil)
			for _, event := range tt.events {
				require.NoError(t, h.storeEvent(clusterName, event))
			}
			tasks := h.getTasks(clusterName)
			require.Len(t, tasks, 1)
			assert.Equal(t, want, tasks[0].TaskLogInfo)
			assert.Equal(t, "bench_task", tasks[0].TaskName)
			assert.Equal(t, types.RUNNING, tasks[0].State)
		})
	}

	t.Run("metadata-only update preserves lifecycle fields", func(t *testing.T) {
		h := NewEventHandler(nil)
		state := stateWithoutLogInfo["taskLifecycleEvent"].(map[string]any)
		state["jobId"] = "aaaabbbb"
		state["workerPid"] = 123
		state["actorReprName"] = "Counter.increment"
		require.NoError(t, h.storeEvent(clusterName, stateWithoutLogInfo))
		before := h.getTasks(clusterName)[0]

		require.NoError(t, h.storeEvent(clusterName, end))
		after := h.getTasks(clusterName)[0]
		assert.Equal(t, before.StateTransitions, after.StateTransitions)
		assert.Equal(t, before.State, after.State)
		assert.Equal(t, before.JobID, after.JobID)
		assert.Equal(t, before.WorkerPID, after.WorkerPID)
		assert.Equal(t, before.ActorReprName, after.ActorReprName)
		require.NotNil(t, after.TaskLogInfo)
		assert.Equal(t, int64(20), after.TaskLogInfo.StdoutEnd)
	})

	t.Run("task attempts remain isolated", func(t *testing.T) {
		h := NewEventHandler(nil)
		attempt1 := makeLogEvent(1, map[string]any{
			"stdoutFile": "attempt1.out", "stderrFile": "attempt1.err",
			"stdoutStart": "25", "stderrStart": "25",
		})
		require.NoError(t, h.storeEvent(clusterName, end))
		require.NoError(t, h.storeEvent(clusterName, attempt1))
		tasks := h.getTasks(clusterName)
		require.Len(t, tasks, 2)
		assert.Equal(t, int64(20), tasks[0].TaskLogInfo.StdoutEnd)
		assert.Equal(t, "attempt1.out", tasks[1].TaskLogInfo.StdoutFile)
		assert.Zero(t, tasks[1].TaskLogInfo.StdoutEnd)
	})

	h := NewEventHandler(nil)
	err := h.storeEvent(clusterName, map[string]any{
		"eventType": string(types.TASK_LIFECYCLE_EVENT),
		"taskLifecycleEvent": map[string]any{
			"taskId": taskID, "taskAttempt": 0, "stateTransitions": []any{},
		},
	})
	require.Error(t, err, "an empty lifecycle event without TaskLogInfo must still be rejected")
}

func TestStoreRay256TaskLogInfoJSONL(t *testing.T) {
	// A task whose execution spans Ray's ray_events_report_interval_ms (1s default)
	// exports its log start and log end as two lifecycle events
	const capturedEvents = `
{"eventType":"TASK_LIFECYCLE_EVENT","taskLifecycleEvent":{"jobId":"AwAAAA==","nodeId":"","stateTransitions":[],"taskAttempt":0,"taskId":"UYPGNywAXvb///////////////8DAAAA","taskLogInfo":{"stdoutFile":"/tmp/ray/session_2026-08-08_01-45-50_123456_1/logs/worker-058747b57347ad6c698b59511de4e64fc37e4c7ad2b033eb7cdf7cf1-03000000-215.out","stderrFile":"/tmp/ray/session_2026-08-08_01-45-50_123456_1/logs/worker-058747b57347ad6c698b59511de4e64fc37e4c7ad2b033eb7cdf7cf1-03000000-215.err","stdoutStart":"512","stderrStart":"0"},"workerId":"","workerPid":0}}
{"eventType":"TASK_DEFINITION_EVENT","taskDefinitionEvent":{"jobId":"AwAAAA==","language":"PYTHON","requiredResources":{"CPU":0.2},"taskAttempt":0,"taskId":"UYPGNywAXvb///////////////8DAAAA","taskName":"bench_task","taskType":"NORMAL_TASK"}}
{"eventType":"TASK_LIFECYCLE_EVENT","taskLifecycleEvent":{"jobId":"AwAAAA==","nodeId":"","stateTransitions":[{"state":"RUNNING","timestamp":"2026-08-08T01:45:58.211147463Z"}],"taskAttempt":0,"taskId":"UYPGNywAXvb///////////////8DAAAA","workerId":"","workerPid":215}}
{"eventType":"TASK_LIFECYCLE_EVENT","taskLifecycleEvent":{"jobId":"AwAAAA==","nodeId":"","stateTransitions":[],"taskAttempt":0,"taskId":"UYPGNywAXvb///////////////8DAAAA","taskLogInfo":{"stdoutEnd":"6331","stderrEnd":"6331"},"workerId":"","workerPid":0}}
{"eventType":"TASK_LIFECYCLE_EVENT","taskLifecycleEvent":{"jobId":"AwAAAA==","nodeId":"D2kMAY8zIxLFuYnLXnAKJtuumyBv618fCGSF7g==","stateTransitions":[{"state":"PENDING_ARGS_AVAIL","timestamp":"2026-08-08T01:45:57.009825296Z"},{"state":"SUBMITTED_TO_WORKER","timestamp":"2026-08-08T01:45:58.209153338Z"},{"state":"FINISHED","timestamp":"2026-08-08T01:45:58.224019505Z"}],"taskAttempt":0,"taskId":"UYPGNywAXvb///////////////8DAAAA","workerId":"BYdHtXNHrWxpi1lRHeTmT8N+THrSsDPrfN988Q==","workerPid":0}}
`

	events, err := DecodeEventFileBytes("ray-2.56-events.jsonl", []byte(capturedEvents))
	require.NoError(t, err)
	require.Len(t, events, 5)

	h := NewEventHandler(nil)
	for _, event := range events {
		require.NoError(t, h.storeEvent("cluster1", event))
	}
	tasks := h.getTasks("cluster1")
	require.Len(t, tasks, 1)
	task := tasks[0]
	assert.Equal(t, "bench_task", task.TaskName)
	assert.Equal(t, types.FINISHED, task.State)
	assert.NotEmpty(t, task.NodeID)
	assert.NotEmpty(t, task.WorkerID)
	require.NotNil(t, task.TaskLogInfo)
	assert.Equal(t, "/tmp/ray/session_2026-08-08_01-45-50_123456_1/logs/worker-058747b57347ad6c698b59511de4e64fc37e4c7ad2b033eb7cdf7cf1-03000000-215.out", task.TaskLogInfo.StdoutFile)
	assert.Equal(t, int64(512), task.TaskLogInfo.StdoutStart)
	assert.Equal(t, int64(6331), task.TaskLogInfo.StdoutEnd)
	assert.Equal(t, int64(0), task.TaskLogInfo.StderrStart)
	assert.Equal(t, int64(6331), task.TaskLogInfo.StderrEnd)
}

// TestActorLifecycleEventDeduplication verifies that duplicate actor events are correctly filtered
func TestActorLifecycleEventDeduplication(t *testing.T) {
	// IDs follow Ray's ID spec; see TestStoreEvent for rationale.
	const (
		testJobID       = "aaaabbbb"                             // 4B
		testActorID     = "aaaabbbb1234aaaabbbb1234" + testJobID // 12B unique + JobID
		testClusterName = "cluster1"
	)

	// Helper to create an ActorStateEvent
	makeActorStateEvent := func(state types.StateType, timestampNano int64) types.ActorStateEvent {
		return types.ActorStateEvent{
			State:     state,
			Timestamp: time.Unix(0, timestampNano),
		}
	}

	// Helper to create an ACTOR_LIFECYCLE_EVENT map
	makeActorLifecycleEvent := func(transitions []map[string]any) map[string]any {
		// Convert []map[string]any to []any for proper type assertion in storeEvent
		transitionsAny := make([]any, len(transitions))
		for i, t := range transitions {
			transitionsAny[i] = t
		}
		return map[string]any{
			"eventType": string(types.ACTOR_LIFECYCLE_EVENT),
			"actorLifecycleEvent": map[string]any{
				"actorId":          testActorID,
				"stateTransitions": transitionsAny,
			},
		}
	}

	// Helper to create state transition
	makeTransition := func(state string, timestampNano int64) map[string]any {
		return map[string]any{
			"state":     state,
			"timestamp": time.Unix(0, timestampNano).Format(time.RFC3339Nano),
		}
	}

	tests := []struct {
		name           string
		existingEvents []types.ActorStateEvent
		newTransitions []map[string]any
		wantEvents     []types.ActorStateEvent
		wantState      types.StateType
	}{
		{
			name: "Actor: Normal deduplication",
			existingEvents: []types.ActorStateEvent{
				makeActorStateEvent(types.PENDING_CREATION, 1000),
				makeActorStateEvent(types.ALIVE, 2000),
			},
			newTransitions: []map[string]any{
				makeTransition("PENDING_CREATION", 1000),
				makeTransition("ALIVE", 2000),
			},
			wantEvents: []types.ActorStateEvent{
				makeActorStateEvent(types.PENDING_CREATION, 1000),
				makeActorStateEvent(types.ALIVE, 2000),
			},
			wantState: types.ALIVE,
		},
		{
			name: "Actor: Out-of-order with sort",
			existingEvents: []types.ActorStateEvent{
				makeActorStateEvent(types.PENDING_CREATION, 1000),
				makeActorStateEvent(types.DEAD, 3000),
			},
			newTransitions: []map[string]any{
				makeTransition("ALIVE", 2000), // Should be inserted between
			},
			wantEvents: []types.ActorStateEvent{
				makeActorStateEvent(types.PENDING_CREATION, 1000),
				makeActorStateEvent(types.ALIVE, 2000),
				makeActorStateEvent(types.DEAD, 3000),
			},
			wantState: types.DEAD,
		},
		{
			name: "Actor: Exact duplicate should not increase count",
			existingEvents: []types.ActorStateEvent{
				makeActorStateEvent(types.ALIVE, 1000),
			},
			newTransitions: []map[string]any{
				makeTransition("ALIVE", 1000),
				makeTransition("ALIVE", 1000),
				makeTransition("ALIVE", 1000),
			},
			wantEvents: []types.ActorStateEvent{
				makeActorStateEvent(types.ALIVE, 1000),
			},
			wantState: types.ALIVE,
		},
		{
			name: "Actor: Same timestamp different states should both be kept",
			existingEvents: []types.ActorStateEvent{
				makeActorStateEvent(types.PENDING_CREATION, 1000),
			},
			newTransitions: []map[string]any{
				makeTransition("ALIVE", 1000), // Same timestamp, different state
			},
			wantEvents: []types.ActorStateEvent{
				makeActorStateEvent(types.PENDING_CREATION, 1000),
				makeActorStateEvent(types.ALIVE, 1000),
			},
			wantState: types.ALIVE,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := NewEventHandler(nil)

			// Pre-populate existing events
			if len(tt.existingEvents) > 0 {
				actorMap := h.ClusterActorMap.GetOrCreateActorMap(testClusterName)
				actorMap.CreateOrMergeActor(testActorID, func(a *types.Actor) {
					a.ActorID = testActorID
					a.Events = tt.existingEvents
					if len(tt.existingEvents) > 0 {
						a.State = tt.existingEvents[len(tt.existingEvents)-1].State
					}
				})
			}

			// Process the lifecycle event
			eventMap := makeActorLifecycleEvent(tt.newTransitions)
			err := h.storeEvent(testClusterName, eventMap)
			if err != nil {
				t.Fatalf("storeEvent() unexpected error: %v", err)
			}

			// Get the actor and verify
			actor, found := h.getActorsMap(testClusterName)[testActorID]
			if !found {
				t.Fatal("Actor not found after processing")
			}

			// Verify events count (most important for dedup testing)
			if len(actor.Events) != len(tt.wantEvents) {
				t.Errorf("Event count = %d, want %d", len(actor.Events), len(tt.wantEvents))
			}

			// Verify final state
			if actor.State != tt.wantState {
				t.Errorf("State = %v, want %v", actor.State, tt.wantState)
			}
		})
	}
}

// TestDriverJobLifeCycleEventDuplication tests that duplicate events are properly filtered and sorted
// TODO(chiayi): Update once more fields are added to driver job event
func TestDriverJobLifecycleEventDuplication(t *testing.T) {
	// IDs follow Ray's ID spec; see TestStoreEvent for rationale.
	const (
		testJobID       = "aaaabbbb" // 4B
		testClusterName = "cluster1"
	)

	makeDriverJobStateTransitionEvent := func(state types.JobState, timestampNano int64) types.JobStateTransition {
		return types.JobStateTransition{
			State:     state,
			Timestamp: time.Unix(0, timestampNano),
		}
	}

	makeDriverJobLifecycleEvent := func(transitions []map[string]any) map[string]any {
		transitionsAny := make([]any, len(transitions))
		for i, t := range transitions {
			transitionsAny[i] = t
		}
		return map[string]any{
			"eventType": string(types.DRIVER_JOB_LIFECYCLE_EVENT),
			"driverJobLifecycleEvent": map[string]any{
				"jobId":            testJobID,
				"stateTransitions": transitionsAny,
			},
		}
	}

	makeTransition := func(state string, timestampNano int64) map[string]any {
		return map[string]any{
			"state":     state,
			"timestamp": time.Unix(0, timestampNano).Format(time.RFC3339Nano),
		}
	}

	tests := []struct {
		name string
		// Available Job States are UNSPECIFIED, CREATED, JOBFINISHED
		existingTransitions []types.JobStateTransition
		newTransitions      []map[string]any
		wantTransitions     []types.JobStateTransition
		wantState           types.JobState
	}{
		{
			name: "Driver Job: Same event is processed twice",
			existingTransitions: []types.JobStateTransition{
				makeDriverJobStateTransitionEvent(types.UNSPECIFIED, 1000),
				makeDriverJobStateTransitionEvent(types.CREATED, 2000),
			},
			newTransitions: []map[string]any{
				makeTransition("UNSPECIFIED", 1000),
				makeTransition("CREATED", 2000),
			},
			wantTransitions: []types.JobStateTransition{
				makeDriverJobStateTransitionEvent(types.UNSPECIFIED, 1000),
				makeDriverJobStateTransitionEvent(types.CREATED, 2000),
			},
			wantState: types.CREATED,
		},
		{
			name:                "Driver Job: Empty existing transitions",
			existingTransitions: []types.JobStateTransition{},
			newTransitions: []map[string]any{
				makeTransition("UNSPECIFIED", 1000),
				makeTransition("CREATED", 2000),
				makeTransition("FINISHED", 3000),
			},
			wantTransitions: []types.JobStateTransition{
				makeDriverJobStateTransitionEvent(types.UNSPECIFIED, 1000),
				makeDriverJobStateTransitionEvent(types.CREATED, 2000),
				makeDriverJobStateTransitionEvent(types.JOBFINISHED, 3000),
			},
			wantState: types.JOBFINISHED,
		},
		{
			name: "Driver Job: Out of order transition event",
			existingTransitions: []types.JobStateTransition{
				makeDriverJobStateTransitionEvent(types.UNSPECIFIED, 1000),
				makeDriverJobStateTransitionEvent(types.JOBFINISHED, 3000),
			},
			newTransitions: []map[string]any{
				makeTransition("CREATED", 2000),
			},
			wantTransitions: []types.JobStateTransition{
				makeDriverJobStateTransitionEvent(types.UNSPECIFIED, 1000),
				makeDriverJobStateTransitionEvent(types.CREATED, 2000),
				makeDriverJobStateTransitionEvent(types.JOBFINISHED, 3000),
			},
			wantState: types.JOBFINISHED,
		},
		{
			name: "Driver Job: Multiple duplicate of same transition event",
			existingTransitions: []types.JobStateTransition{
				makeDriverJobStateTransitionEvent(types.CREATED, 2000),
			},
			newTransitions: []map[string]any{
				makeTransition("CREATED", 2000),
				makeTransition("CREATED", 2000),
				makeTransition("CREATED", 2000),
				makeTransition("CREATED", 2000),
			},
			wantTransitions: []types.JobStateTransition{
				makeDriverJobStateTransitionEvent(types.CREATED, 2000),
			},
			wantState: types.CREATED,
		},
		{
			name: "Driver Job: Different transition events with same time stamp, all be kept",
			existingTransitions: []types.JobStateTransition{
				makeDriverJobStateTransitionEvent(types.UNSPECIFIED, 1000),
			},
			newTransitions: []map[string]any{
				makeTransition("CREATED", 1000),
				makeTransition("CREATED", 2000),
				makeTransition("FINISHED", 2000),
			},
			wantTransitions: []types.JobStateTransition{
				makeDriverJobStateTransitionEvent(types.UNSPECIFIED, 1000),
				makeDriverJobStateTransitionEvent(types.CREATED, 1000),
				makeDriverJobStateTransitionEvent(types.CREATED, 2000),
				makeDriverJobStateTransitionEvent(types.JOBFINISHED, 2000),
			},
			wantState: types.JOBFINISHED,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := NewEventHandler(nil)

			if len(tt.existingTransitions) > 0 {
				jobMap := h.ClusterJobMap.GetOrCreateJobMap(testClusterName)
				jobMap.CreateOrMergeJob(testJobID, func(job *types.Job) {
					job.JobID = testJobID
					job.StateTransitions = tt.existingTransitions
					if len(tt.existingTransitions) > 0 {
						job.State = tt.existingTransitions[len(tt.existingTransitions)-1].State
					}
				})
			}

			eventMap := makeDriverJobLifecycleEvent(tt.newTransitions)
			err := h.storeEvent(testClusterName, eventMap)
			if err != nil {
				t.Fatalf("storeEvent() unexpected error: %v", err)
			}

			job, exists := h.getJobsMap(testClusterName)[testJobID]
			if !exists {
				t.Fatal("Job not found after processing")
			}

			if len(job.StateTransitions) != len(tt.wantTransitions) {
				t.Errorf("Actual transition count %d but expected %d", len(job.StateTransitions), len(tt.wantTransitions))
			}

			if job.State != tt.wantState {
				t.Errorf("Actual State %v but expected %v", job.State, tt.wantState)
			}
		})
	}
}

// TestMultipleReprocessingCycles simulates hourly reprocessing and verifies no memory growth
func TestMultipleReprocessingCycles(t *testing.T) {
	h := NewEventHandler(nil)

	// IDs follow Ray's ID spec; see TestStoreEvent for rationale.
	const (
		testJobID       = "aaaabbbb"                                                 // 4B
		testActorID     = "aaaabbbb1234aaaabbbb1234" + testJobID                     // 12B unique + JobID
		testTaskID      = "ccccdddd5678cccc" + testActorID                           // 8B unique + ActorID
		testNodeID      = "eeeeffff1234eeeeffff1234eeeeffff1234eeeeffff1234eeeeffff" // 28B
		testWorkerID    = "eeeeffff0000eeeeffff0000eeeeffff0000eeeeffff0000eeeeffff" // 28B
		testClusterName = "cluster1"
	)

	// The same events that would be in an event file
	// Use []any to match what storeEvent expects from JSON parsing
	transitions := []any{
		map[string]any{"state": "PENDING_NODE_ASSIGNMENT", "timestamp": time.Unix(0, 1000).Format(time.RFC3339Nano)},
		map[string]any{"state": "RUNNING", "timestamp": time.Unix(0, 2000).Format(time.RFC3339Nano)},
		map[string]any{"state": "FINISHED", "timestamp": time.Unix(0, 3000).Format(time.RFC3339Nano)},
	}

	eventMap := map[string]any{
		"eventType": string(types.TASK_LIFECYCLE_EVENT),
		"taskLifecycleEvent": map[string]any{
			"taskId":           testTaskID,
			"taskAttempt":      float64(0),
			"stateTransitions": transitions,
			"nodeId":           testNodeID,
			"workerId":         testWorkerID,
		},
	}

	// Simulate 10 hourly reprocessing cycles
	for cycle := range 10 {
		err := h.storeEvent(testClusterName, eventMap)
		if err != nil {
			t.Fatalf("Cycle %d: storeEvent() error = %v", cycle, err)
		}

		// Check event count after each cycle
		taskMap := h.ClusterTaskMap.GetOrCreateTaskMap(testClusterName)
		taskMap.Lock()
		tasks := taskMap.TaskMap[testTaskID]
		eventCount := len(tasks[0].StateTransitions)
		taskMap.Unlock()

		// Should always be exactly 3 events, never growing
		if eventCount != 3 {
			t.Errorf("Cycle %d: Event count = %d, want 3 (events are duplicating!)", cycle, eventCount)
		}
	}
}

func TestNormalizeIDToHex(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "empty string returns empty",
			input:    "",
			expected: "",
		},
		{
			name:     "valid base64 converts to hex",
			input:    "AgAAAA==",
			expected: "02000000",
		},
		{
			name:     "invalid base64 returns original",
			input:    "not_valid_base64!!!",
			expected: "not_valid_base64!!!",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := normalizeIDToHex(tt.input)
			if got != tt.expected {
				t.Errorf("normalizeIDToHex(%q) = %q, want %q", tt.input, got, tt.expected)
			}
		})
	}
}

func TestNormalizeActorIDsToHex(t *testing.T) {
	base64Val := "AgAAAA=="
	expectedHex, _ := utils.ConvertBase64ToHex(base64Val)

	actor := types.Actor{
		ActorID:          base64Val,
		JobID:            base64Val,
		PlacementGroupID: base64Val,
		Address: types.Address{
			NodeID:   base64Val,
			WorkerID: base64Val,
		},
	}

	normalizeActorIDsToHex(&actor)

	if actor.ActorID != expectedHex {
		t.Errorf("ActorID = %q, want %q", actor.ActorID, expectedHex)
	}
	if actor.JobID != expectedHex {
		t.Errorf("JobID = %q, want %q", actor.JobID, expectedHex)
	}
	if actor.PlacementGroupID != expectedHex {
		t.Errorf("PlacementGroupID = %q, want %q", actor.PlacementGroupID, expectedHex)
	}
	if actor.Address.NodeID != expectedHex {
		t.Errorf("Address.NodeID = %q, want %q", actor.Address.NodeID, expectedHex)
	}
	if actor.Address.WorkerID != expectedHex {
		t.Errorf("Address.WorkerID = %q, want %q", actor.Address.WorkerID, expectedHex)
	}
}

func TestProcessSingleSession(t *testing.T) {
	clusterInfo := utils.ClusterInfo{Name: "cluster", Namespace: "ns", SessionName: "session1"}

	t.Run("returns error when every listed file fails I/O", func(t *testing.T) {
		mock := newLogEventMockReader()
		mock.addDir("session1", []string{"node1/"})
		mock.addDir("session1/node1/job_events/", []string{"job-01000000/"})
		mock.addDir("session1/node1/job_events/job-01000000/",
			[]string{"01000000-2024-01-01-00.gz"})
		mock.addDir("session1/node1/node_events/",
			[]string{"node1-2024-01-01-00.gz"})
		mock.addDir("session1/logs", []string{})

		h := NewEventHandler(mock)
		err := h.ProcessSingleSession(context.Background(), clusterInfo)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "read 0 of 2")
	})

	t.Run("empty file list returns nil (legit empty session)", func(t *testing.T) {
		mock := newLogEventMockReader()
		mock.addDir("session1", []string{"node1/"})
		mock.addDir("session1/node1/job_events/", []string{})
		mock.addDir("session1/node1/node_events/", []string{})
		mock.addDir("session1/logs", []string{})

		h := NewEventHandler(mock)
		err := h.ProcessSingleSession(context.Background(), clusterInfo)
		require.NoError(t, err)
	})

	t.Run("partial success does not return error", func(t *testing.T) {
		mock := newLogEventMockReader()
		mock.addDir("session1", []string{"node1/", "node2/"})
		mock.addDir("session1/node1/node_events/",
			[]string{"node1-2024-01-01-00.gz"})
		mock.addDir("session1/node2/node_events/",
			[]string{"node2-2024-01-01-00.gz"})

		var buf bytes.Buffer
		gw := gzip.NewWriter(&buf)
		_, err := gw.Write([]byte(`[{"eventType":"NODE_DEFINITION_EVENT","nodeDefinitionEvent":{"nodeId":"YWJjZA=="}}]`))
		require.NoError(t, err)
		require.NoError(t, gw.Close())

		mock.addFile("session1/node1/node_events/node1-2024-01-01-00.gz", buf.String())
		mock.addDir("session1/node1/job_events/", []string{})
		mock.addDir("session1/node2/job_events/", []string{})
		mock.addDir("session1/logs", []string{})

		h := NewEventHandler(mock)
		err = h.ProcessSingleSession(context.Background(), clusterInfo)
		require.NoError(t, err)
	})

	t.Run("all corrupt JSON does not return error", func(t *testing.T) {
		mock := newLogEventMockReader()
		mock.addDir("session1", []string{"node1/", "node2/"})
		mock.addDir("session1/node1/node_events/",
			[]string{"node1-2024-01-01-00.gz"})
		mock.addDir("session1/node2/node_events/",
			[]string{"node2-2024-01-01-00.gz"})

		var buf bytes.Buffer
		gw := gzip.NewWriter(&buf)
		_, err := gw.Write([]byte("this is not json"))
		require.NoError(t, err)
		require.NoError(t, gw.Close())

		mock.addFile("session1/node1/node_events/node1-2024-01-01-00.gz", buf.String())
		mock.addFile("session1/node2/node_events/node2-2024-01-01-00.gz", buf.String())
		mock.addDir("session1/node1/job_events/", []string{})
		mock.addDir("session1/node2/job_events/", []string{})
		mock.addDir("session1/logs", []string{})

		h := NewEventHandler(mock)
		err = h.ProcessSingleSession(context.Background(), clusterInfo)
		require.NoError(t, err)
	})

	t.Run("log events failure alone does not surface as error", func(t *testing.T) {
		mock := newLogEventMockReader()
		mock.addDir("session1", []string{"node1/"})
		mock.addDir("session1/node1/job_events/", []string{})
		mock.addDir("session1/node1/node_events/", []string{})
		mock.addDir("session1/logs", []string{"node1/"})
		mock.addDir("session1/logs/node1/events", []string{"event_GCS.log"})

		h := NewEventHandler(mock)
		err := h.ProcessSingleSession(context.Background(), clusterInfo)
		require.NoError(t, err)
	})

	t.Run("ray events failure surfaces; log events failure stays silent", func(t *testing.T) {
		mock := newLogEventMockReader()
		mock.addDir("session1", []string{"node1/"})
		mock.addDir("session1/node1/node_events/", []string{"node1-2024-01-01-00.gz"})
		mock.addDir("session1/node1/job_events/", []string{})
		mock.addDir("session1/logs", []string{"node1/"})
		mock.addDir("session1/logs/node1/events", []string{"event_GCS.log"})

		h := NewEventHandler(mock)
		err := h.ProcessSingleSession(context.Background(), clusterInfo)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "read 0 of 1 event files")
		assert.NotContains(t, err.Error(), "log event")
	})

	t.Run("transparent decompression of compressed .gz files and reading uncompressed legacy files", func(t *testing.T) {
		mock := newLogEventMockReader()
		mock.addDir("session1", []string{"node1/", "node2/"})
		mock.addDir("session1/node1/node_events/",
			[]string{"node1-2024-01-01-00.gz"})
		mock.addDir("session1/node2/node_events/",
			[]string{"node2-2024-01-01-00"})

		// 1. Compress node1 event file
		var buf bytes.Buffer
		gw := gzip.NewWriter(&buf)
		_, err := gw.Write([]byte(`[{"eventType":"NODE_DEFINITION_EVENT","nodeDefinitionEvent":{"nodeId":"YWJjZA=="}}]`))
		require.NoError(t, err)
		require.NoError(t, gw.Close())
		mock.addFile("session1/node1/node_events/node1-2024-01-01-00.gz", buf.String())

		// 2. Keep node2 event file uncompressed
		mock.addFile("session1/node2/node_events/node2-2024-01-01-00",
			`[{"eventType":"NODE_DEFINITION_EVENT","nodeDefinitionEvent":{"nodeId":"ZWZnaA=="}}]`)

		mock.addDir("session1/node1/job_events/", []string{})
		mock.addDir("session1/node2/job_events/", []string{})
		mock.addDir("session1/logs", []string{})

		h := NewEventHandler(mock)
		err = h.ProcessSingleSession(context.Background(), clusterInfo)
		require.NoError(t, err)

		nodeMap := h.getNodeMap("cluster_ns_session1")
		assert.Len(t, nodeMap, 2)

		// "YWJjZA==" -> hex "61626364" (abcd)
		_, ok := nodeMap["61626364"]
		assert.True(t, ok, "node1 (compressed) should be successfully loaded")

		// "ZWZnaA==" -> hex "65666768" (efgh)
		_, ok = nodeMap["65666768"]
		assert.True(t, ok, "node2 (uncompressed) should be successfully loaded")
	})
}
