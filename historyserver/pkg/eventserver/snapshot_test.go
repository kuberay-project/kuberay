package eventserver

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/ray-project/kuberay/historyserver/pkg/eventserver/types"
	"github.com/ray-project/kuberay/historyserver/pkg/utils"
)

// TestEmptyRoundtrip verifies that an empty SessionSnapshot marshals and
// unmarshals back to an equal struct.
func TestEmptyRoundtrip(t *testing.T) {
	original := SessionSnapshot{
		SessionKey: "raycluster-historyserver_default_session_2026-01-11_19-38-40",
	}

	data, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("json.Marshal failed: %v", err)
	}

	var decoded SessionSnapshot
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("json.Unmarshal failed: %v", err)
	}

	if !reflect.DeepEqual(original, decoded) {
		t.Fatalf("roundtrip mismatch\noriginal=%+v\ndecoded =%+v", original, decoded)
	}
}

// TestFullRoundtrip populates all five maps with at least one entry and
// verifies the snapshot survives a JSON roundtrip intact.
func TestFullRoundtrip(t *testing.T) {
	original := SessionSnapshot{
		SessionKey: "raycluster-historyserver_default_session_2026-01-11_19-38-40",
		Tasks: []types.Task{
			{TaskID: "task-1", TaskAttempt: 0, JobID: "job-1"},
			{TaskID: "task-1", TaskAttempt: 1, JobID: "job-1"},
		},
		Actors: map[string]types.Actor{
			"actor-1": {ActorID: "actor-1", JobID: "job-1"},
		},
		Jobs: map[string]types.Job{
			"job-1": {JobID: "job-1", EntryPoint: "python main.py"},
		},
		Nodes: map[string]types.Node{
			"node-1": {NodeID: "node-1", NodeIPAddress: "10.0.0.1"},
		},
		LogEventsByJobID: map[string][]types.LogEvent{
			"job-1": {
				{EventID: "evt-1", Message: "hello", Severity: "INFO"},
			},
		},
	}

	data, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("json.Marshal failed: %v", err)
	}

	var decoded SessionSnapshot
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("json.Unmarshal failed: %v", err)
	}

	if !reflect.DeepEqual(original, decoded) {
		t.Fatalf("roundtrip mismatch\noriginal=%+v\ndecoded =%+v", original, decoded)
	}
}

// TestBuildSnapshotTasksOfFinishedJob covers the scenario reported in
// https://github.com/ray-project/kuberay/issues/4814: the RayJob finished and the
// cluster was torn down before the tasks' terminal lifecycle events were exported,
// so the last recorded task state is RUNNING. Ray core marks non-terminal tasks of
// a finished job as FAILED at job end (GcsTaskManager::OnJobFinished); the replayed
// snapshot must do the same instead of showing such tasks as running forever.
func TestBuildSnapshotTasksOfFinishedJob(t *testing.T) {
	// IDs follow Ray's ID spec; see TestStoreEvent for rationale.
	const (
		jobID    = "aaaabbbb"                                                 // 4B
		actorID  = "aaaabbbb1234aaaabbbb1234" + jobID                         // 12B unique + JobID
		taskID   = "ccccdddd5678cccc" + actorID                               // 8B unique + ActorID
		nodeID   = "eeeeffff1234eeeeffff1234eeeeffff1234eeeeffff1234eeeeffff" // 28B
		workerID = "eeeeffff0000eeeeffff0000eeeeffff0000eeeeffff0000eeeeffff" // 28B
	)

	session := utils.ClusterInfo{Name: "raycluster-hs", Namespace: "default", SessionName: "session_2026-05-08_10-00-00"}
	clusterSessionKey := utils.BuildClusterSessionKey(session.Name, session.Namespace, session.SessionName)

	taskCreated := time.Unix(0, 1000).UTC()
	taskRunning := time.Unix(0, 2000).UTC()
	jobCreated := time.Unix(0, 500).UTC()
	jobFinished := time.Unix(0, 3000).UTC()

	h := NewEventHandler(nil)

	events := []map[string]any{
		{
			"eventType": string(types.TASK_DEFINITION_EVENT),
			"taskDefinitionEvent": map[string]any{
				"taskId":      taskID,
				"taskName":    "stuck_task",
				"nodeId":      nodeID,
				"taskAttempt": float64(0),
				"jobId":       jobID,
			},
		},
		{
			"eventType": string(types.TASK_LIFECYCLE_EVENT),
			"taskLifecycleEvent": map[string]any{
				"taskId":      taskID,
				"taskAttempt": float64(0),
				"nodeId":      nodeID,
				"workerId":    workerID,
				"stateTransitions": []any{
					map[string]any{"state": "PENDING_ARGS_AVAIL", "timestamp": taskCreated.Format(time.RFC3339Nano)},
					map[string]any{"state": "RUNNING", "timestamp": taskRunning.Format(time.RFC3339Nano)},
					// No terminal transition: the cluster was torn down before the
					// task's FINISHED/FAILED event was exported.
				},
			},
		},
		{
			"eventType": string(types.DRIVER_JOB_LIFECYCLE_EVENT),
			"driverJobLifecycleEvent": map[string]any{
				"jobId": jobID,
				"stateTransitions": []any{
					map[string]any{"state": string(types.CREATED), "timestamp": jobCreated.Format(time.RFC3339Nano)},
					map[string]any{"state": string(types.JOBFINISHED), "timestamp": jobFinished.Format(time.RFC3339Nano)},
				},
			},
		},
	}
	for _, event := range events {
		if err := h.storeEvent(clusterSessionKey, event); err != nil {
			t.Fatalf("storeEvent() unexpected error: %v", err)
		}
	}

	snap := h.BuildSnapshot(session)

	job, ok := snap.Jobs[jobID]
	if !ok {
		t.Fatalf("BuildSnapshot() job %s not found in snapshot", jobID)
	}
	if job.State != types.JOBFINISHED {
		t.Fatalf("BuildSnapshot() job state = %s, want %s", job.State, types.JOBFINISHED)
	}

	if len(snap.Tasks) != 1 {
		t.Fatalf("BuildSnapshot() returned %d tasks, want 1", len(snap.Tasks))
	}
	task := snap.Tasks[0]
	if task.State != types.FAILED {
		t.Errorf("BuildSnapshot() task state = %s, want %s: a non-terminal task of a finished job must not be shown as still running", task.State, types.FAILED)
	}
	if !task.EndTime.Equal(jobFinished) {
		t.Errorf("BuildSnapshot() task end time = %v, want job end time %v", task.EndTime, jobFinished)
	}
	// The serialized state history must end in the same state as the
	// top-level State field.
	if len(task.StateTransitions) == 0 {
		t.Fatalf("BuildSnapshot() task has no state transitions, want a trailing %s transition", types.FAILED)
	}
	lastTransition := task.StateTransitions[len(task.StateTransitions)-1]
	if lastTransition.State != types.FAILED {
		t.Errorf("BuildSnapshot() last task state transition = %s, want %s", lastTransition.State, types.FAILED)
	}
	if !lastTransition.Timestamp.Equal(jobFinished) {
		t.Errorf("BuildSnapshot() last task state transition timestamp = %v, want job end time %v", lastTransition.Timestamp, jobFinished)
	}
}
