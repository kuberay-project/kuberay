package eventserver

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/ray-project/kuberay/historyserver/pkg/eventserver/types"
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
