package eventserver

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ray-project/kuberay/historyserver/pkg/eventserver/types"
)

func TestSubmissionRoundtrip(t *testing.T) {
	rows, rowErrors, err := types.ParseJobDetailsList([]byte(`[
		{"submission_id":"synthetic-driverless","status":"FAILED","job_id":null,"driver_info":null,"driver_exit_code":null,"start_time":null,"end_time":null,"runtime_env":{"synthetic":"discard"}},
		{"submission_id":"synthetic-started","status":"SUCCEEDED","job_id":"01000000","driver_info":{"id":"01000000","node_ip_address":"192.0.2.1","pid":"1"},"driver_exit_code":0,"start_time":100,"end_time":200,"metadata":{"synthetic":"value"}}
	]`))
	require.NoError(t, err)
	require.Empty(t, rowErrors)
	original := SessionSnapshot{SessionKey: "synthetic", Jobs: map[string]types.Job{"01000000": {JobID: "01000000"}}, Submissions: map[string]types.JobDetails{}}
	for _, row := range rows {
		original.Submissions[*row.SubmissionID] = row
	}
	data, err := json.Marshal(original)
	require.NoError(t, err)
	require.NotContains(t, string(data), "discard")
	var decoded SessionSnapshot
	require.NoError(t, json.Unmarshal(data, &decoded))
	require.Equal(t, original, decoded)
	require.Len(t, decoded.Jobs, 1)
	require.NotContains(t, decoded.Jobs, "synthetic-driverless")
	for _, row := range decoded.Submissions {
		require.Nil(t, row.RuntimeEnv)
	}
}

func TestLegacySnapshotOmitsSubmissions(t *testing.T) {
	encoded, err := json.Marshal(SessionSnapshot{Jobs: map[string]types.Job{"01000000": {JobID: "01000000"}}})
	require.NoError(t, err)
	require.NotContains(t, string(encoded), "submissions")
	var decoded SessionSnapshot
	require.NoError(t, json.Unmarshal(encoded, &decoded))
	require.Nil(t, decoded.Submissions)
}

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
