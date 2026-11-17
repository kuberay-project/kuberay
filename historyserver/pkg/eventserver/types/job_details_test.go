package types

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseJobDetailsListPreservesNullableFields(t *testing.T) {
	body := `[{"type":"SUBMISSION","submission_id":"synthetic","job_id":null,"driver_info":null,"status":"FAILED","entrypoint":"python missing.py","message":null,"error_type":null,"start_time":null,"end_time":null,"metadata":null,"runtime_env":{"env_vars":{"SYNTHETIC_ENV":"excluded"}},"driver_agent_http_address":null,"driver_node_id":null,"driver_exit_code":null}]`
	records, rowErrors, err := ParseJobDetailsList([]byte(body))
	require.NoError(t, err)
	require.Empty(t, rowErrors)
	require.Len(t, records, 1)
	record := records[0]
	assert.Nil(t, record.JobID)
	assert.Nil(t, record.DriverInfo)
	assert.Nil(t, record.Message)
	assert.Nil(t, record.ErrorType)
	assert.Nil(t, record.StartTime)
	assert.Nil(t, record.EndTime)
	assert.Nil(t, record.Metadata)
	assert.Nil(t, record.RuntimeEnv)
	assert.Nil(t, record.DriverAgentHTTPAddress)
	assert.Nil(t, record.DriverNodeID)
	assert.Nil(t, record.DriverExitCode)
	encoded, err := json.Marshal(record)
	require.NoError(t, err)
	assert.JSONEq(t, strings.Replace(body[1:len(body)-1], `{"env_vars":{"SYNTHETIC_ENV":"excluded"}}`, `null`, 1), string(encoded))
}

func TestParseJobDetailsListRoundTripValues(t *testing.T) {
	body := `[{"type":"SUBMISSION","submission_id":"synthetic","job_id":"01000000","driver_info":{"id":"01000000","node_ip_address":"192.0.2.1","pid":"42"},"status":"SUCCEEDED","entrypoint":"python synthetic.py","message":"","error_type":"","start_time":1700000000123,"end_time":1700000000456,"metadata":{"fixture":"synthetic"},"runtime_env":null,"driver_agent_http_address":"http://192.0.2.1:52365","driver_node_id":"synthetic_node","driver_exit_code":0}]`
	records, rowErrors, err := ParseJobDetailsList([]byte(body))
	require.NoError(t, err)
	require.Empty(t, rowErrors)
	require.Len(t, records, 1)
	assert.Equal(t, JobDriverInfo{ID: "01000000", NodeIPAddress: "192.0.2.1", PID: "42"}, *records[0].DriverInfo)
	assert.Equal(t, int64(1700000000123), *records[0].StartTime)
	assert.Equal(t, int64(1700000000456), *records[0].EndTime)
	assert.Equal(t, 0, *records[0].DriverExitCode)
	assert.Empty(t, *records[0].Message)
	encoded, err := json.Marshal(records)
	require.NoError(t, err)
	assert.JSONEq(t, body, string(encoded))
}

func TestParseJobDetailsListOlderShapesAndStatuses(t *testing.T) {
	for _, status := range []JobStatus{PENDING, JOBRUNNING, SUCCEEDED, JOBFAILED, STOPPED} {
		for _, identity := range []struct {
			fields   string
			wantType string
		}{
			{fields: `"submission_id":"synthetic"`, wantType: "SUBMISSION"},
			{fields: `"submission_id":"synthetic","job_id":"01000000"`, wantType: "SUBMISSION"},
			{fields: `"job_id":"01000000"`, wantType: "DRIVER"},
			{fields: `"type":"DRIVER","job_id":"01000000","submission_id":"synthetic"`, wantType: "DRIVER"},
		} {
			records, rowErrors, err := ParseJobDetailsList([]byte(`[{` + identity.fields + `,"status":"` + string(status) + `","runtime_env":"ignored"}]`))
			require.NoError(t, err)
			require.Empty(t, rowErrors)
			require.Len(t, records, 1)
			assert.Equal(t, identity.wantType, records[0].Type)
			assert.Equal(t, status, records[0].Status)
			assert.Nil(t, records[0].RuntimeEnv)
		}
	}
}

func TestParseJobDetailsListRequiresJSONArray(t *testing.T) {
	for _, body := range []string{"", "null", "{}", `"synthetic"`, "1", "true", "[{", "[] trailing", "[][]"} {
		records, rowErrors, err := ParseJobDetailsList([]byte(body))
		require.Error(t, err)
		assert.Nil(t, records)
		assert.Empty(t, rowErrors)
		assert.NotContains(t, err.Error(), "synthetic")
	}
	records, rowErrors, err := ParseJobDetailsList([]byte(" \n [] \t"))
	require.NoError(t, err)
	assert.Empty(t, records)
	assert.Empty(t, rowErrors)
}

func TestParseJobDetailsListRetainsValidRowsWithoutPayloadErrors(t *testing.T) {
	invalidRows := []string{
		`null`, `[]`, `"synthetic_private_value"`, `42`, `{}`,
		`{"submission_id":"synthetic_private_value","status":"UNKNOWN"}`,
		`{"type":"UNKNOWN","job_id":"synthetic_private_value","status":"RUNNING"}`,
		`{"type":"SUBMISSION","job_id":"synthetic_private_value","status":"RUNNING"}`,
		`{"type":"SUBMISSION","submission_id":"  ","status":"PENDING"}`,
		`{"type":"DRIVER","submission_id":"synthetic_private_value","status":"RUNNING"}`,
		`{"type":"DRIVER","job_id":"","status":"RUNNING"}`,
		`{"job_id":"01000000","status":"RUNNING","driver_info":{"id":"02000000","node_ip_address":"192.0.2.1","pid":"42"}}`,
		`{"submission_id":"synthetic_private_value","status":"PENDING","driver_info":{"id":"01000000","node_ip_address":"192.0.2.1","pid":"42"}}`,
		`{"job_id":"01000000","status":"RUNNING","driver_info":{}}`,
		`{"job_id":"01000000","status":"RUNNING","driver_info":{"id":"01000000","pid":"42"}}`,
		`{"job_id":"01000000","status":"RUNNING","driver_info":{"id":"01000000","node_ip_address":"192.0.2.1","pid":42}}`,
		`{"job_id":42,"status":"RUNNING"}`,
		`{"submission_id":"synthetic_private_value","status":"PENDING","metadata":{"fixture":42}}`,
		`{"submission_id":"synthetic_private_value","status":"PENDING","start_time":1.5}`,
		`{"submission_id":"synthetic_private_value","status":"PENDING","end_time":9223372036854775808}`,
		`{"submission_id":"synthetic_private_value","status":"PENDING","driver_exit_code":"synthetic_private_value"}`,
	}
	for _, row := range invalidRows {
		records, rowErrors, err := ParseJobDetailsList([]byte(`[{"submission_id":"first","status":"PENDING"},` + row + `,{"submission_id":"last","status":"STOPPED"}]`))
		require.NoError(t, err)
		require.Len(t, rowErrors, 1)
		assert.Contains(t, rowErrors[0].Error(), "row 1:")
		assert.NotContains(t, rowErrors[0].Error(), "synthetic_private_value")
		assert.NotContains(t, rowErrors[0].Error(), "01000000")
		require.Len(t, records, 2)
		assert.Equal(t, "first", *records[0].SubmissionID)
		assert.Equal(t, "last", *records[1].SubmissionID)
		assert.Nil(t, records[0].RuntimeEnv)
		assert.Nil(t, records[1].RuntimeEnv)
	}
	records, rowErrors, err := ParseJobDetailsList([]byte(`[null,{},false]`))
	require.NoError(t, err)
	assert.Empty(t, records)
	require.Len(t, rowErrors, 3)
	assert.Contains(t, rowErrors[0].Error(), "row 0:")
	assert.Contains(t, rowErrors[1].Error(), "row 1:")
	assert.Contains(t, rowErrors[2].Error(), "row 2:")
}

func TestIsTerminalJobStatus(t *testing.T) {
	for _, status := range []JobStatus{SUCCEEDED, JOBFAILED, STOPPED} {
		assert.True(t, IsTerminalJobStatus(status))
	}
	for _, status := range []JobStatus{PENDING, JOBRUNNING, "", "UNKNOWN"} {
		assert.False(t, IsTerminalJobStatus(status))
	}
}
