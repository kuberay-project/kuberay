package types

import (
	"encoding/json"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTaskLogInfoProtoJSON(t *testing.T) {
	want := TaskLogInfo{
		StdoutFile:  "worker.out",
		StderrFile:  "worker.err",
		StdoutStart: 10,
		StdoutEnd:   20,
		StderrStart: 30,
		StderrEnd:   40,
	}

	var got TaskLogInfo
	require.NoError(t, json.Unmarshal([]byte(`{"stdoutFile":"worker.out","stderrFile":"worker.err","stdoutStart":"10","stdoutEnd":"20","stderrStart":"30","stderrEnd":"40"}`), &got))
	assert.Equal(t, want, got)

	t.Run("missing and null fields keep their existing values", func(t *testing.T) {
		got := want
		require.NoError(t, json.Unmarshal([]byte(`{"stdoutFile":"new.out","stdoutStart":null}`), &got))
		assert.Equal(t, "new.out", got.StdoutFile)
		assert.Equal(t, want.StdoutStart, got.StdoutStart)
		assert.Equal(t, want.StdoutEnd, got.StdoutEnd)
		assert.Equal(t, want.StderrFile, got.StderrFile)
	})

	t.Run("full int64 range", func(t *testing.T) {
		var got TaskLogInfo
		data := `{"stdoutStart":"-9223372036854775808","stdoutEnd":"9223372036854775807"}`
		require.NoError(t, json.Unmarshal([]byte(data), &got))
		assert.Equal(t, int64(math.MinInt64), got.StdoutStart)
		assert.Equal(t, int64(math.MaxInt64), got.StdoutEnd)
	})

	for _, data := range []string{
		`{"stdoutEnd":"not-an-integer"}`,
		`{"stdoutEnd":"9223372036854775808"}`,
		`{"stdoutEnd":1}`,
		`{"stdoutEnd":1.5}`,
	} {
		t.Run("invalid value "+data, func(t *testing.T) {
			var got TaskLogInfo
			require.Error(t, json.Unmarshal([]byte(data), &got))
		})
	}

	t.Run("marshals offsets as ProtoJSON strings", func(t *testing.T) {
		data, err := json.Marshal(want)
		require.NoError(t, err)
		assert.JSONEq(t, `{"stdoutFile":"worker.out","stderrFile":"worker.err","stdoutStart":"10","stdoutEnd":"20","stderrStart":"30","stderrEnd":"40"}`, string(data))
	})
}
