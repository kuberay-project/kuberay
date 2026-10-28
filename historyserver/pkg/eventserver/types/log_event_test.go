package types

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestJobEventMap(t *testing.T) {
	t.Run("groups events by jobID and global", func(t *testing.T) {
		em := NewJobEventMap()
		em.AddEvent(&LogEvent{EventID: "1Ac5EF8dC3B20F78", Timestamp: "1770635705", CustomFields: map[string]any{"job_id": "01000000"}})
		em.AddEvent(&LogEvent{EventID: "C798f8dC4Dce8B8b", Timestamp: "1770635720", CustomFields: map[string]any{"job_id": "02000000"}})
		em.AddEvent(&LogEvent{EventID: "A1B2C3D4E5F6A7B8", Timestamp: "1770635800"}) // no job_id → global

		all := em.GetAllEvents()
		assert.Len(t, all, 3)
		assert.Len(t, all["01000000"], 1)
		assert.Len(t, all["02000000"], 1)
		assert.Len(t, all["global"], 1)
	})

	t.Run("returns sorted events by timestamp", func(t *testing.T) {
		em := NewJobEventMap()
		em.AddEvent(&LogEvent{EventID: "e1", Timestamp: "1770635800", CustomFields: map[string]any{"job_id": "01000000"}})
		em.AddEvent(&LogEvent{EventID: "e2", Timestamp: "1770635700", CustomFields: map[string]any{"job_id": "01000000"}})
		em.AddEvent(&LogEvent{EventID: "e3", Timestamp: "1770635750", CustomFields: map[string]any{"job_id": "01000000"}})

		events := em.GetEventsByJobID("01000000")
		assert.Equal(t, "1770635700", events[0]["timestamp"])
		assert.Equal(t, "1770635750", events[1]["timestamp"])
		assert.Equal(t, "1770635800", events[2]["timestamp"])
	})

	t.Run("enforces MaxEventsPerJob limit", func(t *testing.T) {
		em := NewJobEventMap()
		numEvents := int(float64(MaxEventsPerJob)*1.1) + 1
		for i := range numEvents {
			em.AddEvent(&LogEvent{
				EventID:      fmt.Sprintf("evt_%d", i),
				Timestamp:    fmt.Sprintf("%d", 1770635000+i),
				CustomFields: map[string]any{"job_id": "01000000"},
			})
		}

		events := em.GetEventsByJobID("01000000")
		require.Len(t, events, MaxEventsPerJob)
		// Verify oldest events were evicted
		assert.Equal(t, fmt.Sprintf("%d", 1770635000+numEvents-MaxEventsPerJob), events[0]["timestamp"].(string))
	})

	t.Run("returns empty slice for nonexistent job", func(t *testing.T) {
		em := NewJobEventMap()
		events := em.GetEventsByJobID("nonexistent")
		assert.NotNil(t, events)
		assert.Empty(t, events)
	})
}

func TestLogEventToAPIResponse(t *testing.T) {
	// Test data modeled after real Ray event log entries
	event := LogEvent{
		EventID:        "1Ac5EF8dC3B20F78233ca8D7Be329bF9D2CA",
		SourceType:     "JOBS",
		SourceHostname: "raycluster-historyserver-head-6gvqk",
		SourcePid:      568,
		Label:          "",
		Message:        "Started a ray job rayjob-brmng-rhrb7.",
		Timestamp:      "1770635705",
		Severity:       "INFO",
		CustomFields: map[string]any{
			"job_id":        "01000000",
			"submission_id": "rayjob-brmng-rhrb7",
		},
	}

	t.Run("converts fields to camelCase", func(t *testing.T) {
		response := event.ToAPIResponse()
		assert.Equal(t, "1Ac5EF8dC3B20F78233ca8D7Be329bF9D2CA", response["eventId"])
		assert.Equal(t, "JOBS", response["sourceType"])
		assert.Equal(t, "raycluster-historyserver-head-6gvqk", response["sourceHostname"])
		assert.Equal(t, 568, response["sourcePid"])
		assert.Empty(t, response["label"])
		assert.Equal(t, "INFO", response["severity"])
		assert.Equal(t, "1770635705", response["timestamp"])
		assert.Equal(t, "Started a ray job rayjob-brmng-rhrb7.", response["message"])

		cf, ok := response["customFields"].(map[string]any)
		require.True(t, ok)
		assert.Equal(t, "01000000", cf["jobId"])
		assert.Equal(t, "rayjob-brmng-rhrb7", cf["submissionId"])
	})

	t.Run("omits customFields when nil", func(t *testing.T) {
		e := LogEvent{EventID: "e1", Timestamp: "1770635705"}
		_, exists := e.ToAPIResponse()["customFields"]
		assert.False(t, exists)
	})

	t.Run("includes customFields when present but empty", func(t *testing.T) {
		e := LogEvent{EventID: "e1", Timestamp: "1770635705", CustomFields: map[string]any{}}
		cf, exists := e.ToAPIResponse()["customFields"]
		assert.True(t, exists)
		assert.Empty(t, cf)
	})
}

func TestLogEventGetJobID(t *testing.T) {
	tests := []struct {
		name         string
		customFields map[string]any
		want         string
	}{
		{"with job_id", map[string]any{"job_id": "01000000"}, "01000000"},
		{"no job_id key", map[string]any{"submission_id": "rayjob-abc"}, "global"},
		{"nil custom_fields", nil, "global"},
		{"empty job_id", map[string]any{"job_id": ""}, "global"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			event := LogEvent{EventID: "e1", CustomFields: tc.customFields}
			assert.Equal(t, tc.want, event.GetJobID())
		})
	}
}

func TestLogEventStoreGetLogEventsByJobIDDeepCopy(t *testing.T) {
	store := NewLogEventStore()
	const clusterSessionKey = "raycluster-test_default_session_2026-04-22_10-00-00_000000_1"

	m := store.GetOrCreateJobEventMap(clusterSessionKey)
	m.AddEvent(&LogEvent{
		EventID:      "e1",
		Timestamp:    "1770635705",
		CustomFields: map[string]any{"job_id": "01000000"},
	})

	copied := store.GetLogEventsByJobID(clusterSessionKey)
	require.Len(t, copied["01000000"], 1)

	copied["01000000"][0].Message = "mutated"
	copied["01000000"][0].CustomFields["job_id"] = "changed"

	again := store.GetLogEventsByJobID(clusterSessionKey)
	assert.NotEqual(t, "mutated", again["01000000"][0].Message)
	assert.Equal(t, "01000000", again["01000000"][0].CustomFields["job_id"])
}

func TestLogEventRestoreNewline(t *testing.T) {
	event := LogEvent{Message: "line1\\nline2\\rline3"}
	event.RestoreNewline()
	assert.Equal(t, "line1\nline2\nline3", event.Message)
}

func TestToCamelCase(t *testing.T) {
	tests := []struct {
		input, expected string
	}{
		{"source_hostname", "sourceHostname"},
		{"source_pid", "sourcePid"},
		{"job_id", "jobId"},
		{"single", "single"},
		{"", ""},
		{"a_b_c", "aBC"},
	}
	for _, tc := range tests {
		assert.Equal(t, tc.expected, toCamelCase(tc.input), "toCamelCase(%q)", tc.input)
	}
}

func TestToGoogleStyle(t *testing.T) {
	result := toGoogleStyle(map[string]any{
		"job_id":        "01000000",
		"submission_id": "rayjob-brmng-rhrb7",
		"nested_field":  map[string]any{"inner_key": "value"},
	})

	assert.Equal(t, "01000000", result["jobId"])
	assert.Equal(t, "rayjob-brmng-rhrb7", result["submissionId"])
	assert.Equal(t, "value", result["nestedField"].(map[string]any)["innerKey"])
}
