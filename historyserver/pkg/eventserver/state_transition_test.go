package eventserver

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type mockT struct {
	state     string
	timestamp time.Time
}

func (m mockT) GetState() string        { return m.state }
func (m mockT) GetTimestamp() time.Time { return m.timestamp }

func TestMergeStateTransitions(t *testing.T) {
	t1 := time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC)
	t2 := time.Date(2026, 1, 1, 11, 0, 0, 0, time.UTC)
	t3 := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

	tests := []struct {
		name     string
		existing []mockT
		new      []mockT
		expected []mockT
	}{
		{
			name:     "merge_and_sort",
			existing: []mockT{{state: "PENDING", timestamp: t1}, {state: "STOPPED", timestamp: t3}},
			new:      []mockT{{state: "RUNNING", timestamp: t2}},
			expected: []mockT{{state: "PENDING", timestamp: t1}, {state: "RUNNING", timestamp: t2}, {state: "STOPPED", timestamp: t3}},
		},
		{
			name:     "deduplicate_same_state_and_timestamp",
			existing: []mockT{{state: "PENDING", timestamp: t1}},
			new:      []mockT{{state: "PENDING", timestamp: t1}, {state: "RUNNING", timestamp: t2}},
			expected: []mockT{{state: "PENDING", timestamp: t1}, {state: "RUNNING", timestamp: t2}},
		},
		{
			name:     "same_state_different_timestamp",
			existing: []mockT{{state: "PENDING", timestamp: t1}},
			new:      []mockT{{state: "PENDING", timestamp: t2}},
			expected: []mockT{{state: "PENDING", timestamp: t1}, {state: "PENDING", timestamp: t2}},
		},
		{
			name:     "different_state_same_timestamp",
			existing: []mockT{{state: "PENDING", timestamp: t1}},
			new:      []mockT{{state: "RUNNING", timestamp: t1}},
			expected: []mockT{{state: "PENDING", timestamp: t1}, {state: "RUNNING", timestamp: t1}},
		},
		{
			name:     "empty",
			existing: []mockT{},
			new:      []mockT{},
			expected: []mockT{},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expected, MergeStateTransitions(tc.existing, tc.new))
		})
	}
}
