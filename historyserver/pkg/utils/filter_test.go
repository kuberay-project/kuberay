package utils

import (
	"testing"

	"github.com/stretchr/testify/assert"

	eventtypes "github.com/ray-project/kuberay/historyserver/pkg/eventserver/types"
)

// TestApplyTaskFiltersDoesNotMutateInput verifies that sorting never writes
// into the caller's slice, which may alias a shared cached snapshot.
func TestApplyTaskFiltersDoesNotMutateInput(t *testing.T) {
	tasks := []eventtypes.Task{
		{TaskID: "c", TaskAttempt: 0},
		{TaskID: "a", TaskAttempt: 1},
		{TaskID: "a", TaskAttempt: 0},
		{TaskID: "b", TaskAttempt: 0},
	}
	original := make([]eventtypes.Task, len(tasks))
	copy(original, tasks)

	// ExcludeDriver=false with no filters is the path where the input would
	// otherwise be sorted in place.
	sorted, numFiltered := ApplyTaskFilters(tasks, ListAPIOptions{ExcludeDriver: false, Limit: DefaultLimit})

	assert.Equal(t, original, tasks, "input slice must not be mutated")
	assert.Equal(t, len(original), numFiltered)
	assert.Equal(t, []eventtypes.Task{
		{TaskID: "a", TaskAttempt: 0},
		{TaskID: "a", TaskAttempt: 1},
		{TaskID: "b", TaskAttempt: 0},
		{TaskID: "c", TaskAttempt: 0},
	}, sorted)
}
