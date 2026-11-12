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

// TestApplyTaskFiltersMatchesStringsCaseInsensitively mirrors Ray's state API,
// which lowercases the filter column and compares string values case-insensitively.
func TestApplyTaskFiltersMatchesStringsCaseInsensitively(t *testing.T) {
	tasks := []eventtypes.Task{
		{TaskID: "a", TaskName: "train", State: eventtypes.FINISHED, TaskType: eventtypes.NORMAL_TASK},
		{TaskID: "b", TaskName: "eval", State: eventtypes.FAILED, TaskType: eventtypes.NORMAL_TASK},
	}

	tests := []struct {
		name    string
		filter  Filter
		wantIDs []string
	}{
		{"lowercase value with equal", Filter{FilterKey: "state", FilterPredicate: PredicateEqual, FilterValue: "finished"}, []string{"a"}},
		{"mixed case value with not equal", Filter{FilterKey: "state", FilterPredicate: PredicateNotEqual, FilterValue: "Finished"}, []string{"b"}},
		{"uppercase key", Filter{FilterKey: "TASK_NAME", FilterPredicate: PredicateEqual, FilterValue: "TRAIN"}, []string{"a"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, numFiltered := ApplyTaskFilters(tasks, ListAPIOptions{Limit: DefaultLimit, Filters: []Filter{tc.filter}})
			ids := make([]string, 0, len(got))
			for _, task := range got {
				ids = append(ids, task.TaskID)
			}
			assert.Equal(t, tc.wantIDs, ids)
			assert.Equal(t, len(tc.wantIDs), numFiltered)
		})
	}
}

// GetFilterableFieldValue maps task_name to ActorTaskName for actor tasks,
// and task_type to the TaskType enum. Both should still match the way Ray
// compares filter columns and string values case-insensitively.
func TestApplyTaskFiltersMatchesActorNameAndTypeCaseInsensitively(t *testing.T) {
	tasks := []eventtypes.Task{
		{TaskID: "a", TaskName: "train", State: eventtypes.FINISHED, TaskType: eventtypes.NORMAL_TASK},
		{TaskID: "c", TaskName: "ignored", ActorTaskName: "Worker.forward", State: eventtypes.FINISHED, TaskType: eventtypes.ACTOR_TASK},
	}

	tests := []struct {
		name    string
		filters []Filter
		wantIDs []string
	}{
		{
			"actor task name uses ActorTaskName",
			[]Filter{{FilterKey: "task_name", FilterPredicate: PredicateEqual, FilterValue: "worker.forward"}},
			[]string{"c"},
		},
		{
			"task type mixed case",
			[]Filter{{FilterKey: "TASK_TYPE", FilterPredicate: PredicateEqual, FilterValue: "actor_task"}},
			[]string{"c"},
		},
		{
			"type and actor name together",
			[]Filter{
				{FilterKey: "Task_Type", FilterPredicate: PredicateEqual, FilterValue: "Actor_Task"},
				{FilterKey: "TASK_NAME", FilterPredicate: PredicateEqual, FilterValue: "WORKER.FORWARD"},
			},
			[]string{"c"},
		},
		{
			"actor task name does not fall back to TaskName",
			[]Filter{{FilterKey: "task_name", FilterPredicate: PredicateEqual, FilterValue: "ignored"}},
			[]string{},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, numFiltered := ApplyTaskFilters(tasks, ListAPIOptions{Limit: DefaultLimit, Filters: tc.filters})
			ids := make([]string, 0, len(got))
			for _, task := range got {
				ids = append(ids, task.TaskID)
			}
			assert.Equal(t, tc.wantIDs, ids)
			assert.Equal(t, len(tc.wantIDs), numFiltered)
		})
	}
}
