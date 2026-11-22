package utils

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	eventtypes "github.com/ray-project/kuberay/historyserver/pkg/eventserver/types"
)

// --- Test helpers ---

const (
	// testDriverTaskID mimics a real driver task ID: the 20-byte 0xFF prefix followed by a job ID.
	testDriverTaskID = DriverTaskIDPrefix + "01000000"
	testActorID      = "a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1"
	testActorID2     = "b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2"
)

func pyFunc(className, functionName string) *eventtypes.FunctionDescriptor {
	return &eventtypes.FunctionDescriptor{
		PythonFunctionDescriptor: &eventtypes.PythonFunctionDescriptor{
			ClassName:    className,
			FunctionName: functionName,
		},
	}
}

func normalTask(taskID, parentTaskID, name string, state eventtypes.TaskStatus) eventtypes.Task {
	return eventtypes.Task{
		TaskID:       taskID,
		TaskType:     eventtypes.NORMAL_TASK,
		TaskName:     name,
		TaskFunc:     pyFunc("", name),
		ParentTaskID: parentTaskID,
		State:        state,
	}
}

// actorCreationTask builds an ACTOR_CREATION_TASK whose task ID follows Ray's
// ffffffffffffffff{actorID} format.
func actorCreationTask(actorID, parentTaskID, className string) eventtypes.Task {
	return eventtypes.Task{
		TaskID:       actorCreationTaskIDForActorIDPrefix + actorID,
		TaskType:     eventtypes.ACTOR_CREATION_TASK,
		TaskFunc:     pyFunc(className, "__init__"),
		ActorID:      actorID,
		ParentTaskID: parentTaskID,
		State:        eventtypes.FINISHED,
	}
}

func actorTask(taskID, actorID, className, methodName string, state eventtypes.TaskStatus) eventtypes.Task {
	return eventtypes.Task{
		TaskID:        taskID,
		TaskType:      eventtypes.ACTOR_TASK,
		ActorTaskName: methodName,
		ActorFunc:     pyFunc(className, methodName),
		ActorID:       actorID,
		State:         state,
	}
}

func withCreationTime(task eventtypes.Task, unixMilli int64) eventtypes.Task {
	task.CreationTime = time.UnixMilli(unixMilli)
	return task
}

func findNode(t *testing.T, nodes []*NestedTaskSummary, name string) *NestedTaskSummary {
	t.Helper()
	for _, n := range nodes {
		if n.Name == name {
			return n
		}
	}
	require.FailNowf(t, "node not found", "no node named %q in %v", name, nodeNames(nodes))
	return nil
}

func nodeNames(nodes []*NestedTaskSummary) []string {
	names := make([]string, 0, len(nodes))
	for _, n := range nodes {
		names = append(names, n.Name)
	}
	return names
}

// nodeKeys is used instead of nodeNames when the nodes share a name, e.g. two actors
// of the same class.
func nodeKeys(nodes []*NestedTaskSummary) []string {
	keys := make([]string, 0, len(nodes))
	for _, n := range nodes {
		keys = append(keys, n.Key)
	}
	return keys
}

// --- Tests ---

func TestIsDriverTaskID(t *testing.T) {
	tests := []struct {
		scenario string
		taskID   string
		expected bool
	}{
		{scenario: "empty task ID is not a driver task", taskID: "", expected: false},
		{scenario: "exact driver prefix", taskID: DriverTaskIDPrefix, expected: true},
		{scenario: "driver prefix followed by job ID", taskID: testDriverTaskID, expected: true},
		{scenario: "actor creation task prefix is only 8 bytes and must not match", taskID: actorCreationTaskIDForActorIDPrefix + testActorID, expected: false},
		{scenario: "regular task ID", taskID: "1234567890abcdef", expected: false},
	}

	for _, tt := range tests {
		t.Run(tt.scenario, func(t *testing.T) {
			assert.Equal(t, tt.expected, isDriverTaskID(tt.taskID))
		})
	}
}

func TestToSummaryByLineageEmptyInput(t *testing.T) {
	got := ToSummaryByLineage(nil, nil)

	require.NotNil(t, got)
	assert.Empty(t, got.Summary)
	assert.Equal(t, 0, got.TotalTasks)
	assert.Equal(t, 0, got.TotalActorTasks)
	assert.Equal(t, 0, got.TotalActorScheduled)
	assert.Equal(t, "lineage", got.SummaryBy)
}

func TestToSummaryByLineageNestedNormalTasks(t *testing.T) {
	tasks := []eventtypes.Task{
		// The driver task itself must be excluded from the tree.
		{TaskID: testDriverTaskID, TaskType: eventtypes.DRIVER_TASK, TaskName: "driver"},
		normalTask("task1", testDriverTaskID, "parent", eventtypes.RUNNING),
		normalTask("task2", "task1", "child", eventtypes.FINISHED),
	}

	got := ToSummaryByLineage(tasks, nil)

	require.Len(t, got.Summary, 1, "only the root task should be at the top level")
	root := got.Summary[0]
	assert.Equal(t, "parent", root.Name)
	assert.Equal(t, "task1", root.Key)
	assert.Equal(t, string(eventtypes.NORMAL_TASK), root.Type)
	assert.Equal(t, &Link{Type: "task", ID: "task1"}, root.Link)

	require.Len(t, root.Children, 1)
	child := root.Children[0]
	assert.Equal(t, "child", child.Name)
	assert.Equal(t, "task2", child.Key)

	// The DRIVER_TASK is skipped, so it is not counted.
	assert.Equal(t, 2, got.TotalTasks)
	assert.Equal(t, 0, got.TotalActorTasks)
	assert.Equal(t, 0, got.TotalActorScheduled)
}

func TestToSummaryByLineageTaskNameFallsBackToFuncName(t *testing.T) {
	tasks := []eventtypes.Task{
		{
			TaskID:       "task1",
			TaskType:     eventtypes.NORMAL_TASK,
			TaskName:     "explicit_name",
			TaskFunc:     pyFunc("MyClass", "my_func"),
			ParentTaskID: testDriverTaskID,
			State:        eventtypes.FINISHED,
		},
		{
			TaskID:       "task2",
			TaskType:     eventtypes.NORMAL_TASK,
			TaskFunc:     pyFunc("MyClass", "my_func"),
			ParentTaskID: testDriverTaskID,
			State:        eventtypes.FINISHED,
		},
	}

	got := ToSummaryByLineage(tasks, nil)

	require.Len(t, got.Summary, 2)
	assert.NotNil(t, findNode(t, got.Summary, "explicit_name"), "task name should win when set")
	assert.NotNil(t, findNode(t, got.Summary, "MyClass.my_func"), "should fall back to the function call string")
}

func TestToSummaryByLineageGroupsActorTasksUnderActorEntry(t *testing.T) {
	tasks := []eventtypes.Task{
		actorCreationTask(testActorID, testDriverTaskID, "Counter"),
		actorTask("task1", testActorID, "Counter", "increment", eventtypes.FINISHED),
		actorTask("task2", testActorID, "Counter", "get", eventtypes.RUNNING),
	}
	actors := []eventtypes.Actor{{ActorID: testActorID, ActorClass: "Counter"}}

	got := ToSummaryByLineage(tasks, actors)

	require.Len(t, got.Summary, 1, "the actor entry should be the only root node")
	actorNode := got.Summary[0]
	assert.Equal(t, "Counter", actorNode.Name)
	assert.Equal(t, "actor:"+testActorID, actorNode.Key)
	assert.Equal(t, "ACTOR", actorNode.Type)
	assert.Equal(t, &Link{Type: "actor", ID: testActorID}, actorNode.Link)

	// The creation task and both method tasks live under the ACTOR entry, ordered by
	// sortTaskGroups: the RUNNING call first, then the creation task ahead of the other
	// FINISHED sibling.
	require.Len(t, actorNode.Children, 3)
	assert.Equal(t, []string{"get", "Counter.__init__", "increment"}, nodeNames(actorNode.Children))

	assert.Equal(t, 0, got.TotalTasks)
	assert.Equal(t, 2, got.TotalActorTasks)
	assert.Equal(t, 1, got.TotalActorScheduled)
}

func TestToSummaryByLineageGroupsMultipleActorsSeparately(t *testing.T) {
	// Each ACTOR entry owns only its own tasks, even when two actors share a method name.
	tasks := []eventtypes.Task{
		actorCreationTask(testActorID, testDriverTaskID, "Counter"),
		actorCreationTask(testActorID2, testDriverTaskID, "Worker"),
		actorTask("task1", testActorID, "Counter", "increment", eventtypes.FINISHED),
		actorTask("task2", testActorID2, "Worker", "increment", eventtypes.RUNNING),
		actorTask("task3", testActorID2, "Worker", "shutdown", eventtypes.FAILED),
	}
	actors := []eventtypes.Actor{
		{ActorID: testActorID, ActorClass: "Counter"},
		{ActorID: testActorID2, ActorClass: "Worker"},
	}

	got := ToSummaryByLineage(tasks, actors)

	require.Len(t, got.Summary, 2)

	counter := findNode(t, got.Summary, "Counter")
	assert.Equal(t, "actor:"+testActorID, counter.Key)
	assert.Equal(t, []string{"Counter.__init__", "increment"}, nodeNames(counter.Children))
	assert.Equal(t, map[string]int{"FINISHED": 2}, counter.StateCounts)

	worker := findNode(t, got.Summary, "Worker")
	assert.Equal(t, "actor:"+testActorID2, worker.Key)
	// The RUNNING call sorts first, then the FAILED one, then the creation task.
	assert.Equal(t, []string{"increment", "shutdown", "Worker.__init__"}, nodeNames(worker.Children))
	assert.Equal(t, map[string]int{"FINISHED": 1, "RUNNING": 1, "FAILED": 1}, worker.StateCounts)

	assert.Equal(t, 0, got.TotalTasks)
	assert.Equal(t, 3, got.TotalActorTasks)
	assert.Equal(t, 2, got.TotalActorScheduled)
}

func TestToSummaryByLineageMergesSameClassActorsIntoGroup(t *testing.T) {
	// Two actors of the same class are same-named siblings at the root, so they collapse
	// into a GROUP just like same-named tasks do.
	tasks := []eventtypes.Task{
		actorCreationTask(testActorID, testDriverTaskID, "Counter"),
		actorCreationTask(testActorID2, testDriverTaskID, "Counter"),
		actorTask("task1", testActorID, "Counter", "increment", eventtypes.FINISHED),
		actorTask("task2", testActorID2, "Counter", "increment", eventtypes.RUNNING),
	}
	actors := []eventtypes.Actor{
		{ActorID: testActorID, ActorClass: "Counter"},
		{ActorID: testActorID2, ActorClass: "Counter"},
	}

	got := ToSummaryByLineage(tasks, actors)

	require.Len(t, got.Summary, 1)
	group := got.Summary[0]
	assert.Equal(t, "GROUP", group.Type)
	assert.Equal(t, "Counter", group.Name)
	assert.Nil(t, group.Link)

	require.Len(t, group.Children, 2)
	// The actor with a RUNNING method call sorts ahead of the fully finished one.
	assert.Equal(t,
		[]string{"actor:" + testActorID2, "actor:" + testActorID},
		nodeKeys(group.Children),
	)

	// The GROUP aggregates both actors' creation tasks and method calls.
	assert.Equal(t, map[string]int{"FINISHED": 3, "RUNNING": 1}, group.StateCounts)
	assert.Equal(t, 2, got.TotalActorTasks)
	assert.Equal(t, 2, got.TotalActorScheduled)
}

func TestToSummaryByLineageActorNameResolution(t *testing.T) {
	tests := []struct {
		scenario     string
		actors       []eventtypes.Actor
		className    string
		expectedName string
	}{
		{
			scenario:     "repr name wins",
			actors:       []eventtypes.Actor{{ActorID: testActorID, ActorClass: "Counter", ReprName: "Counter(id=1)"}},
			className:    "Counter",
			expectedName: "Counter(id=1)",
		},
		{
			scenario:     "falls back to actor class",
			actors:       []eventtypes.Actor{{ActorID: testActorID, ActorClass: "Counter"}},
			className:    "Counter",
			expectedName: "Counter",
		},
		{
			scenario:     "falls back to the creation task class name when the actor is unknown",
			actors:       nil,
			className:    "Counter",
			expectedName: "Counter",
		},
		{
			scenario:     "falls back to UnknownActor when nothing is available",
			actors:       nil,
			className:    "",
			expectedName: "UnknownActor",
		},
	}

	for _, tt := range tests {
		t.Run(tt.scenario, func(t *testing.T) {
			creation := actorCreationTask(testActorID, testDriverTaskID, tt.className)
			if tt.className == "" {
				// No function descriptor at all, so GetFuncName() returns an empty string.
				creation.TaskFunc = nil
			}

			got := ToSummaryByLineage([]eventtypes.Task{creation}, tt.actors)

			require.Len(t, got.Summary, 1)
			assert.Equal(t, tt.expectedName, got.Summary[0].Name)
		})
	}
}

func TestToSummaryByLineageDerivesActorIDFromCreationTaskID(t *testing.T) {
	// TASK_DEFINITION_EVENT does not carry an actorId, so it has to be derived
	// from the ffffffffffffffff{actorID} task ID format.
	creation := actorCreationTask(testActorID, testDriverTaskID, "Counter")
	creation.ActorID = ""

	tasks := []eventtypes.Task{
		creation,
		actorTask("task1", testActorID, "Counter", "increment", eventtypes.FINISHED),
	}

	got := ToSummaryByLineage(tasks, nil)

	require.Len(t, got.Summary, 1)
	actorNode := got.Summary[0]
	assert.Equal(t, "actor:"+testActorID, actorNode.Key)
	assert.Len(t, actorNode.Children, 2, "creation task and method task should both be attached")
}

func TestToSummaryByLineageSkipsTaskWithUnknownParent(t *testing.T) {
	// The History Server can ingest a child task without its parent's definition event.
	// getOrCreateTaskGroup then returns nil for the unknown parent, so the child never
	// gets attached to the tree, even though it is still counted.
	tasks := []eventtypes.Task{
		normalTask("task1", "0123456789abcdef0123456789abcdef", "orphan", eventtypes.RUNNING),
	}

	got := ToSummaryByLineage(tasks, nil)

	assert.Empty(t, got.Summary, "a task whose parent is unknown has no place in the tree")
	assert.Equal(t, 1, got.TotalTasks)
}

func TestToSummaryByLineageSkipsActorWithoutCreationTask(t *testing.T) {
	// Ray's get_or_create_actor_task_group returns None when the creation task is
	// missing, so the actor task is dropped from the tree but still counted.
	tasks := []eventtypes.Task{
		actorTask("task1", testActorID, "Counter", "increment", eventtypes.FINISHED),
	}

	got := ToSummaryByLineage(tasks, nil)

	assert.Empty(t, got.Summary, "an actor task without a creation task has no place in the tree")
	assert.Equal(t, 1, got.TotalActorTasks)
}

func TestToSummaryByLineageMergesSameNamedSiblings(t *testing.T) {
	tasks := []eventtypes.Task{
		normalTask("task1", testDriverTaskID, "duplicated", eventtypes.FINISHED),
		normalTask("task2", testDriverTaskID, "duplicated", eventtypes.FINISHED),
		normalTask("task3", testDriverTaskID, "unique", eventtypes.FINISHED),
	}

	got := ToSummaryByLineage(tasks, nil)

	require.Len(t, got.Summary, 2)

	group := findNode(t, got.Summary, "duplicated")
	assert.Equal(t, "GROUP", group.Type)
	assert.Equal(t, "duplicated", group.Key)
	assert.Nil(t, group.Link, "GROUP nodes must serialize with a null link")
	assert.Len(t, group.Children, 2)

	single := findNode(t, got.Summary, "unique")
	assert.Equal(t, string(eventtypes.NORMAL_TASK), single.Type, "a single sibling must not be wrapped in a GROUP")
	assert.NotNil(t, single.Link)
}

func TestToSummaryByLineageMergesSameNamedActorTasksIntoGroup(t *testing.T) {
	// Two calls to the same actor method must collapse into a GROUP nested inside the
	// ACTOR entry, and the ACTOR entry must aggregate state counts through that GROUP.
	tasks := []eventtypes.Task{
		actorCreationTask(testActorID, testDriverTaskID, "Counter"),
		actorTask("task1", testActorID, "Counter", "increment", eventtypes.FINISHED),
		actorTask("task2", testActorID, "Counter", "increment", eventtypes.RUNNING),
	}
	actors := []eventtypes.Actor{{ActorID: testActorID, ActorClass: "Counter"}}

	got := ToSummaryByLineage(tasks, actors)

	require.Len(t, got.Summary, 1)
	actorNode := got.Summary[0]
	assert.Equal(t, "ACTOR", actorNode.Type)
	require.Len(t, actorNode.Children, 2, "the creation task plus one GROUP of increment calls")

	group := findNode(t, actorNode.Children, "increment")
	assert.Equal(t, "GROUP", group.Type)
	assert.Nil(t, group.Link)
	assert.Len(t, group.Children, 2)
	assert.Equal(t, map[string]int{"FINISHED": 1, "RUNNING": 1}, group.StateCounts)

	// The ACTOR entry sums its creation task plus everything inside the GROUP.
	assert.Equal(t, map[string]int{"FINISHED": 2, "RUNNING": 1}, actorNode.StateCounts)
	assert.Equal(t, 2, got.TotalActorTasks)
	assert.Equal(t, 1, got.TotalActorScheduled)
}

func TestToSummaryByLineageAggregatesStateCounts(t *testing.T) {
	tasks := []eventtypes.Task{
		normalTask("task1", testDriverTaskID, "parent", eventtypes.RUNNING),
		normalTask("task2", "task1", "child", eventtypes.FINISHED),
		normalTask("task3", "task1", "child", eventtypes.FAILED),
	}

	got := ToSummaryByLineage(tasks, nil)

	require.Len(t, got.Summary, 1)
	parent := got.Summary[0]
	// The parent keeps its own state and sums both children.
	assert.Equal(t, map[string]int{"RUNNING": 1, "FINISHED": 1, "FAILED": 1}, parent.StateCounts)

	require.Len(t, parent.Children, 1)
	group := parent.Children[0]
	assert.Equal(t, "GROUP", group.Type)
	assert.Equal(t, map[string]int{"FINISHED": 1, "FAILED": 1}, group.StateCounts)
}

func TestToSummaryByLineageEmptyStateIsCountedAsNIL(t *testing.T) {
	tasks := []eventtypes.Task{
		normalTask("task1", testDriverTaskID, "no_state", ""),
	}

	got := ToSummaryByLineage(tasks, nil)

	require.Len(t, got.Summary, 1)
	assert.Equal(t, map[string]int{"NIL": 1}, got.Summary[0].StateCounts)
}

func TestToSummaryByLineageActorEntryTakesTimestampFromCreationTask(t *testing.T) {
	// The ACTOR entry is synthetic, so its timestamp has to come from the creation task:
	// not from an unrelated task that started earlier, and not from the actor's own later
	// method call. Note that mergeSiblingsForTaskGroup would also pull the creation task's
	// timestamp up into the ACTOR entry, so this pins the resulting value rather than the
	// exact code path that produced it.
	tasks := []eventtypes.Task{
		withCreationTime(actorCreationTask(testActorID, testDriverTaskID, "Counter"), 3000),
		withCreationTime(actorTask("task1", testActorID, "Counter", "increment", eventtypes.FINISHED), 7000),
		withCreationTime(normalTask("task2", testDriverTaskID, "unrelated", eventtypes.FINISHED), 1000),
	}

	got := ToSummaryByLineage(tasks, nil)

	require.Len(t, got.Summary, 2)
	actorNode := findNode(t, got.Summary, "Counter")
	assert.Equal(t, "ACTOR", actorNode.Type)
	require.NotNil(t, actorNode.Timestamp)
	assert.Equal(t, int64(3000), *actorNode.Timestamp)
}

func TestToSummaryByLineageSortsRunningBeforePendingBeforeFailed(t *testing.T) {
	tasks := []eventtypes.Task{
		normalTask("task1", testDriverTaskID, "finished_task", eventtypes.FINISHED),
		normalTask("task2", testDriverTaskID, "running_task", eventtypes.RUNNING),
		normalTask("task3", testDriverTaskID, "pending_task", eventtypes.PENDING_NODE_ASSIGNMENT),
		normalTask("task4", testDriverTaskID, "failed_task", eventtypes.FAILED),
	}

	got := ToSummaryByLineage(tasks, nil)

	assert.Equal(t,
		[]string{"running_task", "pending_task", "failed_task", "finished_task"},
		nodeNames(got.Summary),
	)
}

func TestToSummaryByLineageSortsByTimestampWhenPriorityIsEqual(t *testing.T) {
	tasks := []eventtypes.Task{
		withCreationTime(normalTask("task1", testDriverTaskID, "later", eventtypes.FINISHED), 2000),
		withCreationTime(normalTask("task2", testDriverTaskID, "earlier", eventtypes.FINISHED), 1000),
	}

	got := ToSummaryByLineage(tasks, nil)

	assert.Equal(t, []string{"earlier", "later"}, nodeNames(got.Summary))
}

func TestToSummaryByLineagePropagatesMinChildTimestampUpward(t *testing.T) {
	// The child started before its parent's recorded creation time, so the parent
	// node must adopt the earlier timestamp.
	tasks := []eventtypes.Task{
		withCreationTime(normalTask("task1", testDriverTaskID, "parent", eventtypes.FINISHED), 5000),
		withCreationTime(normalTask("task2", "task1", "child", eventtypes.FINISHED), 1000),
	}

	got := ToSummaryByLineage(tasks, nil)

	require.Len(t, got.Summary, 1)
	parent := got.Summary[0]
	require.NotNil(t, parent.Timestamp)
	assert.Equal(t, int64(1000), *parent.Timestamp)
}

func TestToSummaryByLineageActorsAreNestedUnderTheirParentTask(t *testing.T) {
	// An actor created inside a normal task must appear under that task, not at the root.
	tasks := []eventtypes.Task{
		normalTask("task1", testDriverTaskID, "creator", eventtypes.FINISHED),
		actorCreationTask(testActorID, "task1", "Counter"),
		actorCreationTask(testActorID2, testDriverTaskID, "Worker"),
	}

	got := ToSummaryByLineage(tasks, nil)

	require.Len(t, got.Summary, 2)
	creator := findNode(t, got.Summary, "creator")
	require.Len(t, creator.Children, 1)
	assert.Equal(t, "actor:"+testActorID, creator.Children[0].Key)

	rootActor := findNode(t, got.Summary, "Worker")
	assert.Equal(t, "ACTOR", rootActor.Type)
	assert.Equal(t, 2, got.TotalActorScheduled)
}
