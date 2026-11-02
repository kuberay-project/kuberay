package utils

import (
	"math"
	"sort"
	"strings"

	"github.com/sirupsen/logrus"

	eventtypes "github.com/ray-project/kuberay/historyserver/pkg/eventserver/types"
)

// --- Lineage Summary Types ---

// Link represents a navigation reference to a task or actor detail page
// Ref: https://github.com/ray-project/ray/blob/f3d444ab01279a3870033fb4d34314cd8c987b22/python/ray/util/state/common.py#L994-L998
type Link struct {
	Type string `json:"type"` // task or actor
	ID   string `json:"id"`   // task ID or actor ID
}

// NestedTaskSummary represents an entry in the task lineage tree.
// An entry can be:
// - a task (type = NORMAL_TASK, ACTOR_TASK, or ACTOR_CREATION_TASK)
// - an actor grouping its creation and method tasks (type = ACTOR)
// - a group of same-named siblings, created when >1 sibling shares a name (type = GROUP)
// Ref: https://github.com/ray-project/ray/blob/f3d444ab01279a3870033fb4d34314cd8c987b22/python/ray/util/state/common.py#L1001-L1018
type NestedTaskSummary struct {
	Name        string               `json:"name"`
	Key         string               `json:"key"`
	Type        string               `json:"type"`
	Timestamp   *int64               `json:"timestamp"`
	StateCounts map[string]int       `json:"state_counts"`
	Children    []*NestedTaskSummary `json:"children"`
	// Use `json:"link"` without omitempty so that GROUP nodes serialize as "link": null
	// instead of omitting the field entirely. This matches Ray Dashboard's behavior:
	// Ray's NestedTaskSummary defines `link: Optional[Link] = None`, and Python's
	// dataclasses.asdict() serializes None as JSON null (never omits the field).
	// GROUP nodes are created without a link, so they always have "link": null.
	// Ref: https://github.com/ray-project/ray/blob/d0b1d151d8ea964a711e451d0ae736f8bf95b629/python/ray/util/state/common.py#L1001-L1018
	Link *Link `json:"link"`
}

// TaskSummaries is the response for summary_by=lineage
// Ref: https://github.com/ray-project/ray/blob/f3d444ab01279a3870033fb4d34314cd8c987b22/python/ray/util/state/common.py#L1022-L1033
type TaskSummaries struct {
	Summary             []*NestedTaskSummary `json:"summary"`
	TotalTasks          int                  `json:"total_tasks"`
	TotalActorTasks     int                  `json:"total_actor_tasks"`
	TotalActorScheduled int                  `json:"total_actor_scheduled"`
	SummaryBy           string               `json:"summary_by"`
}

// DriverTaskIDPrefix is the hex prefix of the driver's task ID (20 bytes of 0xFF).
// In Ray, tasks whose parent_task_id starts with this prefix are spawned directly
// by the driver and should be placed at the root level of the lineage tree.
// Ref: https://github.com/ray-project/ray/blob/d0b1d151d8ea964a711e451d0ae736f8bf95b629/python/ray/util/state/common.py#L979
const DriverTaskIDPrefix = "ffffffffffffffffffffffffffffffffffffffff"

// actorCreationTaskIDForActorIDPrefix is the hex representation of the 8-byte unique bytes
// that Ray uses as the prefix of ACTOR_CREATION_TASK task IDs.
const actorCreationTaskIDForActorIDPrefix = "ffffffffffffffff"

// lineageBuilder encapsulates the state needed to build a lineage tree.
// Ref: https://github.com/ray-project/ray/blob/f3d444ab01279a3870033fb4d34314cd8c987b22/python/ray/util/state/common.py#L1098-L1118
type lineageBuilder struct {
	tasksByID                     map[string]eventtypes.Task  // taskID -> Task
	actorDict                     map[string]eventtypes.Actor // actorID -> Actor for name resolution
	actorCreationTaskIDForActorID map[string]string           // actorID -> creation taskID (to find actor's parent in lineage)
	taskGroupByID                 map[string]*NestedTaskSummary
	summary                       []*NestedTaskSummary
	totalTasks                    int // NORMAL_TASK count
	totalActorTasks               int // ACTOR_TASK count
	totalActorScheduled           int // ACTOR_CREATION_TASK count
}

// isDriverTaskID checks if the given taskID belongs to a driver task.
// Task IDs are already normalized to hex by normalizeTaskIDsToHex during event ingestion,
// so we simply check the hex prefix directly.
func isDriverTaskID(taskID string) bool {
	return taskID != "" && strings.HasPrefix(taskID, DriverTaskIDPrefix)
}

// ToSummaryByLineage constructs a hierarchical task summary following Ray's lineage algorithm.
// The algorithm has 5 steps:
//  1. Index all tasks by ID and track actor creation tasks
//  2. Build tree structure based on task ownership (actor tasks -> actor entry, others -> parent task)
//  3. Merge siblings with the same name into GROUP nodes
//  4. Calculate total state_counts by summing children recursively
//  5. Sort by running > pending > failed > timestamp > actor_creation
//
// Ref: https://github.com/ray-project/ray/blob/d0b1d151d8ea964a711e451d0ae736f8bf95b629/python/ray/util/state/common.py#L1075-L1375
func ToSummaryByLineage(tasks []eventtypes.Task, actors []eventtypes.Actor) *TaskSummaries {
	b := &lineageBuilder{
		tasksByID:                     make(map[string]eventtypes.Task),
		actorDict:                     make(map[string]eventtypes.Actor),
		actorCreationTaskIDForActorID: make(map[string]string),
		taskGroupByID:                 make(map[string]*NestedTaskSummary),
		summary:                       make([]*NestedTaskSummary, 0),
	}

	// Step 1: Index
	b.indexData(tasks, actors)

	// Step 2: Build tree
	b.buildTree(tasks)

	// Step 3: Merge siblings
	b.summary, _ = b.mergeSiblingsForTaskGroup(b.summary)

	// Step 4 & 5: Calculate totals and sort
	b.calcTotalForTaskGroup(b.summary)
	b.sortTaskGroups(b.summary)

	logrus.Debugf("Built lineage summary: %d root nodes, %d tasks, %d actor tasks, %d actors",
		len(b.summary), b.totalTasks, b.totalActorTasks, b.totalActorScheduled)

	return &TaskSummaries{
		Summary:             b.summary,
		TotalTasks:          b.totalTasks,
		TotalActorTasks:     b.totalActorTasks,
		TotalActorScheduled: b.totalActorScheduled,
		SummaryBy:           "lineage",
	}
}

// indexData builds lookup maps to access during tree construction.
// Also tracks ACTOR_CREATION_TASK -> actorID mapping to determine actor ownership in the tree.
// Ref: https://github.com/ray-project/ray/blob/d0b1d151d8ea964a711e451d0ae736f8bf95b629/python/ray/util/state/common.py#L1106-L1118
func (b *lineageBuilder) indexData(tasks []eventtypes.Task, actors []eventtypes.Actor) {
	for _, task := range tasks {
		// ACTOR_CREATION_TASK from TASK_DEFINITION_EVENT has no ActorID.
		// Derive it from task_id: ffffffffffffffff{actorID}
		// We modify the local copy (range value) instead of tasks[i] to avoid mutating the caller's slice.
		// Ref: https://github.com/ray-project/ray/blob/36be009ae360788550e541d81806493f52963730/src/ray/common/id.cc#L171-L176
		if task.TaskType == eventtypes.ACTOR_CREATION_TASK {
			if task.ActorID == "" && strings.HasPrefix(task.TaskID, actorCreationTaskIDForActorIDPrefix) {
				task.ActorID = task.TaskID[len(actorCreationTaskIDForActorIDPrefix):]
			}
			if task.ActorID != "" {
				b.actorCreationTaskIDForActorID[task.ActorID] = task.TaskID
			}
		}
		b.tasksByID[task.TaskID] = task
	}
	for _, actor := range actors {
		b.actorDict[actor.ActorID] = actor
	}
}

// buildTree iterates through all tasks, creates tree nodes, and updates state counts.
// NOTE: DRIVER_TASK is skipped because Ray's live Dashboard API excludes them from lineage.
func (b *lineageBuilder) buildTree(tasks []eventtypes.Task) {
	for _, task := range tasks {
		if task.TaskType == eventtypes.DRIVER_TASK {
			continue
		}

		group := b.getOrCreateTaskGroup(task.TaskID)
		if group == nil {
			continue
		}

		state := string(task.State)
		if state == "" {
			state = "NIL"
		}
		group.StateCounts[state]++

		switch task.TaskType {
		case eventtypes.NORMAL_TASK:
			b.totalTasks++
		case eventtypes.ACTOR_CREATION_TASK:
			b.totalActorScheduled++
		case eventtypes.ACTOR_TASK:
			b.totalActorTasks++
		}
	}
}

// getOrCreateTaskGroup returns the existing NestedTaskSummary for a task, or creates one.
//
// Parent assignment rules:
// - ACTOR_TASK / ACTOR_CREATION_TASK -> parent is the ACTOR
// - NORMAL_TASK with driver parent or no parent -> root level
// - NORMAL_TASK with valid parent -> nested under parent task
//
// Ref: https://github.com/ray-project/ray/blob/d0b1d151d8ea964a711e451d0ae736f8bf95b629/python/ray/util/state/common.py#L1120-L1177
func (b *lineageBuilder) getOrCreateTaskGroup(taskID string) *NestedTaskSummary {
	if existing, ok := b.taskGroupByID[taskID]; ok {
		return existing
	}

	task, ok := b.tasksByID[taskID]
	if !ok {
		logrus.Debugf("Missing task data for %s", taskID)
		return nil
	}

	// Prefer user-defined task name, fallback to function call string to match Ray Dashboard
	funcName := task.GetTaskName()
	if funcName == "" {
		funcName = task.GetFuncName()
	}

	var timestamp *int64
	if !task.CreationTime.IsZero() {
		ts := task.CreationTime.UnixMilli()
		timestamp = &ts
	}

	group := &NestedTaskSummary{
		Name:        funcName,
		Key:         taskID,
		Type:        string(task.TaskType),
		Timestamp:   timestamp,
		StateCounts: make(map[string]int),
		Children:    make([]*NestedTaskSummary, 0),
		Link:        &Link{Type: "task", ID: taskID},
	}
	b.taskGroupByID[taskID] = group

	// Determine parent based on task type:
	// Actor-related tasks are grouped under their ACTOR entry, not their parent task.
	if task.TaskType == eventtypes.ACTOR_TASK || task.TaskType == eventtypes.ACTOR_CREATION_TASK {
		parentTaskGroup := b.getOrCreateActorTaskGroup(task.ActorID)
		if parentTaskGroup != nil {
			parentTaskGroup.Children = append(parentTaskGroup.Children, group)
		}
	} else {
		// Normal tasks: place under parent task, or at root if parent is driver/missing
		parentTaskID := task.ParentTaskID
		if parentTaskID == "" || isDriverTaskID(parentTaskID) {
			// No parent or parent is driver -> root level
			b.summary = append(b.summary, group)
		} else {
			parentTaskGroup := b.getOrCreateTaskGroup(parentTaskID)
			if parentTaskGroup != nil {
				parentTaskGroup.Children = append(parentTaskGroup.Children, group)
			}
		}
	}

	return group
}

// getOrCreateActorTaskGroup returns the existing ACTOR entry, or creates one.
// The ACTOR entry acts as a container for all actor-related tasks (creation + method calls).
// Its position in the tree is determined by the creation task's parent, matching Ray's behavior.
//
// Actor name resolution order: ReprName -> ActorClass -> creation task's GetFuncName() -> "UnknownActor"
//
// Ref: https://github.com/ray-project/ray/blob/d0b1d151d8ea964a711e451d0ae736f8bf95b629/python/ray/util/state/common.py#L1179-L1236
func (b *lineageBuilder) getOrCreateActorTaskGroup(actorID string) *NestedTaskSummary {
	key := "actor:" + actorID
	if existing, ok := b.taskGroupByID[key]; ok {
		return existing
	}

	// Look up the creation task for this actor. If it doesn't exist, return nil
	// to skip this actor group entirely. This matches Ray's behavior:
	// Ray's get_or_create_actor_task_group returns None when creation_task is missing.
	// Ref: https://github.com/ray-project/ray/blob/d0b1d151d8ea964a711e451d0ae736f8bf95b629/python/ray/util/state/common.py#L1193-L1195
	creationTaskID, hasCreationTask := b.actorCreationTaskIDForActorID[actorID]
	if !hasCreationTask {
		logrus.Debugf("Missing creation task for actor %s, skipping actor group", actorID)
		return nil
	}
	creationTask, hasCreationTaskData := b.tasksByID[creationTaskID]
	if !hasCreationTaskData {
		logrus.Debugf("Missing task data for creation task %s of actor %s, skipping actor group", creationTaskID, actorID)
		return nil
	}

	// Find actor name: prefer ReprName, fallback to ActorClass
	var actorName string
	if actor, ok := b.actorDict[actorID]; ok {
		actorName = actor.ReprName
		if actorName == "" {
			actorName = actor.ActorClass
		}
	}

	// Fallback: extract class name from creation task's function name (e.g., "Counter.__init__" -> "Counter")
	if actorName == "" {
		funcName := creationTask.GetFuncName()
		if funcName != "" {
			parts := strings.Split(funcName, ".")
			actorName = parts[0]
		}
	}
	if actorName == "" {
		actorName = "UnknownActor"
	}

	// Get timestamp from creation task
	var timestamp *int64
	if !creationTask.CreationTime.IsZero() {
		ts := creationTask.CreationTime.UnixMilli()
		timestamp = &ts
	}

	group := &NestedTaskSummary{
		Name:        actorName,
		Key:         key,
		Type:        "ACTOR",
		Timestamp:   timestamp,
		StateCounts: make(map[string]int),
		Children:    make([]*NestedTaskSummary, 0),
		Link:        &Link{Type: "actor", ID: actorID},
	}
	b.taskGroupByID[key] = group

	// Determine ACTOR entry's parent: same as its creation task's parent to match Ray's tree structure.
	// creationTask is guaranteed to exist (early return above if missing).
	parentTaskID := creationTask.ParentTaskID
	if parentTaskID == "" || isDriverTaskID(parentTaskID) {
		b.summary = append(b.summary, group)
	} else {
		parentTaskGroup := b.getOrCreateTaskGroup(parentTaskID)
		if parentTaskGroup != nil {
			parentTaskGroup.Children = append(parentTaskGroup.Children, group)
		}
	}

	return group
}

// mergeSiblingsForTaskGroup groups children with the same name into GROUP nodes.
// It also propagates the minimum timestamp from descendants upward, matching Ray's behavior:
// if a child's subtree contains an earlier timestamp, the child's own timestamp is updated.
// Returns the merged siblings and the minimum timestamp across all siblings (for upward propagation).
// Ref: https://github.com/ray-project/ray/blob/d0b1d151d8ea964a711e451d0ae736f8bf95b629/python/ray/util/state/common.py#L1261-L1311
func (b *lineageBuilder) mergeSiblingsForTaskGroup(siblings []*NestedTaskSummary) ([]*NestedTaskSummary, *int64) {
	if len(siblings) == 0 {
		return siblings, nil
	}

	// Group by name, preserving insertion order.
	// For each child, first recursively merge its children and propagate min timestamp upward.
	groups := make(map[string]*NestedTaskSummary)
	order := make([]string, 0)
	var minTimestamp *int64

	for _, child := range siblings {
		// Recursively merge children and propagate min timestamp upward.
		var childMinTimestamp *int64
		child.Children, childMinTimestamp = b.mergeSiblingsForTaskGroup(child.Children)
		if childMinTimestamp != nil && *childMinTimestamp < getTimestamp(child) {
			ts := *childMinTimestamp
			child.Timestamp = &ts
		}

		// Create temporary GROUP node for this name if not exists.
		if _, exists := groups[child.Name]; !exists {
			order = append(order, child.Name)
			groups[child.Name] = &NestedTaskSummary{
				Name: child.Name,
				Key:  child.Name,
				Type: "GROUP",
			}
		}
		g := groups[child.Name]
		g.Children = append(g.Children, child)

		// Track min timestamp for this group and overall.
		if child.Timestamp != nil {
			if g.Timestamp == nil || *child.Timestamp < *g.Timestamp {
				ts := *child.Timestamp
				g.Timestamp = &ts
				if minTimestamp == nil || ts < *minTimestamp {
					minTS := ts
					minTimestamp = &minTS
				}
			}
		}
	}

	// Build result: groups with >1 children stay as GROUP, single-child groups unwrap.
	result := make([]*NestedTaskSummary, 0, len(order))
	for _, name := range order {
		g := groups[name]
		if len(g.Children) == 1 {
			result = append(result, g.Children[0])
		} else {
			g.StateCounts = make(map[string]int)
			// GROUP nodes don't have a link
			result = append(result, g)
		}
	}

	return result, minTimestamp
}

// calcTotalForTaskGroup recursively sums children's state_counts into parent nodes.
// This allows parent/GROUP nodes to display aggregated state information
// (e.g., a GROUP shows "3 RUNNING, 2 FINISHED" from its children).
// Ref: https://github.com/ray-project/ray/blob/d0b1d151d8ea964a711e451d0ae736f8bf95b629/python/ray/util/state/common.py#L1343-L1363
func (b *lineageBuilder) calcTotalForTaskGroup(groups []*NestedTaskSummary) {
	for _, group := range groups {
		if len(group.Children) > 0 {
			// First, calculate totals for children
			b.calcTotalForTaskGroup(group.Children)

			// Then, sum children's state_counts into this group
			for _, child := range group.Children {
				for state, count := range child.StateCounts {
					group.StateCounts[state] += count
				}
			}
		}
	}
}

// sortTaskGroups sorts nodes to surface the most important tasks first.
// Priority: running > pending > failed > earliest timestamp > actor_creation.
// This matches Ray Dashboard's sort order so users see active tasks at the top.
// Ref: https://github.com/ray-project/ray/blob/d0b1d151d8ea964a711e451d0ae736f8bf95b629/python/ray/util/state/common.py#L1331-L1341
func (b *lineageBuilder) sortTaskGroups(groups []*NestedTaskSummary) {
	// First, recursively sort children
	for _, group := range groups {
		if len(group.Children) > 0 {
			b.sortTaskGroups(group.Children)
		}
	}

	// Sort this level
	sort.SliceStable(groups, func(i, j int) bool {
		// 1. Running tasks first (descending)
		runI := getRunningTasksCount(groups[i])
		runJ := getRunningTasksCount(groups[j])
		if runI != runJ {
			return runI > runJ
		}

		// 2. Pending tasks (descending)
		pendI := getPendingTasksCount(groups[i])
		pendJ := getPendingTasksCount(groups[j])
		if pendI != pendJ {
			return pendI > pendJ
		}

		// 3. Failed tasks (descending)
		failI := groups[i].StateCounts["FAILED"]
		failJ := groups[j].StateCounts["FAILED"]
		if failI != failJ {
			return failI > failJ
		}

		// 4. Timestamp (ascending - earlier first)
		tsI := getTimestamp(groups[i])
		tsJ := getTimestamp(groups[j])
		if tsI != tsJ {
			return tsI < tsJ
		}

		// 5. ACTOR_CREATION_TASK before others
		return groups[i].Type == "ACTOR_CREATION_TASK" && groups[j].Type != "ACTOR_CREATION_TASK"
	})
}

// getRunningTasksCount returns the total count of all running sub-states.
// Ray has multiple running states (RUNNING, RUNNING_IN_RAY_GET, RUNNING_IN_RAY_WAIT).
func getRunningTasksCount(g *NestedTaskSummary) int {
	return g.StateCounts["RUNNING"] +
		g.StateCounts["RUNNING_IN_RAY_GET"] +
		g.StateCounts["RUNNING_IN_RAY_WAIT"]
}

// getPendingTasksCount returns the total count of all pending sub-states.
// Ray has multiple pending states depending on what the task is waiting for.
func getPendingTasksCount(g *NestedTaskSummary) int {
	return g.StateCounts["PENDING_ARGS_AVAIL"] +
		g.StateCounts["PENDING_NODE_ASSIGNMENT"] +
		g.StateCounts["PENDING_OBJ_STORE_MEM_AVAIL"] +
		g.StateCounts["PENDING_ARGS_FETCH"]
}

// getTimestamp returns the timestamp, or MaxInt64 if nil (to sort unstarted tasks last).
func getTimestamp(g *NestedTaskSummary) int64 {
	if g.Timestamp == nil {
		return math.MaxInt64
	}
	return *g.Timestamp
}

// TaskSummaryPerFuncOrClassName represents a task summary entry for func_name mode.
// Ref: https://github.com/ray-project/ray/blob/777f37f002c14bd4c587f4d095b85c62690647de/python/ray/util/state/common.py#L983-L990
type TaskSummaryPerFuncOrClassName struct {
	FuncOrClassName string         `json:"func_or_class_name"`
	Type            string         `json:"type"`
	StateCounts     map[string]int `json:"state_counts"`
}

// TaskSummariesByFuncName is the response for summary_by=func_name
// Uses map instead of slice for summary (different from lineage mode)
type TaskSummariesByFuncName struct {
	Summary             map[string]*TaskSummaryPerFuncOrClassName `json:"summary"`
	TotalTasks          int                                       `json:"total_tasks"`
	TotalActorTasks     int                                       `json:"total_actor_tasks"`
	TotalActorScheduled int                                       `json:"total_actor_scheduled"`
	SummaryBy           string                                    `json:"summary_by"`
}
