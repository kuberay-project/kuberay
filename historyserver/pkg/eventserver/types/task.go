package types

import (
	"fmt"
	"maps"
	"sort"
	"sync"
	"time"
)

// For common proto definitions, please refer to:
// https://github.com/ray-project/ray/blob/master/src/ray/protobuf/common.proto.

// Type of a task.
type TaskType string

const (
	NORMAL_TASK         TaskType = "NORMAL_TASK"
	ACTOR_CREATION_TASK TaskType = "ACTOR_CREATION_TASK"
	ACTOR_TASK          TaskType = "ACTOR_TASK"
	DRIVER_TASK         TaskType = "DRIVER_TASK"
)

// Language represents the language of a task or worker.
type Language string

const (
	PYTHON Language = "PYTHON"
	JAVA   Language = "JAVA"
	CPP    Language = "CPP"
)

// JavaFunctionDescriptor is a FunctionDescriptor for Java.
type JavaFunctionDescriptor struct {
	ClassName    string `json:"className"`
	FunctionName string `json:"functionName"`
	Signature    string `json:"signature"`
}

// CppFunctionDescriptor is a FunctionDescriptor for C/C++.
type CppFunctionDescriptor struct {
	FunctionName string `json:"functionName"`
	Caller       string `json:"caller"`
	ClassName    string `json:"className"`
}

// PythonFunctionDescriptor is a FunctionDescriptor for Python.
type PythonFunctionDescriptor struct {
	ModuleName   string `json:"moduleName"`
	ClassName    string `json:"className"`
	FunctionName string `json:"functionName"`
	FunctionHash string `json:"functionHash"`
}

// FunctionDescriptor is a union wrapper for various function descriptor types.
// Ray's proto guarantees that the following three function descriptors hold an "oneof" relationship.
// Ref: https://github.com/ray-project/ray/blob/36be009ae360788550e541d81806493f52963730/src/ray/protobuf/common.proto#L157-L164.
type FunctionDescriptor struct {
	JavaFunctionDescriptor   *JavaFunctionDescriptor   `json:"javaFunctionDescriptor,omitempty"`
	PythonFunctionDescriptor *PythonFunctionDescriptor `json:"pythonFunctionDescriptor,omitempty"`
	CppFunctionDescriptor    *CppFunctionDescriptor    `json:"cppFunctionDescriptor,omitempty"`
}

// TaskStatus represents the current state of a task.
type TaskStatus string

// The following statuses follow a rough chronological order of transition.
// For typical order of states, please refer to:
// https://github.com/ray-project/ray/blob/d0b1d151d8ea964a711e451d0ae736f8bf95b629/src/ray/protobuf/common.proto#L884-L899.
// TODO(jiangjiawei1103): Each entity (actor, task, job, node) should have its own const def with entity name prepended to avoid conflicts.
const (
	NIL                                        TaskStatus = "NIL"
	PENDING_ARGS_AVAIL                         TaskStatus = "PENDING_ARGS_AVAIL"
	PENDING_NODE_ASSIGNMENT                    TaskStatus = "PENDING_NODE_ASSIGNMENT"
	PENDING_OBJ_STORE_MEM_AVAIL                TaskStatus = "PENDING_OBJ_STORE_MEM_AVAIL"
	PENDING_ARGS_FETCH                         TaskStatus = "PENDING_ARGS_FETCH"
	SUBMITTED_TO_WORKER                        TaskStatus = "SUBMITTED_TO_WORKER"
	PENDING_ACTOR_TASK_ARGS_FETCH              TaskStatus = "PENDING_ACTOR_TASK_ARGS_FETCH"
	PENDING_ACTOR_TASK_ORDERING_OR_CONCURRENCY TaskStatus = "PENDING_ACTOR_TASK_ORDERING_OR_CONCURRENCY"
	RUNNING                                    TaskStatus = "RUNNING"
	RUNNING_IN_RAY_GET                         TaskStatus = "RUNNING_IN_RAY_GET"
	RUNNING_IN_RAY_WAIT                        TaskStatus = "RUNNING_IN_RAY_WAIT"
	FINISHED                                   TaskStatus = "FINISHED"
	FAILED                                     TaskStatus = "FAILED"
	GETTING_AND_PINNING_ARGS                   TaskStatus = "GETTING_AND_PINNING_ARGS"
)

// TaskStateTransition represents a change in a task's state at a specific timestamp.
type TaskStateTransition struct {
	State     TaskStatus `json:"state"`
	Timestamp time.Time  `json:"timestamp"`
}

type TaskLogInfo struct {
	StdoutFile  string `json:"stdoutFile"`
	StderrFile  string `json:"stderrFile"`
	StdoutStart int64  `json:"stdoutStart,string"`
	StdoutEnd   int64  `json:"stdoutEnd,string"`
	StderrStart int64  `json:"stderrStart,string"`
	StderrEnd   int64  `json:"stderrEnd,string"`
}

// Task's fields are populated from the TASK_DEFINITION_EVENT, ACTOR_TASK_DEFINITION_EVENT, and TASK_LIFECYCLE_EVENT.
// A TASK_DEFINITION_EVENT or an ACTOR_TASK_DEFINITION_EVENT is expected to be emitted once per task attempt,
// For proto definitions, please refer to:
// https://github.com/ray-project/ray/tree/master/src/ray/protobuf/public.
// For field population, please refer to:
// https://github.com/ray-project/ray/blob/36be009ae360788550e541d81806493f52963730/src/ray/core_worker/task_event_buffer.cc#L189-L295.
type Task struct {
	// TaskID and TaskAttempt form the unique identifier for a task.
	TaskID      string `json:"taskId"`
	TaskAttempt int    `json:"taskAttempt"`

	// The task definition information.
	// TaskType is the type of a task, only available for TASK_DEFINITION_EVENT.
	// For ACTOR_TASK_DEFINITION_EVENT, the TaskType is populated as ACTOR_TASK manually.
	TaskType TaskType `json:"taskType,omitempty"`
	Language Language `json:"language,omitempty"`
	// For TASK_DEFINITION_EVENT, only TaskFunc and TaskName are populated.
	// For ACTOR_TASK_DEFINITION_EVENT, only ActorFunc and ActorTaskName are populated.
	// It might be better to define separate structs or TaskDefinition interface with custom JSON marshal/unmarshal logic.
	TaskFunc          *FunctionDescriptor `json:"taskFunc,omitempty"`
	ActorFunc         *FunctionDescriptor `json:"actorFunc,omitempty"`
	TaskName          string              `json:"taskName,omitempty"`
	ActorTaskName     string              `json:"actorTaskName,omitempty"`
	RequiredResources map[string]float64  `json:"requiredResources,omitempty"`

	// The correlation ids of the task that can be used to correlate the task with other events.
	// ActorID is only available for ACTOR_TASK_DEFINITION_EVENT.
	ActorID              string            `json:"actorId,omitempty"`
	JobID                string            `json:"jobId"` // Present in both DEFINITION and LIFECYCLE events.
	ParentTaskID         string            `json:"parentTaskId,omitempty"`
	PlacementGroupID     string            `json:"placementGroupId,omitempty"`
	RefIDs               map[string]string `json:"refIds,omitempty"`
	SerializedRuntimeEnv string            `json:"serializedRuntimeEnv,omitempty"`
	// CallSite is the human readable stacktrace of the actor task invocation.
	CallSite *string `json:"callSite,omitempty"`
	// LabelSelector is the key-value label constraints of the node to schedule this actor task on.
	LabelSelector map[string]string `json:"labelSelector,omitempty"`

	// The task execution information, populated from TASK_LIFECYCLE_EVENT.
	StateTransitions []TaskStateTransition `json:"stateTransitions,omitempty"`
	// RayErrorInfo is only populated when the state error info has any values.
	// Ref: https://github.com/ray-project/ray/blob/36be009ae360788550e541d81806493f52963730/src/ray/core_worker/task_event_buffer.cc#L263-L265.
	RayErrorInfo *RayErrorInfo `json:"rayErrorInfo,omitempty"`

	ProfileData     *ProfileData `json:"profile_data,omitempty"`
	FuncOrClassName string       `json:"functionName,omitempty"`
	NodeID          string       `json:"nodeId,omitempty"`
	WorkerID        string       `json:"workerId,omitempty"`
	WorkerPID       int          `json:"workerPid,omitempty"`
	// Whether the task is paused by the debugger.
	IsDebuggerPaused *bool `json:"isDebuggerPaused,omitempty"`
	// Actor task repr name, if applicable.
	ActorReprName *string `json:"actorReprName,omitempty"`

	// TaskLogInfo was added at https://github.com/ray-project/ray/pull/60287.
	TaskLogInfo *TaskLogInfo `json:"taskLogInfo,omitempty"`

	State        TaskStatus
	CreationTime time.Time
	StartTime    time.Time
	EndTime      time.Time
}

// TaskProfileEvents represents the structure of TASK_PROFILE_EVENT from Ray Event Export API
// Note: Ray API uses "attemptNumber" for TASK_PROFILE_EVENT, but "taskAttempt"
// for TASK_LIFECYCLE_EVENT and TASK_DEFINITION_EVENT
type TaskProfileEvents struct {
	TaskID        string        `json:"taskId"`
	AttemptNumber int           `json:"attemptNumber"`
	JobID         string        `json:"jobId"`
	ProfileEvents ProfileEvents `json:"profileEvents"`
}

// ProfileEvents contains component and event information
type ProfileEvents struct {
	ComponentID   string              `json:"componentId"`
	ComponentType string              `json:"componentType"`
	NodeIPAddress string              `json:"nodeIpAddress"`
	Events        []ProfileEventEntry `json:"events"`
}

// ProfileEventEntry represents a single profile event within Spans
type ProfileEventEntry struct {
	EventName string `json:"eventName"`
	StartTime string `json:"startTime"`
	EndTime   string `json:"endTime"`
	ExtraData string `json:"extraData"`
}

type ProfileData struct {
	ComponentID   string            `json:"component_id"`
	ComponentType string            `json:"component_type"`
	NodeIPAddress string            `json:"node_ip_address"`
	Events        []ProfileEventRaw `json:"events"`
}

// StartTime and EndTime are in nanoseconds,
// matching Ray's implementation in profile_event.cc which uses absl::GetCurrentTimeNanos()).
type ProfileEventRaw struct {
	EventName string `json:"event_name"`
	StartTime int64  `json:"start_time"` // nanoseconds
	EndTime   int64  `json:"end_time"`   // nanoseconds
	ExtraData string `json:"extra_data"`
}

// TaskMap is a struct that uses TaskID as key and stores a list of Task attempts.
// Each TaskID maps to a slice of Tasks, where each element represents a different attempt.
type TaskMap struct {
	TaskMap map[string][]Task
	Mu      sync.Mutex
}

func (t *TaskMap) Lock() {
	t.Mu.Lock()
}

func (t *TaskMap) Unlock() {
	t.Mu.Unlock()
}

func NewTaskMap() *TaskMap {
	return &TaskMap{
		TaskMap: make(map[string][]Task),
	}
}

type ClusterTaskMap struct {
	// ClusterTaskMap is a map of cluster session ID to TaskMap.
	ClusterTaskMap map[string]*TaskMap
	Mu             sync.RWMutex
}

func (c *ClusterTaskMap) RLock() {
	c.Mu.RLock()
}

func (c *ClusterTaskMap) RUnlock() {
	c.Mu.RUnlock()
}

func (c *ClusterTaskMap) Lock() {
	c.Mu.Lock()
}

func (c *ClusterTaskMap) Unlock() {
	c.Mu.Unlock()
}

// GetOrCreateTaskMap retrieves the TaskMap for the given cluster session, creating it if it doesn't exist.
func (c *ClusterTaskMap) GetOrCreateTaskMap(clusterSessionKey string) *TaskMap {
	c.Lock()
	defer c.Unlock()

	taskMap, exists := c.ClusterTaskMap[clusterSessionKey]
	if !exists {
		taskMap = NewTaskMap()
		c.ClusterTaskMap[clusterSessionKey] = taskMap
	}
	return taskMap
}

// CreateOrMergeTaskAttempt creates a new slice of Task attempts or insert the current attempt at the correct position for the given taskId.
// Uses binary search for O(log n) lookup. Maintains sorted order by AttemptNumber.
// This handles the case where LIFECYCLE events arrive before DEFINITION events.
//   - If the attempt doesn't exist, creates a new one at the correct position
//   - If the attempt exists, applies mergeFn to merge new data into existing
func (t *TaskMap) CreateOrMergeAttempt(taskId string, taskAttempt int, mergeFn func(*Task)) {
	t.Lock()
	defer t.Unlock()

	// Case 1: tasks doesn't exist.
	// Create a new slice of Task attempts with the current attempt.
	tasks, exists := t.TaskMap[taskId]
	if !exists {
		newTask := Task{TaskID: taskId, TaskAttempt: taskAttempt}
		mergeFn(&newTask)
		t.TaskMap[taskId] = []Task{newTask}
		return
	}

	// Case 2: tasks exists and the current attempt already exists.
	// Run binary search to find the first index where TaskAttempt >= taskAttempt.
	idx := sort.Search(len(tasks), func(i int) bool {
		return tasks[i].TaskAttempt >= taskAttempt
	})

	// Apply the merge function to the existing attempt.
	if idx < len(tasks) && tasks[idx].TaskAttempt == taskAttempt {
		mergeFn(&tasks[idx])
		return
	}

	// Case 3: tasks exists and the current attempt doesn't exist.
	// Create a new ActorTask attempt and apply the merge function.
	newTask := Task{TaskID: taskId, TaskAttempt: taskAttempt}
	mergeFn(&newTask)

	// Insert the current attempt at the correct position.
	tasks = append(tasks, Task{})    // Extend slice by 1
	copy(tasks[idx+1:], tasks[idx:]) // Shift elements right
	tasks[idx] = newTask             // Insert at correct position
	t.TaskMap[taskId] = tasks
}

// GetTaskName returns the task name of the task.
func (t *Task) GetTaskName() string {
	if t.TaskType == ACTOR_TASK {
		return t.ActorTaskName
	}
	return t.TaskName
}

// GetFuncName returns the function name of the task.
func (t *Task) GetFuncName() string {
	if t.TaskType == ACTOR_TASK {
		return t.ActorFunc.CallString()
	}
	return t.TaskFunc.CallString()
}

// GetLastState returns the last state of the task.
func (t *Task) GetLastState() TaskStatus {
	if len(t.StateTransitions) == 0 {
		return NIL
	}
	return t.StateTransitions[len(t.StateTransitions)-1].State
}

// GetFilterableFieldValue returns the filterable field value. For now, we only consider those strongly related to task filtering.
// Ref: https://github.com/ray-project/ray/blob/d0b1d151d8ea964a711e451d0ae736f8bf95b629/src/ray/protobuf/gcs_service.proto#L795-L859.
// For all filterable fields, please refer to:
// Ref: https://github.com/ray-project/ray/blob/d0b1d151d8ea964a711e451d0ae736f8bf95b629/python/ray/util/state/common.py#L730-L819.
// TODO(jiangjiawei1103): Define object-specific filterable fields (e.g., Actor, Node).
func (t *Task) GetFilterableFieldValue(filterKey string) string {
	switch filterKey {
	case "task_type":
		return string(t.TaskType)
	case "job_id":
		return t.JobID
	case "task_id":
		return t.TaskID
	case "actor_id":
		return t.ActorID
	case "task_name":
		if t.TaskType == ACTOR_TASK {
			return t.ActorTaskName
		}
		return t.TaskName
	case "state":
		return string(t.State)
	default:
		return ""
	}
}

// DeepCopy returns a deep copy of the Task, including fields of type slice, map, and pointer.
// This prevents race conditions when the returned Task is used after locks are released.
func (t Task) DeepCopy() Task {
	cp := t

	cp.TaskFunc = t.TaskFunc.DeepCopy()
	cp.ActorFunc = t.ActorFunc.DeepCopy()

	if len(t.RequiredResources) > 0 {
		cp.RequiredResources = make(map[string]float64, len(t.RequiredResources))
		maps.Copy(cp.RequiredResources, t.RequiredResources)
	}

	if len(t.RefIDs) > 0 {
		cp.RefIDs = make(map[string]string, len(t.RefIDs))
		maps.Copy(cp.RefIDs, t.RefIDs)
	}

	if t.CallSite != nil {
		callSite := *t.CallSite
		cp.CallSite = &callSite
	}

	if len(t.LabelSelector) > 0 {
		cp.LabelSelector = make(map[string]string, len(t.LabelSelector))
		maps.Copy(cp.LabelSelector, t.LabelSelector)
	}

	if len(t.StateTransitions) > 0 {
		cp.StateTransitions = make([]TaskStateTransition, len(t.StateTransitions))
		copy(cp.StateTransitions, t.StateTransitions)
	}

	if t.RayErrorInfo != nil {
		rayErrorInfo := *t.RayErrorInfo
		cp.RayErrorInfo = &rayErrorInfo
	}

	if t.IsDebuggerPaused != nil {
		isDebuggerPaused := *t.IsDebuggerPaused
		cp.IsDebuggerPaused = &isDebuggerPaused
	}

	if t.ActorReprName != nil {
		actorReprName := *t.ActorReprName
		cp.ActorReprName = &actorReprName
	}

	if t.TaskLogInfo != nil {
		taskLogInfo := *t.TaskLogInfo
		cp.TaskLogInfo = &taskLogInfo
	}

	if t.ProfileData != nil {
		cp.ProfileData = &ProfileData{
			ComponentID:   t.ProfileData.ComponentID,
			ComponentType: t.ProfileData.ComponentType,
			NodeIPAddress: t.ProfileData.NodeIPAddress,
		}
		if len(t.ProfileData.Events) > 0 {
			cp.ProfileData.Events = make([]ProfileEventRaw, len(t.ProfileData.Events))
			copy(cp.ProfileData.Events, t.ProfileData.Events)
		}
	}
	return cp
}

// FunctionDescriptor.DeepCopy returns a deep copy of the FunctionDescriptor.
func (f *FunctionDescriptor) DeepCopy() *FunctionDescriptor {
	if f == nil {
		return nil
	}

	cp := &FunctionDescriptor{}
	if f.PythonFunctionDescriptor != nil {
		pythonFunctionDescriptor := *f.PythonFunctionDescriptor
		cp.PythonFunctionDescriptor = &pythonFunctionDescriptor
	}
	if f.JavaFunctionDescriptor != nil {
		javaFunctionDescriptor := *f.JavaFunctionDescriptor
		cp.JavaFunctionDescriptor = &javaFunctionDescriptor
	}
	if f.CppFunctionDescriptor != nil {
		cppFunctionDescriptor := *f.CppFunctionDescriptor
		cp.CppFunctionDescriptor = &cppFunctionDescriptor
	}

	return cp
}

// FunctionDescriptor.CallString returns the function name of the FunctionDescriptor.
// Ref: https://github.com/ray-project/ray/blob/d0b1d151d8ea964a711e451d0ae736f8bf95b629/src/ray/common/function_descriptor.h#L203-L212.
func (f *FunctionDescriptor) CallString() string {
	if f == nil {
		return ""
	}

	var className string
	var functionName string
	if f.JavaFunctionDescriptor != nil {
		className = f.JavaFunctionDescriptor.ClassName
		functionName = f.JavaFunctionDescriptor.FunctionName
	}
	if f.PythonFunctionDescriptor != nil {
		className = f.PythonFunctionDescriptor.ClassName
		functionName = f.PythonFunctionDescriptor.FunctionName
	}
	if f.CppFunctionDescriptor != nil {
		className = f.CppFunctionDescriptor.ClassName
		functionName = f.CppFunctionDescriptor.FunctionName
	}

	if className == "" {
		return functionName
	}
	return fmt.Sprintf("%s.%s", className, functionName)
}
