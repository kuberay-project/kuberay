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

func TestNewTaskMap(t *testing.T) {
	tm := NewTaskMap()
	require.NotNil(t, tm)
	require.NotNil(t, tm.TaskMap)
	assert.Empty(t, tm.TaskMap)
}

func TestGetOrCreateTaskMap(t *testing.T) {
	ctm := &ClusterTaskMap{ClusterTaskMap: make(map[string]*TaskMap)}

	tm := ctm.GetOrCreateTaskMap("session-0")
	assert.Same(t, tm, ctm.GetOrCreateTaskMap("session-0"))
	assert.NotSame(t, tm, ctm.GetOrCreateTaskMap("session-1"))

	ctm.RLock()
	assert.Same(t, tm, ctm.ClusterTaskMap["session-0"])
	ctm.RUnlock()
	assert.Len(t, ctm.ClusterTaskMap, 2)
}

func TestCreateOrMergeAttempt(t *testing.T) {
	const taskID = "task-0"

	t.Run("creates a new task when taskID does not exist", func(t *testing.T) {
		tm := NewTaskMap()
		tm.CreateOrMergeAttempt(taskID, 0, func(task *Task) {
			task.TaskName = "attempt-0"
		})

		require.Len(t, tm.TaskMap[taskID], 1)
		assert.Equal(t, Task{TaskID: taskID, TaskAttempt: 0, TaskName: "attempt-0"}, tm.TaskMap[taskID][0])
	})

	t.Run("merges into an existing attempt", func(t *testing.T) {
		tm := NewTaskMap()
		tm.CreateOrMergeAttempt(taskID, 0, func(task *Task) {
			task.TaskName = "attempt-0"
		})
		tm.CreateOrMergeAttempt(taskID, 0, func(task *Task) {
			task.JobID = "job-0"
		})

		require.Len(t, tm.TaskMap[taskID], 1)
		assert.Equal(t, "attempt-0", tm.TaskMap[taskID][0].TaskName)
		assert.Equal(t, "job-0", tm.TaskMap[taskID][0].JobID)
	})

	t.Run("inserts a new attempt in sorted order", func(t *testing.T) {
		tm := NewTaskMap()
		tm.CreateOrMergeAttempt(taskID, 2, func(task *Task) {
			task.TaskName = "attempt-2"
			task.JobID = "job-2"
		})
		tm.CreateOrMergeAttempt(taskID, 0, func(task *Task) {
			task.TaskName = "attempt-0"
			task.JobID = "job-0"
		})
		tm.CreateOrMergeAttempt(taskID, 1, func(task *Task) {
			task.TaskName = "attempt-1"
			task.JobID = "job-1"
		})

		got := tm.TaskMap[taskID]
		require.Len(t, got, 3)
		assert.Equal(t, []int{0, 1, 2}, []int{got[0].TaskAttempt, got[1].TaskAttempt, got[2].TaskAttempt})
		assert.Equal(t, []string{"attempt-0", "attempt-1", "attempt-2"}, []string{got[0].TaskName, got[1].TaskName, got[2].TaskName})
		assert.Equal(t, []string{"job-0", "job-1", "job-2"}, []string{got[0].JobID, got[1].JobID, got[2].JobID})
	})
}

func TestGetTaskName(t *testing.T) {
	tests := []struct {
		name string
		task Task
		want string
	}{
		{
			name: "actor task uses ActorTaskName",
			task: Task{TaskType: ACTOR_TASK, TaskName: "task-0", ActorTaskName: "actor-task-0"},
			want: "actor-task-0",
		},
		{
			name: "non-actor task uses TaskName",
			task: Task{TaskType: NORMAL_TASK, TaskName: "task-0", ActorTaskName: "actor-task-0"},
			want: "task-0",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, tc.task.GetTaskName())
		})
	}
}

func TestGetFuncName(t *testing.T) {
	pythonFunc := &FunctionDescriptor{
		PythonFunctionDescriptor: &PythonFunctionDescriptor{ClassName: "Python", FunctionName: "fn"},
	}
	actorFunc := &FunctionDescriptor{
		PythonFunctionDescriptor: &PythonFunctionDescriptor{ClassName: "Actor", FunctionName: "fn"},
	}

	tests := []struct {
		name string
		task Task
		want string
	}{
		{
			name: "actor task uses ActorFunc",
			task: Task{TaskType: ACTOR_TASK, TaskFunc: pythonFunc, ActorFunc: actorFunc},
			want: "Actor.fn",
		},
		{
			name: "non-actor task uses TaskFunc",
			task: Task{TaskType: NORMAL_TASK, TaskFunc: pythonFunc, ActorFunc: actorFunc},
			want: "Python.fn",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, tc.task.GetFuncName())
		})
	}
}

func TestGetLastState(t *testing.T) {
	tests := []struct {
		name string
		task Task
		want TaskStatus
	}{
		{name: "empty transitions", task: Task{}, want: NIL},
		{
			name: "returns the last transition",
			task: Task{StateTransitions: []TaskStateTransition{
				{State: PENDING_ARGS_AVAIL},
				{State: RUNNING},
				{State: FINISHED},
			}},
			want: FINISHED,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, tc.task.GetLastState())
		})
	}
}

func TestGetFilterableFieldValue(t *testing.T) {
	task := Task{
		TaskID:        "task-0",
		TaskType:      NORMAL_TASK,
		TaskName:      "task-0",
		ActorTaskName: "actor-task-0",
		ActorID:       "actor-0",
		JobID:         "job-0",
		State:         RUNNING,
	}
	actorTask := task
	actorTask.TaskType = ACTOR_TASK

	tests := []struct {
		name string
		task Task
		key  string
		want string
	}{
		{"task_type", task, "task_type", string(NORMAL_TASK)},
		{"job_id", task, "job_id", "job-0"},
		{"task_id", task, "task_id", "task-0"},
		{"actor_id", task, "actor_id", "actor-0"},
		{"task_name for normal task", task, "task_name", "task-0"},
		{"task_name for actor task", actorTask, "task_name", "actor-task-0"},
		{"state", task, "state", string(RUNNING)},
		{"unknown key", task, "node_id", ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, tc.task.GetFilterableFieldValue(tc.key))
		})
	}
}

func TestDeepCopy(t *testing.T) {
	t.Run("nil nested fields stay nil", func(t *testing.T) {
		cp := Task{TaskID: "task-0"}.DeepCopy()
		assert.Equal(t, "task-0", cp.TaskID)
		assert.Nil(t, cp.TaskFunc)
		assert.Nil(t, cp.ActorFunc)
		assert.Nil(t, cp.RequiredResources)
		assert.Nil(t, cp.RefIDs)
		assert.Nil(t, cp.CallSite)
		assert.Nil(t, cp.LabelSelector)
		assert.Nil(t, cp.StateTransitions)
		assert.Nil(t, cp.RayErrorInfo)
		assert.Nil(t, cp.ProfileData)
		assert.Nil(t, cp.IsDebuggerPaused)
		assert.Nil(t, cp.ActorReprName)
		assert.Nil(t, cp.TaskLogInfo)
	})

	t.Run("copy is independent of the original", func(t *testing.T) {
		callSite := "foo.py:1"
		paused := true
		repr := "Actor"
		orig := Task{
			TaskID:      "task-0",
			TaskAttempt: 0,
			TaskType:    NORMAL_TASK,
			TaskFunc: &FunctionDescriptor{
				PythonFunctionDescriptor: &PythonFunctionDescriptor{ClassName: "Python", FunctionName: "fn"},
			},
			ActorFunc: &FunctionDescriptor{
				PythonFunctionDescriptor: &PythonFunctionDescriptor{ClassName: "Actor", FunctionName: "fn"},
			},
			RequiredResources: map[string]float64{"CPU": 1},
			RefIDs:            map[string]string{"obj": "ref"},
			CallSite:          &callSite,
			LabelSelector:     map[string]string{"k": "v"},
			StateTransitions:  []TaskStateTransition{{State: RUNNING}},
			RayErrorInfo:      &RayErrorInfo{ErrorMessage: "boom", ErrorType: WorkerDied},
			ProfileData: &ProfileData{
				ComponentID:   "cid",
				ComponentType: "worker",
				NodeIPAddress: "10.0.0.1",
				Events:        []ProfileEventRaw{{EventName: "span", StartTime: 1, EndTime: 2}},
			},
			IsDebuggerPaused: &paused,
			ActorReprName:    &repr,
			TaskLogInfo:      &TaskLogInfo{StdoutFile: "worker.out"},
		}

		cp := orig.DeepCopy()
		assert.Equal(t, orig, cp)

		cp.RequiredResources["CPU"] = 99
		cp.RefIDs["obj"] = "changed"
		cp.LabelSelector["k"] = "changed"
		cp.StateTransitions[0].State = FAILED
		*cp.CallSite = "mutated"
		cp.RayErrorInfo.ErrorMessage = "mutated"
		*cp.IsDebuggerPaused = false
		*cp.ActorReprName = "mutated"
		cp.TaskLogInfo.StdoutFile = "mutated"
		cp.TaskFunc.PythonFunctionDescriptor.FunctionName = "mutated"
		cp.ActorFunc.PythonFunctionDescriptor.FunctionName = "mutated"
		cp.ProfileData.ComponentID = "mutated"
		cp.ProfileData.Events[0].EventName = "mutated"

		assert.Equal(t, 1.0, orig.RequiredResources["CPU"])
		assert.Equal(t, "ref", orig.RefIDs["obj"])
		assert.Equal(t, "v", orig.LabelSelector["k"])
		assert.Equal(t, RUNNING, orig.StateTransitions[0].State)
		assert.Equal(t, "foo.py:1", *orig.CallSite)
		assert.Equal(t, "boom", orig.RayErrorInfo.ErrorMessage)
		assert.True(t, *orig.IsDebuggerPaused)
		assert.Equal(t, "Actor", *orig.ActorReprName)
		assert.Equal(t, "worker.out", orig.TaskLogInfo.StdoutFile)
		assert.Equal(t, "fn", orig.TaskFunc.PythonFunctionDescriptor.FunctionName)
		assert.Equal(t, "fn", orig.ActorFunc.PythonFunctionDescriptor.FunctionName)
		assert.Equal(t, "cid", orig.ProfileData.ComponentID)
		assert.Equal(t, "span", orig.ProfileData.Events[0].EventName)
	})
}

func TestFunctionDescriptorDeepCopy(t *testing.T) {
	t.Run("nil receiver", func(t *testing.T) {
		var fd *FunctionDescriptor
		assert.Nil(t, fd.DeepCopy())
	})

	t.Run("copy is independent of the original", func(t *testing.T) {
		orig := &FunctionDescriptor{
			JavaFunctionDescriptor:   &JavaFunctionDescriptor{ClassName: "Java", FunctionName: "fn"},
			PythonFunctionDescriptor: &PythonFunctionDescriptor{ClassName: "Python", FunctionName: "fn"},
			CppFunctionDescriptor:    &CppFunctionDescriptor{ClassName: "Cpp", FunctionName: "fn"},
		}
		cp := orig.DeepCopy()
		require.NotNil(t, cp)
		assert.Equal(t, orig, cp)
		assert.NotSame(t, orig, cp)

		cp.JavaFunctionDescriptor.FunctionName = "mutated"
		cp.PythonFunctionDescriptor.FunctionName = "mutated"
		cp.CppFunctionDescriptor.FunctionName = "mutated"

		assert.Equal(t, "fn", orig.JavaFunctionDescriptor.FunctionName)
		assert.Equal(t, "fn", orig.PythonFunctionDescriptor.FunctionName)
		assert.Equal(t, "fn", orig.CppFunctionDescriptor.FunctionName)
	})
}

func TestCallString(t *testing.T) {
	tests := []struct {
		name string
		fd   *FunctionDescriptor
		want string
	}{
		{name: "nil", fd: nil, want: ""},
		{
			name: "python with class",
			fd: &FunctionDescriptor{PythonFunctionDescriptor: &PythonFunctionDescriptor{
				ClassName:    "Python",
				FunctionName: "fn",
			}},
			want: "Python.fn",
		},
		{
			name: "python without class",
			fd: &FunctionDescriptor{PythonFunctionDescriptor: &PythonFunctionDescriptor{
				FunctionName: "fn",
			}},
			want: "fn",
		},
		{
			name: "java with class",
			fd: &FunctionDescriptor{JavaFunctionDescriptor: &JavaFunctionDescriptor{
				ClassName:    "Java",
				FunctionName: "fn",
			}},
			want: "Java.fn",
		},
		{
			name: "cpp with class",
			fd: &FunctionDescriptor{CppFunctionDescriptor: &CppFunctionDescriptor{
				ClassName:    "Cpp",
				FunctionName: "fn",
			}},
			want: "Cpp.fn",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, tc.fd.CallString())
		})
	}
}
