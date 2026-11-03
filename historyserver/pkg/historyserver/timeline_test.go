package historyserver

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ray-project/kuberay/historyserver/pkg/eventserver"
	eventtypes "github.com/ray-project/kuberay/historyserver/pkg/eventserver/types"
)

const (
	driverTaskUniqueID = "aaaaaaaaaaaaaaaa"
	noActorID          = "ffffffffffffffffffffffff"
	jobIDHex           = "01000000"
	driverTaskID       = driverTaskUniqueID + noActorID + jobIDHex

	actorUniqueID = "bbbbbbbbbbbbbbbbbbbbbbbb"
	actorTaskID   = driverTaskUniqueID + actorUniqueID + jobIDHex
	actorID       = actorUniqueID + jobIDHex
)

func TestExtractActorIDFromTaskID(t *testing.T) {
	tests := map[string]struct {
		taskID string
		want   string
	}{
		"too short": {
			taskID: "abc",
			want:   "",
		},
		"base64 encoded id": {
			taskID: "cGFzc3dvcmQ=",
			want:   "",
		},
		"driver task with all-f actor portion": {
			taskID: driverTaskID,
			want:   "",
		},
		"mixed-case all-f actor portion": {
			taskID: driverTaskUniqueID + "FFFFFFFFFFFFFFFFFFFFFFFF" + jobIDHex,
			want:   "",
		},
		"actor task": {
			taskID: actorTaskID,
			want:   actorID,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, tc.want, extractActorIDFromTaskID(tc.taskID))
		})
	}
}

func TestGetChromeTraceColor(t *testing.T) {
	tests := map[string]struct {
		eventName string
		want      string
	}{
		"task prefix": {
			eventName: "task::slow_task",
			want:      "generic_work",
		},
		"task deserialize arguments": {
			eventName: "task:deserialize_arguments",
			want:      "rail_load",
		},
		"task execute": {
			eventName: "task:execute",
			want:      "rail_animation",
		},
		"task store outputs": {
			eventName: "task:store_outputs",
			want:      "rail_idle",
		},
		"task submit task": {
			eventName: "task:submit_task",
			want:      "rail_response",
		},
		"task": {
			eventName: "task",
			want:      "rail_response",
		},
		"worker idle": {
			eventName: "worker_idle",
			want:      "cq_build_abandoned",
		},
		"ray get": {
			eventName: "ray.get",
			want:      "good",
		},
		"ray put": {
			eventName: "ray.put",
			want:      "terrible",
		},
		"ray wait": {
			eventName: "ray.wait",
			want:      "vsync_highlight_color",
		},
		"submit task": {
			eventName: "submit_task",
			want:      "background_memory_dump",
		},
		"wait for function": {
			eventName: "wait_for_function",
			want:      "detailed_memory_dump",
		},
		"fetch and run function": {
			eventName: "fetch_and_run_function",
			want:      "detailed_memory_dump",
		},
		"register remote function": {
			eventName: "register_remote_function",
			want:      "detailed_memory_dump",
		},
		"unknown event": {
			eventName: "unknown_event",
			want:      "generic_work",
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, tc.want, getChromeTraceColor(tc.eventName))
		})
	}
}

func TestGetTasksTimeline(t *testing.T) {
	t.Run("nil snapshot", func(t *testing.T) {
		events := getTasksTimeline(nil, "")
		assert.Empty(t, events)
	})

	t.Run("empty tasks", func(t *testing.T) {
		events := getTasksTimeline(&eventserver.SessionSnapshot{}, "")
		assert.Empty(t, events)
	})

	t.Run("job id filter excludes non-matching tasks", func(t *testing.T) {
		snap := &eventserver.SessionSnapshot{
			Tasks: []eventtypes.Task{
				makeTimelineTask(timelineTaskOpts{
					jobID:         "job-a",
					componentType: "worker",
					componentID:   "worker-a",
					nodeIP:        "10.0.0.1",
					eventName:     "task:execute",
				}),
				makeTimelineTask(timelineTaskOpts{
					jobID:         "job-b",
					componentType: "worker",
					componentID:   "worker-b",
					nodeIP:        "10.0.0.2",
					eventName:     "task:execute",
				}),
			},
		}

		events := getTasksTimeline(snap, "job-a")
		traceEvents := filterTraceEvents(events)

		require.Len(t, traceEvents, 1)
		assert.Equal(t, "job-a", traceEvents[0].Args["job_id"])
	})

	t.Run("job id filter with no matches", func(t *testing.T) {
		snap := &eventserver.SessionSnapshot{
			Tasks: []eventtypes.Task{
				makeTimelineTask(timelineTaskOpts{
					jobID:         "job-a",
					componentType: "worker",
					componentID:   "worker-a",
					nodeIP:        "10.0.0.1",
				}),
			},
		}

		assert.Empty(t, getTasksTimeline(snap, "missing-job"))
	})

	t.Run("include all tasks when job id is nil", func(t *testing.T) {
		snap := &eventserver.SessionSnapshot{
			Tasks: []eventtypes.Task{
				makeTimelineTask(timelineTaskOpts{
					jobID:         "job-a",
					componentType: "worker",
					componentID:   "worker-a",
					nodeIP:        "10.0.0.1",
					eventName:     "task:execute",
				}),
				makeTimelineTask(timelineTaskOpts{
					jobID:         "job-b",
					componentType: "worker",
					componentID:   "worker-b",
					nodeIP:        "10.0.0.2",
					eventName:     "task:execute",
				}),
			},
		}

		events := getTasksTimeline(snap, "")
		traceEvents := filterTraceEvents(events)

		require.Len(t, traceEvents, 2)
	})

	t.Run("skips tasks without profile data", func(t *testing.T) {
		snap := &eventserver.SessionSnapshot{
			Tasks: []eventtypes.Task{{JobID: "job-a"}},
		}
		assert.Empty(t, getTasksTimeline(snap, ""))
	})

	t.Run("skips tasks with empty profile events", func(t *testing.T) {
		snap := &eventserver.SessionSnapshot{
			Tasks: []eventtypes.Task{{
				JobID: "job-a",
				ProfileData: &eventtypes.ProfileData{
					ComponentType: "worker",
					ComponentID:   "worker-a",
					NodeIPAddress: "10.0.0.1",
					Events:        nil,
				},
			}},
		}
		assert.Empty(t, getTasksTimeline(snap, ""))
	})

	t.Run("skips unsupported component types", func(t *testing.T) {
		snap := &eventserver.SessionSnapshot{
			Tasks: []eventtypes.Task{
				makeTimelineTask(timelineTaskOpts{
					componentType: "raylet",
					componentID:   "raylet-a",
					nodeIP:        "10.0.0.1",
				}),
			},
		}
		assert.Empty(t, getTasksTimeline(snap, ""))
	})

	t.Run("skips tasks without node ip", func(t *testing.T) {
		snap := &eventserver.SessionSnapshot{
			Tasks: []eventtypes.Task{{
				JobID: "job-a",
				ProfileData: &eventtypes.ProfileData{
					ComponentType: "worker",
					ComponentID:   "worker-a",
					Events: []eventtypes.ProfileEventRaw{{
						EventName: "task:execute",
						StartTime: 1_000_000,
						EndTime:   2_000_000,
					}},
				},
			}},
		}
		assert.Empty(t, getTasksTimeline(snap, ""))
	})

	t.Run("includes driver component", func(t *testing.T) {
		snap := &eventserver.SessionSnapshot{
			Tasks: []eventtypes.Task{
				makeTimelineTask(timelineTaskOpts{
					componentType: "driver",
					componentID:   "driver-a",
					nodeIP:        "10.0.0.1",
					eventName:     "task:execute",
				}),
			},
		}

		events := getTasksTimeline(snap, "")
		require.NotEmpty(t, events)
		assert.NotEmpty(t, filterTraceEvents(events))
	})

	t.Run("emits metadata and trace events", func(t *testing.T) {
		snap := &eventserver.SessionSnapshot{
			Tasks: []eventtypes.Task{
				makeTimelineTask(timelineTaskOpts{
					taskID:          driverTaskID,
					jobID:           "01000000",
					taskAttempt:     2,
					funcOrClassName: "my_func",
					componentType:   "worker",
					componentID:     "worker-a",
					nodeIP:          "10.0.0.1",
					eventName:       "task:execute",
					startTime:       1_000_000,
					endTime:         2_500_000,
				}),
			},
		}

		events := getTasksTimeline(snap, "")
		processName := findMetadataEvent(events, "process_name")
		threadName := findMetadataEvent(events, "thread_name")
		traceEvents := filterTraceEvents(events)

		require.NotNil(t, processName)
		assert.Equal(t, "M", processName.Phase)
		assert.Nil(t, processName.TID)
		assert.Equal(t, "Node 10.0.0.1", processName.Args["name"])

		require.NotNil(t, threadName)
		assert.Equal(t, "M", threadName.Phase)
		require.NotNil(t, threadName.TID)
		assert.Equal(t, "worker:worker-a", threadName.Args["name"])

		require.Len(t, traceEvents, 1)
		traceEvent := traceEvents[0]
		assert.Equal(t, "X", traceEvent.Phase)
		assert.Equal(t, "task:execute", traceEvent.Category)
		assert.Equal(t, "task:execute", traceEvent.Name)
		assert.Equal(t, "rail_animation", traceEvent.Color)
		require.NotNil(t, traceEvent.Timestamp)
		require.NotNil(t, traceEvent.Duration)
		assert.InDelta(t, 1000.0, *traceEvent.Timestamp, 0)
		assert.InDelta(t, 1500.0, *traceEvent.Duration, 0)
		assert.Equal(t, driverTaskID, traceEvent.Args["task_id"])
		assert.Equal(t, "01000000", traceEvent.Args["job_id"])
		assert.Equal(t, 2, traceEvent.Args["attempt_number"])
		assert.Equal(t, "my_func", traceEvent.Args["func_or_class_name"])
		assert.Nil(t, traceEvent.Args["actor_id"])
	})

	t.Run("emits multiple trace events for multiple profile events", func(t *testing.T) {
		snap := &eventserver.SessionSnapshot{
			Tasks: []eventtypes.Task{{
				TaskID:          driverTaskID,
				JobID:           "01000000",
				FuncOrClassName: "my_func",
				ProfileData: &eventtypes.ProfileData{
					ComponentType: "worker",
					ComponentID:   "worker-a",
					NodeIPAddress: "10.0.0.1",
					Events: []eventtypes.ProfileEventRaw{
						{
							EventName: "task:deserialize_arguments",
							StartTime: 1_000_000,
							EndTime:   1_500_000,
						},
						{
							EventName: "task:execute",
							StartTime: 1_500_000,
							EndTime:   3_000_000,
						},
						{
							EventName: "task:store_outputs",
							StartTime: 3_000_000,
							EndTime:   3_500_000,
						},
					},
				},
			}},
		}

		traceEvents := filterTraceEvents(getTasksTimeline(snap, ""))
		require.Len(t, traceEvents, 3)

		assert.Equal(t, "task:deserialize_arguments", traceEvents[0].Category)
		assert.Equal(t, "rail_load", traceEvents[0].Color)
		assert.InDelta(t, 500.0, *traceEvents[0].Duration, 0)

		assert.Equal(t, "task:execute", traceEvents[1].Category)
		assert.Equal(t, "rail_animation", traceEvents[1].Color)
		assert.InDelta(t, 1500.0, *traceEvents[1].Duration, 0)

		assert.Equal(t, "task:store_outputs", traceEvents[2].Category)
		assert.Equal(t, "rail_idle", traceEvents[2].Color)
		assert.InDelta(t, 500.0, *traceEvents[2].Duration, 0)

		// All events share the same PID/TID (same worker)
		assert.Equal(t, traceEvents[0].PID, traceEvents[1].PID)
		assert.Equal(t, traceEvents[0].PID, traceEvents[2].PID)
		assert.Equal(t, *traceEvents[0].TID, *traceEvents[1].TID)
		assert.Equal(t, *traceEvents[0].TID, *traceEvents[2].TID)
	})

	t.Run("includes only valid tasks when snapshot has mixed tasks", func(t *testing.T) {
		snap := &eventserver.SessionSnapshot{
			Tasks: []eventtypes.Task{
				// valid
				makeTimelineTask(timelineTaskOpts{
					jobID:         "job-a",
					componentType: "worker",
					componentID:   "worker-a",
					nodeIP:        "10.0.0.1",
					eventName:     "task:execute",
				}),
				// no profile data
				{JobID: "job-a"},
				// unsupported component
				makeTimelineTask(timelineTaskOpts{
					componentType: "raylet",
					componentID:   "raylet-a",
					nodeIP:        "10.0.0.1",
				}),
				// missing node IP
				{
					JobID: "job-a",
					ProfileData: &eventtypes.ProfileData{
						ComponentType: "worker",
						ComponentID:   "worker-b",
						Events: []eventtypes.ProfileEventRaw{{
							EventName: "task:execute",
							StartTime: 1_000_000,
							EndTime:   2_000_000,
						}},
					},
				},
				// valid, different job
				makeTimelineTask(timelineTaskOpts{
					jobID:         "job-b",
					componentType: "worker",
					componentID:   "worker-c",
					nodeIP:        "10.0.0.2",
					eventName:     "task:execute",
				}),
			},
		}

		traceEvents := filterTraceEvents(getTasksTimeline(snap, ""))
		require.Len(t, traceEvents, 2)

		jobIDs := []string{
			traceEvents[0].Args["job_id"].(string),
			traceEvents[1].Args["job_id"].(string),
		}
		assert.ElementsMatch(t, []string{"job-a", "job-b"}, jobIDs)
	})

	t.Run("falls back to GetFuncName", func(t *testing.T) {
		snap := &eventserver.SessionSnapshot{
			Tasks: []eventtypes.Task{
				makeTimelineTask(timelineTaskOpts{
					funcOrClassName: "",
					taskFunc: &eventtypes.FunctionDescriptor{
						PythonFunctionDescriptor: &eventtypes.PythonFunctionDescriptor{
							FunctionName: "fallback_func",
						},
					},
					componentType: "worker",
					componentID:   "worker-a",
					nodeIP:        "10.0.0.1",
					eventName:     "task:execute",
				}),
			},
		}

		traceEvents := filterTraceEvents(getTasksTimeline(snap, ""))
		require.Len(t, traceEvents, 1)
		assert.Equal(t, "fallback_func", traceEvents[0].Args["func_or_class_name"])
	})

	t.Run("extraData overrides task id", func(t *testing.T) {
		snap := &eventserver.SessionSnapshot{
			Tasks: []eventtypes.Task{
				makeTimelineTask(timelineTaskOpts{
					taskID:        driverTaskID,
					componentType: "worker",
					componentID:   "worker-a",
					nodeIP:        "10.0.0.1",
					eventName:     "task:execute",
					extraData:     `{"task_id":"cccccccccccccccccccccccccccccccccccccccccccc"}`,
				}),
			},
		}

		traceEvents := filterTraceEvents(getTasksTimeline(snap, ""))
		require.Len(t, traceEvents, 1)
		assert.Equal(t, "cccccccccccccccccccccccccccccccccccccccccccc", traceEvents[0].Args["task_id"])
	})

	t.Run("task prefix event uses extraData name", func(t *testing.T) {
		snap := &eventserver.SessionSnapshot{
			Tasks: []eventtypes.Task{
				makeTimelineTask(timelineTaskOpts{
					componentType: "worker",
					componentID:   "worker-a",
					nodeIP:        "10.0.0.1",
					eventName:     "task::slow_task",
					extraData:     `{"name":"slow_task"}`,
				}),
			},
		}

		traceEvents := filterTraceEvents(getTasksTimeline(snap, ""))
		require.Len(t, traceEvents, 1)
		assert.Equal(t, "slow_task", traceEvents[0].Name)
		assert.Equal(t, "generic_work", traceEvents[0].Color)
		assert.Equal(t, "slow_task", traceEvents[0].Args["name"])
	})

	t.Run("invalid extraData does not panic", func(t *testing.T) {
		snap := &eventserver.SessionSnapshot{
			Tasks: []eventtypes.Task{
				makeTimelineTask(timelineTaskOpts{
					componentType: "worker",
					componentID:   "worker-a",
					nodeIP:        "10.0.0.1",
					eventName:     "task:execute",
					extraData:     "not-json",
				}),
			},
		}

		traceEvents := filterTraceEvents(getTasksTimeline(snap, ""))
		require.Len(t, traceEvents, 1)
		assert.Equal(t, "task:execute", traceEvents[0].Name)
	})

	t.Run("extracts actor id for actor tasks", func(t *testing.T) {
		snap := &eventserver.SessionSnapshot{
			Tasks: []eventtypes.Task{
				makeTimelineTask(timelineTaskOpts{
					taskID:        actorTaskID,
					componentType: "worker",
					componentID:   "worker-a",
					nodeIP:        "10.0.0.1",
					eventName:     "task:execute",
				}),
			},
		}

		traceEvents := filterTraceEvents(getTasksTimeline(snap, ""))
		require.Len(t, traceEvents, 1)
		assert.Equal(t, actorID, traceEvents[0].Args["actor_id"])
	})

	t.Run("assigns pid and tid across nodes and workers", func(t *testing.T) {
		snap := &eventserver.SessionSnapshot{
			Tasks: []eventtypes.Task{
				makeTimelineTask(timelineTaskOpts{
					jobID:         "job-a",
					componentType: "worker",
					componentID:   "worker-a",
					nodeIP:        "10.0.0.1",
					eventName:     "task:execute",
				}),
				makeTimelineTask(timelineTaskOpts{
					jobID:         "job-a",
					componentType: "worker",
					componentID:   "worker-b",
					nodeIP:        "10.0.0.1",
					eventName:     "task:execute",
				}),
				makeTimelineTask(timelineTaskOpts{
					jobID:         "job-a",
					componentType: "worker",
					componentID:   "worker-c",
					nodeIP:        "10.0.0.2",
					eventName:     "task:execute",
				}),
			},
		}

		traceEvents := filterTraceEvents(getTasksTimeline(snap, ""))
		require.Len(t, traceEvents, 3)

		assert.Equal(t, traceEvents[0].PID, traceEvents[1].PID)
		assert.NotEqual(t, traceEvents[0].PID, traceEvents[2].PID)
		assert.NotEqual(t, *traceEvents[0].TID, *traceEvents[1].TID)
	})
}

type timelineTaskOpts struct {
	taskID          string
	jobID           string
	taskAttempt     int
	funcOrClassName string
	taskFunc        *eventtypes.FunctionDescriptor
	componentType   string
	componentID     string
	nodeIP          string
	eventName       string
	startTime       int64
	endTime         int64
	extraData       string
}

func makeTimelineTask(opts timelineTaskOpts) eventtypes.Task {
	if opts.taskID == "" {
		opts.taskID = driverTaskID
	}
	if opts.jobID == "" {
		opts.jobID = "01000000"
	}
	if opts.componentType == "" {
		opts.componentType = "worker"
	}
	if opts.componentID == "" {
		opts.componentID = "worker-a"
	}
	if opts.nodeIP == "" {
		opts.nodeIP = "10.0.0.1"
	}
	if opts.eventName == "" {
		opts.eventName = "task:execute"
	}
	if opts.startTime == 0 {
		opts.startTime = 1_000_000
	}
	if opts.endTime == 0 {
		opts.endTime = 2_000_000
	}

	return eventtypes.Task{
		TaskID:          opts.taskID,
		JobID:           opts.jobID,
		TaskAttempt:     opts.taskAttempt,
		FuncOrClassName: opts.funcOrClassName,
		TaskFunc:        opts.taskFunc,
		ProfileData: &eventtypes.ProfileData{
			ComponentType: opts.componentType,
			ComponentID:   opts.componentID,
			NodeIPAddress: opts.nodeIP,
			Events: []eventtypes.ProfileEventRaw{{
				EventName: opts.eventName,
				StartTime: opts.startTime,
				EndTime:   opts.endTime,
				ExtraData: opts.extraData,
			}},
		},
	}
}

func filterTraceEvents(events []eventtypes.ChromeTraceEvent) []eventtypes.ChromeTraceEvent {
	traceEvents := make([]eventtypes.ChromeTraceEvent, 0)
	for _, event := range events {
		if event.Phase == "X" {
			traceEvents = append(traceEvents, event)
		}
	}
	return traceEvents
}

func findMetadataEvent(events []eventtypes.ChromeTraceEvent, name string) *eventtypes.ChromeTraceEvent {
	for i := range events {
		if events[i].Phase == "M" && events[i].Name == name {
			return &events[i]
		}
	}
	return nil
}
