package historyserver

import (
	"encoding/json"
	"strings"

	"github.com/ray-project/kuberay/historyserver/pkg/eventserver"
	eventtypes "github.com/ray-project/kuberay/historyserver/pkg/eventserver/types"
)

// getTasksTimeline returns timeline data in Chrome Tracing Format.
// Output format matches Ray Dashboard's /api/v0/tasks/timeline endpoint.
func getTasksTimeline(snap *eventserver.SessionSnapshot, jobID string) []eventtypes.ChromeTraceEvent {
	var tasks []eventtypes.Task
	if snap != nil {
		if jobID != "" {
			for _, task := range snap.Tasks {
				if task.JobID == jobID {
					tasks = append(tasks, task)
				}
			}
		} else {
			tasks = snap.Tasks
		}
	}

	if len(tasks) == 0 {
		return []eventtypes.ChromeTraceEvent{}
	}

	events := []eventtypes.ChromeTraceEvent{}

	filteredTasks := make([]eventtypes.Task, 0, len(tasks))
	for _, task := range tasks {
		if task.ProfileData == nil || len(task.ProfileData.Events) == 0 {
			continue
		}
		// Only include worker and driver components (consistent with Ray's profiling implementation in profiling.py)
		componentType := task.ProfileData.ComponentType
		if componentType != "worker" && componentType != "driver" {
			continue
		}
		if task.ProfileData.NodeIPAddress == "" {
			continue
		}
		filteredTasks = append(filteredTasks, task)
	}

	// Build PID/TID mappings
	// PID: Node IP -> numeric ID
	// TID: clusterID (componentType:componentId) -> globally unique numeric ID
	nodeIPToPID := make(map[string]int)
	nodeIPToClusterIDToTID := make(map[string]map[string]int) // nodeIP -> clusterID (componentType:componentId) -> tid
	pidCounter := 0
	tidCounter := 0

	// First pass: collect all unique nodes and workers
	for _, task := range filteredTasks {
		nodeIP := task.ProfileData.NodeIPAddress
		clusterID := task.ProfileData.ComponentType + ":" + task.ProfileData.ComponentID
		if _, exists := nodeIPToPID[nodeIP]; !exists {
			nodeIPToPID[nodeIP] = pidCounter
			pidCounter++
			nodeIPToClusterIDToTID[nodeIP] = make(map[string]int)
		}
		if _, exists := nodeIPToClusterIDToTID[nodeIP][clusterID]; !exists {
			nodeIPToClusterIDToTID[nodeIP][clusterID] = tidCounter
			tidCounter++
		}
	}

	// Generate process_name and thread_name metadata events
	for nodeIP, pid := range nodeIPToPID {
		events = append(events, eventtypes.ChromeTraceEvent{
			Name:  "process_name",
			PID:   pid,
			TID:   nil,
			Phase: "M",
			Args: map[string]any{
				"name": "Node " + nodeIP,
			},
		})

		for clusterID, tid := range nodeIPToClusterIDToTID[nodeIP] {
			tidVal := tid
			events = append(events, eventtypes.ChromeTraceEvent{
				Name:  "thread_name",
				PID:   pid,
				TID:   &tidVal,
				Phase: "M",
				Args: map[string]any{
					"name": clusterID,
				},
			})
		}
	}

	// Generate trace events from ProfileData
	for _, task := range filteredTasks {
		nodeIP := task.ProfileData.NodeIPAddress
		clusterID := task.ProfileData.ComponentType + ":" + task.ProfileData.ComponentID

		pid, ok := nodeIPToPID[nodeIP]
		if !ok {
			continue
		}
		var tidPtr *int
		if tid, ok := nodeIPToClusterIDToTID[nodeIP][clusterID]; ok {
			tidVal := tid
			tidPtr = &tidVal
		} else {
			// This shouldn't happen if first pass worked correctly,
			// but skip to avoid null TID
			continue
		}

		for _, profEvent := range task.ProfileData.Events {
			// Convert nanoseconds to microseconds
			startTimeUs := float64(profEvent.StartTime) / 1000.0
			durationUs := float64(profEvent.EndTime-profEvent.StartTime) / 1000.0

			// Parse extraData for additional fields
			var extraData map[string]any
			if profEvent.ExtraData != "" {
				_ = json.Unmarshal([]byte(profEvent.ExtraData), &extraData)
			}

			// Determine task_id and func_or_class_name
			taskIDForArgs := task.TaskID
			funcOrClassName := task.FuncOrClassName

			// Fallback to GetFuncName() if FuncOrClassName is empty
			// This ensures consistency with Ray's profiling.py which uses task["func_or_class_name"]
			// from TASK_DEFINITION_EVENT, and handles cases where TASK_PROFILE_EVENT is missing
			if funcOrClassName == "" {
				funcOrClassName = task.GetFuncName()
			}

			// Try to get from extraData if available (for hex format task_id)
			if extraData != nil {
				if tid, ok := extraData["task_id"].(string); ok && tid != "" {
					taskIDForArgs = tid
				}
			}

			// Build args
			actorID := extractActorIDFromTaskID(taskIDForArgs)
			args := map[string]any{
				"task_id":            taskIDForArgs,
				"job_id":             task.JobID,
				"attempt_number":     task.TaskAttempt,
				"func_or_class_name": funcOrClassName,
				"actor_id":           nil,
			}

			if actorID != "" {
				args["actor_id"] = actorID
			}

			// Determine event name for display
			eventName := profEvent.EventName
			displayName := profEvent.EventName

			// For overall task events like "task::slow_task", use the full name from extraData
			if strings.HasPrefix(profEvent.EventName, "task::") && extraData != nil {
				if name, ok := extraData["name"].(string); ok && name != "" {
					displayName = name
					args["name"] = name
				}
			}

			traceEvent := eventtypes.ChromeTraceEvent{
				Category:  eventName,
				Name:      displayName,
				PID:       pid,
				TID:       tidPtr,
				Timestamp: &startTimeUs,
				Duration:  &durationUs,
				Color:     getChromeTraceColor(eventName),
				Args:      args,
				Phase:     "X",
			}

			events = append(events, traceEvent)
		}
	}

	return events
}

// getChromeTraceColor maps event names to Chrome trace colors
// Based on Ray's _default_color_mapping in profiling.py
func getChromeTraceColor(eventName string) string {
	// Handle task::xxx pattern (overall task event)
	if strings.HasPrefix(eventName, "task::") {
		return "generic_work"
	}

	// Direct mapping for known event names
	// This logic follows Ray's profiling implementation:
	// https://github.com/ray-project/ray/blob/68d01c4c48a59c7768ec9c2359a1859966c446b6/python/ray/_private/profiling.py#L25
	switch eventName {
	case "task:deserialize_arguments":
		return "rail_load"
	case "task:execute":
		return "rail_animation"
	case "task:store_outputs":
		return "rail_idle"
	case "task:submit_task", "task":
		return "rail_response"
	case "worker_idle":
		return "cq_build_abandoned"
	case "ray.get":
		return "good"
	case "ray.put":
		return "terrible"
	case "ray.wait":
		return "vsync_highlight_color"
	case "submit_task":
		return "background_memory_dump"
	case "wait_for_function", "fetch_and_run_function", "register_remote_function":
		return "detailed_memory_dump"
	default:
		return "generic_work"
	}
}

// extractActorIDFromTaskID extracts the ActorID from a TaskID following Ray's ID specification.
//
// Design doc: src/ray/design_docs/id_specification.md
// - TaskID: 8B unique + 16B ActorID (total 24 bytes = 48 hex chars)
// - ActorID: 12B unique + 4B JobID (total 16 bytes = 32 hex chars)
//
// For a 48-character hex TaskID, the last 32 hex characters (bytes 16–48)
// correspond to the ActorID. This function further checks the "unique" portion
// of the ActorID (first 24 hex chars) and returns an empty string if it is all Fs,
// which indicates normal/driver tasks with no associated actor.
func extractActorIDFromTaskID(taskIDHex string) string {
	if len(taskIDHex) != 48 {
		return "" // can't process if encoded in base64
	}

	actorPortion := taskIDHex[16:40] // 24 chars for actor id (12 bytes)
	jobPortion := taskIDHex[40:48]   // 8 chars for job id (4 bytes)

	// Check if all Fs (no actor)
	if strings.ToLower(actorPortion) == "ffffffffffffffffffffffff" {
		return ""
	}

	return actorPortion + jobPortion
}
