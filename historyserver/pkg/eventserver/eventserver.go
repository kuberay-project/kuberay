package eventserver

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/ray-project/kuberay/historyserver/pkg/compression"
	"github.com/ray-project/kuberay/historyserver/pkg/eventserver/types"
	"github.com/ray-project/kuberay/historyserver/pkg/storage"
	"github.com/ray-project/kuberay/historyserver/pkg/storage/clusterlogs"
	"github.com/ray-project/kuberay/historyserver/pkg/utils"
)

type EventHandler struct {
	reader storage.StorageReader

	ClusterTaskMap     *types.ClusterTaskMap
	ClusterActorMap    *types.ClusterActorMap
	ClusterJobMap      *types.ClusterJobMap
	ClusterNodeMap     *types.ClusterNodeMap
	ClusterLogEventMap *types.ClusterLogEventMap // For /events API (Log Events from logs/events/)
}

var eventFilePattern = regexp.MustCompile(`-\d{4}-\d{2}-\d{2}-\d{2}(-\d+)?(\.jsonl)?(\.gz)?$`)

// taskPrefix is extracted to avoid hard-coded "task::" usage
const taskPrefix = "task::"

func isValidEventFile(fileName string) bool {
	// Skip directories
	if strings.HasSuffix(fileName, "/") {
		return false
	}
	// Accept both legacy files ({nodeId}-{YYYY-MM-DD-HH}) and the new
	// disk-first JSONL files ({nodeId}-{YYYY-MM-DD-HH}.jsonl[.gz]).
	return eventFilePattern.MatchString(fileName)
}

// maxJSONLLineBytes bounds a single JSONL line during scanning. It matches the
// collector's default maximum rotated-file size (see eventcollector defaults),
// so any single event the collector can persist to a file can also be read back
// here. Without this, bufio.Scanner's default 64 KiB token limit would return
// ErrTooLong for a large event (e.g. a big task profile) and cause the entire
// file to be skipped.
const maxJSONLLineBytes = 100 * 1024 * 1024

// DecodeEventFileBytes parses the raw bytes of an event file.
//
// Gzip decompression is handled at the reader level in ProcessSingleSession
// via compression.ReadCompressedContent, so data arriving here is always
// already decompressed.
//
// Format auto-detection (JSON array vs JSONL) is always enabled so the
// historyserver remains forward-compatible with new collectors that emit JSONL
// and backward-compatible with legacy data stored as a JSON array.
func DecodeEventFileBytes(fileName string, raw []byte) ([]map[string]any, error) {
	trimmed := bytes.TrimLeft(raw, " \t\r\n")
	if len(trimmed) == 0 {
		return nil, nil
	}

	// Format auto-detection is always enabled.
	// JSON array (legacy format) starts with '['.
	if trimmed[0] == '[' {
		var out []map[string]any
		if err := json.Unmarshal(raw, &out); err != nil {
			return nil, fmt.Errorf("unmarshal JSON array %s: %w", fileName, err)
		}
		return out, nil
	}

	// Otherwise treat as JSONL: one object per non-empty line.
	var out []map[string]any
	scanner := bufio.NewScanner(bytes.NewReader(raw))
	scanner.Buffer(make([]byte, 0, 64*1024), maxJSONLLineBytes)
	for scanner.Scan() {
		line := bytes.TrimSpace(scanner.Bytes())
		if len(line) == 0 {
			continue
		}
		var obj map[string]any
		if err := json.Unmarshal(line, &obj); err != nil {
			logrus.Warnf("Skipping malformed JSONL line in %s: %v", fileName, err)
			continue
		}
		out = append(out, obj)
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("scan JSONL %s: %w", fileName, err)
	}
	return out, nil
}

func NewEventHandler(reader storage.StorageReader) *EventHandler {
	return &EventHandler{
		reader: reader,
		ClusterTaskMap: &types.ClusterTaskMap{
			ClusterTaskMap: make(map[string]*types.TaskMap),
		},
		ClusterActorMap: &types.ClusterActorMap{
			ClusterActorMap: make(map[string]*types.ActorMap),
		},
		ClusterJobMap: &types.ClusterJobMap{
			ClusterJobMap: make(map[string]*types.JobMap),
		},
		ClusterNodeMap: &types.ClusterNodeMap{
			ClusterNodeMap: make(map[string]*types.NodeMap),
		},
		ClusterLogEventMap: types.NewClusterLogEventMap(),
	}
}

// storeEvent unmarshals the event map into the correct actor/task struct and then stores it into the corresonding list.
func (h *EventHandler) storeEvent(clusterSessionKey string, eventMap map[string]any) error {
	eventTypeVal, ok := eventMap["eventType"]
	if !ok {
		return fmt.Errorf("event missing 'eventType' field")
	}
	eventTypeStr, ok := eventTypeVal.(string)
	if !ok {
		return fmt.Errorf("eventType is not a string, got %T", eventTypeVal)
	}
	eventType := types.EventType(eventTypeStr)

	logrus.Debugf("current eventType: %v", eventType)
	switch eventType {
	case types.TASK_DEFINITION_EVENT:
		return h.handleTaskDefinitionEvent(eventMap, clusterSessionKey, false)
	case types.TASK_LIFECYCLE_EVENT:
		return h.handleTaskLifecycleEvent(eventMap, clusterSessionKey)
	case types.ACTOR_DEFINITION_EVENT:
		actorDef, ok := eventMap["actorDefinitionEvent"]
		if !ok {
			return fmt.Errorf("event does not have 'actorDefinitionEvent'")
		}
		jsonActorDefinition, err := json.Marshal(actorDef)
		if err != nil {
			return err
		}

		var currActor types.Actor
		if err := json.Unmarshal(jsonActorDefinition, &currActor); err != nil {
			return err
		}

		normalizeActorIDsToHex(&currActor)

		// Use CreateOrMergeActor pattern (same as Task)
		actorMap := h.ClusterActorMap.GetOrCreateActorMap(clusterSessionKey)
		actorMap.CreateOrMergeActor(currActor.ActorID, func(a *types.Actor) {
			// Preserve lifecycle-derived fields that may have arrived first
			existingEvents := a.Events
			existingState := a.State
			existingStartTime := a.StartTime
			existingEndTime := a.EndTime
			existingNumRestarts := a.NumRestarts
			existingPID := a.PID
			existingExitDetails := a.ExitDetails
			existingAddress := a.Address

			// Overwrite with definition fields
			*a = currActor

			// Restore lifecycle-derived fields if they existed
			if len(existingEvents) > 0 {
				a.Events = existingEvents
				a.State = existingState
				a.StartTime = existingStartTime
				a.EndTime = existingEndTime
				a.NumRestarts = existingNumRestarts
				a.PID = existingPID
				a.ExitDetails = existingExitDetails
				a.Address = existingAddress
			}
		})
	case types.ACTOR_LIFECYCLE_EVENT:
		lifecycleEvent, ok := eventMap["actorLifecycleEvent"].(map[string]any)
		if !ok {
			return fmt.Errorf("invalid actorLifecycleEvent format")
		}

		actorId, _ := lifecycleEvent["actorId"].(string)
		actorId = normalizeIDToHex(actorId)
		transitions, _ := lifecycleEvent["stateTransitions"].([]any)

		if len(transitions) == 0 || actorId == "" {
			return nil
		}

		// Parse state transitions into ActorStateEvent slice
		var stateEvents []types.ActorStateEvent
		for _, transition := range transitions {
			tr, ok := transition.(map[string]any)
			if !ok {
				continue
			}
			state, _ := tr["state"].(string)
			timestampStr, _ := tr["timestamp"].(string)
			nodeId, _ := tr["nodeId"].(string)
			workerId, _ := tr["workerId"].(string)
			reprName, _ := tr["reprName"].(string)

			// Convert IDs from base64 to hex
			nodeId = normalizeIDToHex(nodeId)
			workerId = normalizeIDToHex(workerId)

			var timestamp time.Time
			if timestampStr != "" {
				timestamp, _ = time.Parse(time.RFC3339Nano, timestampStr)
			}

			// DeathCause is a complex object, store as JSON string
			var deathCause string
			if dc, ok := tr["deathCause"]; ok {
				if dcBytes, err := json.Marshal(dc); err == nil {
					deathCause = string(dcBytes)
				}
			}

			stateEvents = append(stateEvents, types.ActorStateEvent{
				State:      types.StateType(state),
				Timestamp:  timestamp,
				NodeID:     nodeId,
				WorkerID:   workerId,
				ReprName:   reprName,
				DeathCause: deathCause,
			})
		}

		if len(stateEvents) == 0 {
			return nil
		}

		actorMap := h.ClusterActorMap.GetOrCreateActorMap(clusterSessionKey)
		actorMap.CreateOrMergeActor(actorId, func(a *types.Actor) {
			// Ensure ActorID is set (in case LIFECYCLE arrives before DEFINITION)
			a.ActorID = actorId

			// --- DEDUPLICATION using (State + Timestamp) as unique key ---
			// Build a set of existing event keys to detect duplicates
			type eventKey struct {
				State     string
				Timestamp int64
			}
			existingKeys := make(map[eventKey]bool)
			for _, e := range a.Events {
				existingKeys[eventKey{string(e.State), e.Timestamp.UnixNano()}] = true
			}

			// Only append events that haven't been seen before
			for _, e := range stateEvents {
				key := eventKey{string(e.State), e.Timestamp.UnixNano()}
				if !existingKeys[key] {
					a.Events = append(a.Events, e)
					existingKeys[key] = true
				}
			}

			// Sort events by timestamp to ensure correct order
			sort.Slice(a.Events, func(i, j int) bool {
				return a.Events[i].Timestamp.Before(a.Events[j].Timestamp)
			})

			if len(a.Events) == 0 {
				return
			}

			lastEvent := a.Events[len(a.Events)-1]

			// --- UPDATE STATE ---
			a.State = lastEvent.State

			// --- UPDATE ADDRESS from ALIVE state ---
			// NodeID and WorkerID are only populated in ALIVE state
			for i := len(a.Events) - 1; i >= 0; i-- {
				if a.Events[i].State == types.ALIVE && a.Events[i].NodeID != "" {
					a.Address.NodeID = a.Events[i].NodeID
					a.Address.WorkerID = a.Events[i].WorkerID
					break
				}
			}

			// --- UPDATE ReprName from latest ---
			if lastEvent.ReprName != "" {
				a.ReprName = lastEvent.ReprName
			}

			// --- CALCULATE StartTime (first ALIVE timestamp) ---
			if a.StartTime.IsZero() {
				for _, e := range a.Events {
					if e.State == types.ALIVE {
						a.StartTime = e.Timestamp
						break
					}
				}
			}

			// --- HANDLE DEAD state ---
			if lastEvent.State == types.DEAD {
				a.EndTime = lastEvent.Timestamp

				// Parse deathCause to extract PID, IP, errorMessage
				if lastEvent.DeathCause != "" {
					var deathCauseMap map[string]any
					if err := json.Unmarshal([]byte(lastEvent.DeathCause), &deathCauseMap); err == nil {
						if ctx, ok := deathCauseMap["actorDiedErrorContext"].(map[string]any); ok {
							// Extract PID
							if pid, ok := ctx["pid"].(float64); ok {
								a.PID = int(pid)
							}
							// Extract IP address
							if ip, ok := ctx["nodeIpAddress"].(string); ok {
								a.Address.IPAddress = ip
							}
							// Extract error message as ExitDetails
							if errMsg, ok := ctx["errorMessage"].(string); ok {
								a.ExitDetails = errMsg
							}
						}
					}
				}
			}

			// --- COUNT RESTARTS ---
			restartCount := 0
			for _, e := range a.Events {
				if e.State == types.RESTARTING {
					restartCount++
				}
			}
			a.NumRestarts = restartCount
		})
	case types.ACTOR_TASK_DEFINITION_EVENT:
		return h.handleTaskDefinitionEvent(eventMap, clusterSessionKey, true)
	case types.DRIVER_JOB_DEFINITION_EVENT:
		// NOTE: When event comes in, JobID will be in base64, processing will convert it to Hex
		jobDef, ok := eventMap["driverJobDefinitionEvent"]
		if !ok {
			return fmt.Errorf("event does not have 'driverJobDefinitionEvent'")
		}

		jsonDriverJobDefinition, err := json.Marshal(jobDef)
		if err != nil {
			return err
		}

		var currJob types.Job
		if err := json.Unmarshal(jsonDriverJobDefinition, &currJob); err != nil {
			return err
		}

		// Convert JobID from base64 to hex
		currJob.JobID, err = utils.ConvertBase64ToHex(currJob.JobID)
		if err != nil {
			logrus.Errorf("Failed to convert JobID from base64 to Hex, will keep JobID in base64: %v", err)
		}

		// Convert DriverNodeID from base64 to hex
		currJob.DriverNodeID, err = utils.ConvertBase64ToHex(currJob.DriverNodeID)
		if err != nil {
			logrus.Errorf("Failed to convert DriverNodeID from base64 to hex, will keep DriverNodeID in base64: %v", err)
		}

		jobMap := h.ClusterJobMap.GetOrCreateJobMap(clusterSessionKey)
		jobMap.CreateOrMergeJob(currJob.JobID, func(j *types.Job) {
			// If for some reason jobID is empty, we will keep whatever is in 'j'
			var existingJobID string
			if currJob.JobID == "" {
				existingJobID = j.JobID
			}

			// ========== Lifecycle-derived fields ==========
			// These fields are set by DRIVER_JOB_LIFECYCLE_EVENT
			// We need to preserve them if lifecycle event arrived before definition event
			existingStateTransitions := j.StateTransitions
			existingState := j.State
			existingStartTime := j.StartTime
			existingEndTime := j.EndTime

			// Overwrite with definition fields
			*j = currJob

			// Restore lifecycle-derived fields if they existed
			if len(existingStateTransitions) > 0 {
				if existingJobID != "" {
					// This means that jobID was somehow empty.
					j.JobID = existingJobID
				}
				j.StateTransitions = existingStateTransitions
				j.State = existingState
				j.StartTime = existingStartTime
				j.EndTime = existingEndTime
			}

			// ========== Definition-only fields ==========
			// Status, StatusTransitions, Message, DriverExitCode
			// are ONLY set by DRIVER_JOB_DEFINITION_EVENT
			// They are already in currJob, no need to restore
		})
	case types.DRIVER_JOB_LIFECYCLE_EVENT:
		// NOTE: When event comes in, JobID will be in base64, processing will convert it to Hex
		jobLifecycleEvent, ok := eventMap["driverJobLifecycleEvent"].(map[string]any)
		if !ok {
			return fmt.Errorf("invalid driverJobLifecycleEvent format")
		}

		// Get JobID and also convert JobID to hex from base64
		// Will leave it as it if it's empty or somehow not a string
		jobId, ok := jobLifecycleEvent["jobId"].(string)
		if !ok {
			logrus.Errorf("jobID is missing or is not a string, leaving it as is")
		} else {
			jobId, _ = utils.ConvertBase64ToHex(jobId)
		}
		stateTransitionUnstructed, _ := jobLifecycleEvent["stateTransitions"].([]any)

		if len(stateTransitionUnstructed) == 0 || jobId == "" {
			return nil
		}

		// TODO(chiayi): Will need to convert status timeline once it is added as well
		// Following fields are related to status transition:
		//  - status
		//  - message
		//  - errorType
		//  - driverExitCode
		var stateTransitions []types.JobStateTransition
		for _, transition := range stateTransitionUnstructed {
			tr, ok := transition.(map[string]any)
			if !ok {
				continue
			}
			state, _ := tr["state"].(string)
			timestampStr, _ := tr["timestamp"].(string)

			var timestamp time.Time
			if timestampStr != "" {
				timestamp, _ = time.Parse(time.RFC3339Nano, timestampStr)
			}

			stateTransitions = append(stateTransitions, types.JobStateTransition{
				State:     types.JobState(state),
				Timestamp: timestamp,
			})
		}

		if len(stateTransitions) == 0 {
			return nil
		}

		jobMap := h.ClusterJobMap.GetOrCreateJobMap(clusterSessionKey)
		jobMap.CreateOrMergeJob(jobId, func(j *types.Job) {
			// TODO(chiayi): take care of status (job progress) state if part of DriverJobLifecycleEvent
			j.JobID = jobId

			type stateTransitionKey struct {
				State     string
				Timestamp int64
			}

			existingStateKeys := make(map[stateTransitionKey]bool)
			for _, t := range j.StateTransitions {
				existingStateKeys[stateTransitionKey{string(t.State), t.Timestamp.UnixNano()}] = true
			}

			for _, t := range stateTransitions {
				key := stateTransitionKey{string(t.State), t.Timestamp.UnixNano()}
				if !existingStateKeys[key] {
					j.StateTransitions = append(j.StateTransitions, t)
					existingStateKeys[key] = true
				}
			}

			sort.Slice(j.StateTransitions, func(i, k int) bool {
				return j.StateTransitions[i].Timestamp.Before(j.StateTransitions[k].Timestamp)
			})

			if len(j.StateTransitions) == 0 {
				return
			}

			lastStateTransition := j.StateTransitions[len(j.StateTransitions)-1]
			j.State = lastStateTransition.State

			if j.StartTime.IsZero() {
				for _, t := range j.StateTransitions {
					if t.State == types.CREATED {
						j.StartTime = t.Timestamp
						break
					}
				}
			}

			if lastStateTransition.State == types.JOBFINISHED {
				j.EndTime = lastStateTransition.Timestamp
			}
		})
	case types.NODE_DEFINITION_EVENT:
		return h.handleNodeDefinitionEvent(eventMap, clusterSessionKey)
	case types.NODE_LIFECYCLE_EVENT:
		return h.handleNodeLifecycleEvent(eventMap, clusterSessionKey)
	case types.TASK_PROFILE_EVENT:
		return h.handleTaskProfileEvent(eventMap, clusterSessionKey)
	default:
		logrus.Infof("Event not supported, skipping: %v", eventMap)
	}

	return nil
}

// getClusterLogPathPrefix returns the cluster storage prefix:
// e.g., cluster-history/{ownerKind}/{namespace}/{ownerName}/{clusterName}
func (h *EventHandler) getClusterLogPathPrefix(clusterInfo utils.ClusterInfo) string {
	return clusterlogs.Prefix("", clusterInfo.OwnerKind, clusterInfo.OwnerName, clusterInfo.Namespace, clusterInfo.Name)
}

// getAllJobEventFiles get all the job event files for the given cluster.
func (h *EventHandler) getAllJobEventFiles(clusterInfo utils.ClusterInfo) []string {
	var allJobFiles []string
	clusterLogPathPrefix := h.getClusterLogPathPrefix(clusterInfo)

	// Check candidate prefixes under each node (<sessionName>/<nodeName>/job_events/)
	var candidatePrefixes []string
	for _, nodeName := range clusterlogs.ListSessionNodeDirs(h.reader, clusterLogPathPrefix, clusterInfo.SessionName) {
		candidatePrefixes = append(candidatePrefixes, clusterlogs.RelJobEventsDir(clusterInfo.SessionName, nodeName, "")+"/")
	}

	for _, jobEventDirPrefix := range candidatePrefixes {
		jobDirList := h.reader.ListFiles(clusterLogPathPrefix, jobEventDirPrefix)
		for _, jobDir := range jobDirList {
			if !strings.HasSuffix(jobDir, "/") {
				continue
			}
			jobDirPath := jobEventDirPrefix + jobDir
			jobFiles := h.reader.ListFiles(clusterLogPathPrefix, jobDirPath)
			for _, jobFile := range jobFiles {
				if isValidEventFile(jobFile) {
					allJobFiles = append(allJobFiles, jobDirPath+jobFile)
				}
			}
		}
	}
	return allJobFiles
}

// getAllNodeEventFiles retrieves all node event files for the given cluster
func (h *EventHandler) getAllNodeEventFiles(clusterInfo utils.ClusterInfo) []string {
	clusterLogPathPrefix := h.getClusterLogPathPrefix(clusterInfo)

	// Check candidate prefixes under each node (<sessionName>/<nodeName>/node_events/)
	var candidatePrefixes []string
	for _, nodeName := range clusterlogs.ListSessionNodeDirs(h.reader, clusterLogPathPrefix, clusterInfo.SessionName) {
		candidatePrefixes = append(candidatePrefixes, clusterlogs.RelNodeEventsDir(clusterInfo.SessionName, nodeName)+"/")
	}

	var nodeEventFiles []string
	for _, nodeEventDirPrefix := range candidatePrefixes {
		nodeEventFileNames := h.reader.ListFiles(clusterLogPathPrefix, nodeEventDirPrefix)
		for _, fileName := range nodeEventFileNames {
			if isValidEventFile(fileName) {
				fullPath := nodeEventDirPrefix + fileName
				nodeEventFiles = append(nodeEventFiles, fullPath)
			}
		}
	}
	return nodeEventFiles
}

// GetTasks returns a slice of thread-safe deep copies of all task attempts for a given cluster session.
// Deep copy ensures the returned data is safe to use after the lock is released.
func (h *EventHandler) getTasks(clusterSessionKey string) []types.Task {
	h.ClusterTaskMap.RLock()
	defer h.ClusterTaskMap.RUnlock()

	taskMap, ok := h.ClusterTaskMap.ClusterTaskMap[clusterSessionKey]
	if !ok {
		// TODO(jiangjiawei1103): Add error handling.
		logrus.Errorf("Task map not found for cluster session: %s", clusterSessionKey)
		return []types.Task{}
	}

	taskMap.Lock()
	defer taskMap.Unlock()

	// Flatten all task attempts into a single slice with deep copy.
	allTasks := make([]types.Task, 0)
	for _, taskAttempts := range taskMap.TaskMap {
		for _, taskAttempt := range taskAttempts {
			allTasks = append(allTasks, taskAttempt.DeepCopy())
		}
	}

	return allTasks
}

// GetActorsMap returns a thread-safe deep copy of all actors as a map for a given cluster
func (h *EventHandler) getActorsMap(clusterName string) map[string]types.Actor {
	h.ClusterActorMap.RLock()
	defer h.ClusterActorMap.RUnlock()

	actorMap, ok := h.ClusterActorMap.ClusterActorMap[clusterName]
	if !ok {
		return map[string]types.Actor{}
	}

	actorMap.Lock()
	defer actorMap.Unlock()

	actors := make(map[string]types.Actor, len(actorMap.ActorMap))
	for id, actor := range actorMap.ActorMap {
		actors[id] = actor.DeepCopy()
	}
	return actors
}

func (h *EventHandler) getJobsMap(clusterName string) map[string]types.Job {
	h.ClusterJobMap.RLock()
	defer h.ClusterJobMap.RUnlock()

	jobMap, ok := h.ClusterJobMap.ClusterJobMap[clusterName]
	if !ok {
		return map[string]types.Job{}
	}

	jobMap.Lock()
	defer jobMap.Unlock()

	jobs := make(map[string]types.Job, len(jobMap.JobMap))
	for id, job := range jobMap.JobMap {
		jobs[id] = job.DeepCopy()
	}
	return jobs
}

// getLogEventsByJobID returns a thread-safe deep copy of log events grouped by job ID.
func (h *EventHandler) getLogEventsByJobID(clusterSessionKey string) map[string][]types.LogEvent {
	return h.ClusterLogEventMap.GetLogEventsByJobID(clusterSessionKey)
}

// handleTaskDefinitionEvent processes TASK_DEFINITION_EVENT or ACTOR_TASK_DEFINITION_EVENT and preserves the task attempt ordering.
func (h *EventHandler) handleTaskDefinitionEvent(eventMap map[string]any, clusterSessionKey string, isActorTask bool) error {
	var taskDefField string
	if isActorTask {
		taskDefField = "actorTaskDefinitionEvent"
	} else {
		taskDefField = "taskDefinitionEvent"
	}

	taskDef, ok := eventMap[taskDefField]
	if !ok {
		return fmt.Errorf("event does not have '%s' field", taskDefField)
	}

	jsonTaskDefinition, err := json.Marshal(taskDef)
	if err != nil {
		return fmt.Errorf("failed to marshal %s event: %w", taskDefField, err)
	}

	var currTask types.Task
	if err := json.Unmarshal(jsonTaskDefinition, &currTask); err != nil {
		return fmt.Errorf("failed to unmarshal %s event: %w", taskDefField, err)
	}
	if currTask.TaskID == "" {
		return fmt.Errorf("task ID is empty")
	}
	normalizeTaskIDsToHex(&currTask)

	// Manually set the task type for an actor task.
	if isActorTask {
		currTask.TaskType = types.ACTOR_TASK
	}

	taskMap := h.ClusterTaskMap.GetOrCreateTaskMap(clusterSessionKey)
	taskMap.CreateOrMergeAttempt(currTask.TaskID, currTask.TaskAttempt, func(task *types.Task) {
		// Preserve existing lifecycle-derived fields to avoid overwriting.
		existingStateTransitions := task.StateTransitions
		existingRayErrorInfo := task.RayErrorInfo
		existingNodeID := task.NodeID
		existingWorkerID := task.WorkerID
		existingWorkerPID := task.WorkerPID
		existingIsDebuggerPaused := task.IsDebuggerPaused
		existingActorReprName := task.ActorReprName
		existingTaskLogInfo := task.TaskLogInfo

		existingProfileData := task.ProfileData
		existingFuncOrClassName := task.FuncOrClassName
		existingCreationTime := task.CreationTime
		existingStartTime := task.StartTime
		existingEndTime := task.EndTime

		*task = currTask

		if len(existingStateTransitions) > 0 {
			task.StateTransitions = existingStateTransitions
			task.RayErrorInfo = existingRayErrorInfo
			task.NodeID = existingNodeID
			task.WorkerID = existingWorkerID
			task.WorkerPID = existingWorkerPID
			task.IsDebuggerPaused = existingIsDebuggerPaused
			task.ActorReprName = existingActorReprName
			task.State = task.GetLastState()
			task.StartTime = existingStartTime
			task.EndTime = existingEndTime
			task.CreationTime = existingCreationTime
		}
		if existingTaskLogInfo != nil {
			task.TaskLogInfo = existingTaskLogInfo
		}

		// Restore profile-derived fields (from TASK_PROFILE_EVENT)
		// All three come from the same event, so check together
		if existingProfileData != nil {
			task.ProfileData = existingProfileData
			if existingFuncOrClassName != "" {
				task.FuncOrClassName = existingFuncOrClassName
			}
		}
	})

	return nil
}

// mergeTaskLogInfo combines Ray's partial log-start and log-end updates.
func mergeTaskLogInfo(current **types.TaskLogInfo, update *types.TaskLogInfo) {
	if update == nil {
		return
	}
	if *current == nil {
		copied := *update
		*current = &copied
		return
	}

	mergeTaskLogStream(
		&(*current).StdoutFile,
		&(*current).StdoutStart,
		&(*current).StdoutEnd,
		update.StdoutFile,
		update.StdoutStart,
		update.StdoutEnd,
	)
	mergeTaskLogStream(
		&(*current).StderrFile,
		&(*current).StderrStart,
		&(*current).StderrEnd,
		update.StderrFile,
		update.StderrStart,
		update.StderrEnd,
	)
}

func mergeTaskLogStream(
	currentFile *string,
	currentStart, currentEnd *int64,
	updateFile string,
	updateStart, updateEnd int64,
) {
	if updateFile != "" {
		*currentFile = updateFile
		*currentStart = updateStart
	} else if updateStart != 0 {
		*currentStart = updateStart
	}
	if updateEnd != 0 || *currentEnd == 0 {
		*currentEnd = updateEnd
	}
}

// handleTaskLifecycleEvent processes TASK_LIFECYCLE_EVENT and merges state transitions for a given task attempt.
func (h *EventHandler) handleTaskLifecycleEvent(eventMap map[string]any, clusterSessionKey string) error {
	taskLifecycle, ok := eventMap["taskLifecycleEvent"]
	if !ok {
		return fmt.Errorf("event does not have 'taskLifecycleEvent' field")
	}

	jsonTaskLifecycle, err := json.Marshal(taskLifecycle)
	if err != nil {
		return fmt.Errorf("failed to marshal task lifecycle event: %w", err)
	}

	var currTask types.Task
	if err := json.Unmarshal(jsonTaskLifecycle, &currTask); err != nil {
		return fmt.Errorf("failed to unmarshal task lifecycle event: %w", err)
	}
	if currTask.TaskID == "" {
		return fmt.Errorf("task ID is empty")
	}
	normalizeTaskIDsToHex(&currTask)

	if len(currTask.StateTransitions) == 0 && currTask.TaskLogInfo == nil {
		return fmt.Errorf("TASK_LIFECYCLE_EVENT must have at least one state transition or task log info")
	}

	taskMap := h.ClusterTaskMap.GetOrCreateTaskMap(clusterSessionKey)
	taskMap.CreateOrMergeAttempt(currTask.TaskID, currTask.TaskAttempt, func(task *types.Task) {
		// --- DEDUPLICATION using (State + Timestamp) as unique key ---
		// Build a set of existing event keys to detect duplicates
		type eventKey struct {
			State     string
			Timestamp int64
		}
		existingKeys := make(map[eventKey]bool)
		for _, e := range task.StateTransitions {
			existingKeys[eventKey{string(e.State), e.Timestamp.UnixNano()}] = true
		}

		// Only append events that haven't been seen before
		for _, e := range currTask.StateTransitions {
			key := eventKey{string(e.State), e.Timestamp.UnixNano()}
			if !existingKeys[key] {
				task.StateTransitions = append(task.StateTransitions, e)
				existingKeys[key] = true
			}
		}

		// Sort events by timestamp to ensure correct order
		sort.Slice(task.StateTransitions, func(i, j int) bool {
			return task.StateTransitions[i].Timestamp.Before(task.StateTransitions[j].Timestamp)
		})

		if currTask.JobID != "" {
			task.JobID = currTask.JobID
		}
		mergeTaskLogInfo(&task.TaskLogInfo, currTask.TaskLogInfo)

		if len(currTask.StateTransitions) == 0 {
			return
		}

		// TODO(jiangjiawei1103): Before beta, the lifecycle-related fields are overwritten.
		// In beta, the complete historical replay will be supported.
		task.RayErrorInfo = currTask.RayErrorInfo
		if currTask.NodeID != "" {
			task.NodeID = currTask.NodeID
		}
		if currTask.WorkerID != "" {
			task.WorkerID = currTask.WorkerID
		}
		if currTask.WorkerPID != 0 {
			task.WorkerPID = currTask.WorkerPID
		}
		task.IsDebuggerPaused = currTask.IsDebuggerPaused
		task.ActorReprName = currTask.ActorReprName
		task.State = task.GetLastState()

		// Derive creation time, start time and end time from state transitions.
		// Ref: https://github.com/ray-project/ray/blob/d0b1d151d8ea964a711e451d0ae736f8bf95b629/python/ray/util/state/common.py#L1660-L1685
		for _, tr := range task.StateTransitions {
			switch tr.State {
			case types.PENDING_ARGS_AVAIL:
				if task.CreationTime.IsZero() {
					task.CreationTime = tr.Timestamp
				}
			case types.RUNNING:
				if task.StartTime.IsZero() {
					task.StartTime = tr.Timestamp
				}
			case types.FINISHED, types.FAILED:
				// Take the latest timestamp as the end time.
				task.EndTime = tr.Timestamp
			}
		}
	})

	return nil
}

// handleNodeDefinitionEvent processes NODE_DEFINITION_EVENT and merges it with the existing node map for a given cluster session.
func (h *EventHandler) handleNodeDefinitionEvent(eventMap map[string]any, clusterSessionID string) error {
	nodeDef, exists := eventMap["nodeDefinitionEvent"]
	if !exists {
		return fmt.Errorf("event does not have 'nodeDefinitionEvent' field")
	}

	jsonNodeDefinition, err := json.Marshal(nodeDef)
	if err != nil {
		return fmt.Errorf("failed to marshal node definition event: %w", err)
	}

	var currNode types.Node
	if err := json.Unmarshal(jsonNodeDefinition, &currNode); err != nil {
		return fmt.Errorf("failed to unmarshal node definition event: %w", err)
	}
	if currNode.NodeID == "" {
		return fmt.Errorf("node ID is empty")
	}

	currNode.NodeID, err = utils.ConvertBase64ToHex(currNode.NodeID)
	if err != nil {
		logrus.Errorf("Failed to convert node ID from base64 to hex, will keep node ID in base64: %v", err)
	}

	nodeMap := h.ClusterNodeMap.GetOrCreateNodeMap(clusterSessionID)
	nodeMap.CreateOrMergeNode(currNode.NodeID, func(node *types.Node) {
		existingStateTransitions := node.StateTransitions

		*node = currNode

		if len(existingStateTransitions) > 0 {
			node.StateTransitions = existingStateTransitions
		}
	})

	return nil
}

// handleNodeLifecycleEvent processes NODE_LIFECYCLE_EVENT and merges state transitions.
func (h *EventHandler) handleNodeLifecycleEvent(eventMap map[string]any, clusterSessionID string) error {
	nodeLifecycle, exists := eventMap["nodeLifecycleEvent"]
	if !exists {
		return fmt.Errorf("event does not have 'nodeLifecycleEvent' field")
	}

	jsonNodeLifecycle, err := json.Marshal(nodeLifecycle)
	if err != nil {
		return fmt.Errorf("failed to marshal node lifecycle event: %w", err)
	}

	var currNode types.Node
	if err := json.Unmarshal(jsonNodeLifecycle, &currNode); err != nil {
		return fmt.Errorf("failed to unmarshal node lifecycle event: %w", err)
	}
	if currNode.NodeID == "" {
		return fmt.Errorf("node ID is empty")
	}

	currNode.NodeID, err = utils.ConvertBase64ToHex(currNode.NodeID)
	if err != nil {
		logrus.Errorf("Failed to convert node ID from base64 to hex, will keep node ID in base64: %v", err)
	}

	// A NODE_LIFECYCLE_EVENT must have at least one state transition.
	// Ref: https://github.com/ray-project/ray/blob/221a19395a836fada12ebb2bac7bff00a666faa5/src/ray/protobuf/public/events_node_lifecycle_event.proto#L58-L61.
	if len(currNode.StateTransitions) == 0 {
		return fmt.Errorf("node lifecycle event does not have any state transitions")
	}

	nodeMap := h.ClusterNodeMap.GetOrCreateNodeMap(clusterSessionID)
	nodeMap.CreateOrMergeNode(currNode.NodeID, func(n *types.Node) {
		// Merge state transitions.
		n.StateTransitions = MergeStateTransitions[types.NodeStateTransition](
			n.StateTransitions,
			currNode.StateTransitions,
		)
	})

	return nil
}

func (h *EventHandler) handleTaskProfileEvent(eventMap map[string]any, clusterSessionKey string) error {
	taskProfileEvent, ok := eventMap["taskProfileEvents"]
	if !ok {
		return fmt.Errorf("event does not have 'taskProfileEvents'")
	}
	jsonBytes, err := json.Marshal(taskProfileEvent)
	if err != nil {
		return err
	}

	var profileData types.TaskProfileEvents
	if err := json.Unmarshal(jsonBytes, &profileData); err != nil {
		logrus.Errorf("Failed to unmarshal TASK_PROFILE_EVENT: %v", err)
		return err
	}

	if profileData.TaskID == "" || len(profileData.ProfileEvents.Events) == 0 {
		logrus.Debugf("TASK_PROFILE_EVENT has no taskId or events, skipping")
		return nil
	}

	// Convert events to ProfileEventRaw format
	rawEvents := make([]types.ProfileEventRaw, 0, len(profileData.ProfileEvents.Events))
	for _, e := range profileData.ProfileEvents.Events {
		startTime, err := strconv.ParseInt(e.StartTime, 10, 64)
		if err != nil {
			logrus.Warnf("Failed to parse StartTime '%s': %v", e.StartTime, err)
			continue
		}
		endTime, err := strconv.ParseInt(e.EndTime, 10, 64)
		if err != nil {
			logrus.Warnf("Failed to parse EndTime '%s': %v", e.EndTime, err)
			continue
		}

		rawEvents = append(rawEvents, types.ProfileEventRaw{
			EventName: e.EventName,
			StartTime: startTime,
			EndTime:   endTime,
			ExtraData: e.ExtraData,
		})
	}
	// Convert IDs from base64 to hex before merge (consistent with JOB_DEFINITION_EVENT pattern)
	profileData.TaskID, err = utils.ConvertBase64ToHex(profileData.TaskID)
	if err != nil {
		logrus.Errorf("Failed to convert TaskID from base64 to Hex, will use base64: %v", err)
	}
	profileData.JobID, err = utils.ConvertBase64ToHex(profileData.JobID)
	if err != nil {
		logrus.Errorf("Failed to convert JobID from base64 to Hex, will use base64: %v", err)
	}
	profileData.ProfileEvents.ComponentID, err = utils.ConvertBase64ToHex(profileData.ProfileEvents.ComponentID)
	if err != nil {
		logrus.Errorf("Failed to convert ComponentID from base64 to Hex, will use base64: %v", err)
	}

	taskMap := h.ClusterTaskMap.GetOrCreateTaskMap(clusterSessionKey)
	taskMap.CreateOrMergeAttempt(profileData.TaskID, profileData.AttemptNumber, func(t *types.Task) {
		// Ensure core identifiers are set
		if t.TaskID == "" {
			t.TaskID = profileData.TaskID
		}
		if t.JobID == "" {
			t.JobID = profileData.JobID
		}

		// Set AttemptNumber to match the attempt we're merging into
		t.TaskAttempt = profileData.AttemptNumber

		// Initialize ProfileData if not exists
		if t.ProfileData == nil {
			t.ProfileData = &types.ProfileData{
				ComponentID:   profileData.ProfileEvents.ComponentID,
				ComponentType: profileData.ProfileEvents.ComponentType,
				NodeIPAddress: profileData.ProfileEvents.NodeIPAddress,
			}
		}

		// Merge events with deduplication based on (eventName, startTime, endTime)
		type eventKey struct {
			EventName string
			StartTime int64
			EndTime   int64
		}
		existingKeys := make(map[eventKey]struct{}, len(t.ProfileData.Events)+len(rawEvents))
		for _, e := range t.ProfileData.Events {
			existingKeys[eventKey{e.EventName, e.StartTime, e.EndTime}] = struct{}{}
		}
		for _, e := range rawEvents {
			key := eventKey{e.EventName, e.StartTime, e.EndTime}
			if _, ok := existingKeys[key]; !ok {
				t.ProfileData.Events = append(t.ProfileData.Events, e)
				existingKeys[key] = struct{}{}
			}
		}

		// Extract func_or_class_name from extraData if available
		for _, e := range rawEvents {
			if strings.HasPrefix(e.EventName, taskPrefix) && e.ExtraData != "" {
				var extra map[string]any
				if err := json.Unmarshal([]byte(e.ExtraData), &extra); err == nil {
					if name, ok := extra["name"].(string); ok && name != "" {
						// For actor methods, name might be just "increment" or "get_count"
						// But eventName has the full form like "task::Counter.increment"
						// Use eventName to get the full func_or_class_name
						t.FuncOrClassName = strings.TrimPrefix(e.EventName, taskPrefix)
					}
				}
			}
		}
	})
	return nil
}

// normalizeIDToHex converts a single base64-encoded Ray ID to hex
func normalizeIDToHex(base64ID string) string {
	if base64ID == "" {
		return ""
	}

	hexID, err := utils.ConvertBase64ToHex(base64ID)
	if err != nil {
		logrus.Errorf("Failed to convert ID from base64 to hex, keeping original: %v", err)
		return base64ID
	}
	return hexID
}

// normalizeTaskIDsToHex converts base64-encoded Ray IDs in task-related events:
//   - TASK_DEFINITION_EVENT
//   - ACTOR_TASK_DEFINITION_EVENT
//   - TASK_LIFECYCLE_EVENT
//
// to hex so stored tasks match the live cluster API schema.
func normalizeTaskIDsToHex(task *types.Task) {
	task.TaskID = normalizeIDToHex(task.TaskID)
	task.ActorID = normalizeIDToHex(task.ActorID)
	task.JobID = normalizeIDToHex(task.JobID)
	task.ParentTaskID = normalizeIDToHex(task.ParentTaskID)
	task.PlacementGroupID = normalizeIDToHex(task.PlacementGroupID)
	task.NodeID = normalizeIDToHex(task.NodeID)
	task.WorkerID = normalizeIDToHex(task.WorkerID)
}

// normalizeActorIDsToHex converts base64-encoded Ray IDs in actor events:
//   - ACTOR_DEFINITION_EVENT
//
// to hex so stored actors match the live cluster API schema.
// Note: ACTOR_LIFECYCLE_EVENT IDs are normalized inline at parse time.
func normalizeActorIDsToHex(actor *types.Actor) {
	actor.ActorID = normalizeIDToHex(actor.ActorID)
	actor.JobID = normalizeIDToHex(actor.JobID)
	actor.PlacementGroupID = normalizeIDToHex(actor.PlacementGroupID)
	actor.Address.NodeID = normalizeIDToHex(actor.Address.NodeID)
	actor.Address.WorkerID = normalizeIDToHex(actor.Address.WorkerID)
}

// getNodeMap returns a thread-safe deep copy of all nodes for a given cluster session.
func (h *EventHandler) getNodeMap(clusterSessionID string) map[string]types.Node {
	h.ClusterNodeMap.RLock()
	defer h.ClusterNodeMap.RUnlock()

	nodeMap, ok := h.ClusterNodeMap.ClusterNodeMap[clusterSessionID]
	if !ok {
		return map[string]types.Node{}
	}

	nodeMap.Lock()
	defer nodeMap.Unlock()

	nodes := make(map[string]types.Node, len(nodeMap.NodeMap))
	for id, node := range nodeMap.NodeMap {
		nodes[id] = node.DeepCopy()
	}
	return nodes
}

// ProcessSingleSession reads all event files for a single session synchronously
// and populates the handler's in-memory maps.
//
// TODO(jiangjiawei1103): Empty event file list vs ListFiles outage is ambiguous without
// StorageReader interface surfacing errors.
func (h *EventHandler) ProcessSingleSession(ctx context.Context, clusterInfo utils.ClusterInfo) error {
	clusterLogPathPrefix := h.getClusterLogPathPrefix(clusterInfo)
	clusterSessionKey := utils.BuildClusterSessionKey(clusterInfo.Name, clusterInfo.Namespace, clusterInfo.SessionName)

	// ClusterLogEventMap backs only the /events endpoint, so log event read failures must not
	// block marking the session as loaded and force subsequent Ray event re-processing.
	logEventReader := NewLogEventReader(h.reader)
	if err := logEventReader.ReadLogEvents(clusterInfo, clusterSessionKey, h.ClusterLogEventMap); err != nil {
		logrus.Errorf("Incomplete Log Events read for %s: %v. /events endpoint may serve partial data.",
			clusterSessionKey, err)
	}

	eventFileList := append(h.getAllJobEventFiles(clusterInfo), h.getAllNodeEventFiles(clusterInfo)...)
	logrus.Debugf("current eventFileList for cluster %s is: %v", clusterInfo.Name, eventFileList)

	rayEventsTotal := len(eventFileList)
	rayEventsRead := 0
	for _, eventFile := range eventFileList {
		if err := ctx.Err(); err != nil {
			return err
		}
		logrus.Debugf("Reading event file: %s", eventFile)

		var eventioReader io.Reader
		if strings.HasSuffix(eventFile, ".gz") {
			rc, err := compression.ReadCompressedContent(h.reader, clusterLogPathPrefix, eventFile)
			if err != nil {
				logrus.Errorf("Failed to decompress event file %s: %v", eventFile, err)
				continue
			}
			eventioReader = rc
		} else {
			eventioReader = h.reader.GetContent(clusterLogPathPrefix, eventFile)
		}

		if eventioReader == nil {
			logrus.Errorf("Failed to get content for event file: %s, skipping", eventFile)
			continue
		}

		eventbytes, err := io.ReadAll(eventioReader)
		if closer, ok := eventioReader.(io.Closer); ok {
			closer.Close()
		}

		if err != nil {
			logrus.Errorf("Failed to read events for file %s: %v", eventFile, err)
			continue
		}
		rayEventsRead++

		// DecodeEventFileBytes and storeEvent failures are treated as corrupt-data errors:
		// retrying won't fix bad bytes, accepting partial loss.
		eventList, err := DecodeEventFileBytes(eventFile, eventbytes)
		if err != nil {
			logrus.Errorf("Failed to decode events for file %s: %v", eventFile, err)
			continue
		}

		for _, event := range eventList {
			if event == nil {
				continue
			}
			if err := h.storeEvent(clusterSessionKey, event); err != nil {
				logrus.Errorf("Failed to store events for file %s: %v", eventFile, err)
				continue
			}
		}
	}

	if rayEventsTotal > 0 && rayEventsRead == 0 {
		return fmt.Errorf("read 0 of %d event files for %s: likely transient storage outage",
			rayEventsTotal, clusterSessionKey)
	}
	return nil
}
