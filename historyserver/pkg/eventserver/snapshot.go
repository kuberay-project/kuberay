package eventserver

import (
	"github.com/ray-project/kuberay/historyserver/pkg/eventserver/types"
	"github.com/ray-project/kuberay/historyserver/pkg/utils"
)

// SessionSnapshot is the in-memory representation of a dead session's processed
// event state. It is cached and shared across requests, so treat it as
// immutable and clone a field before modifying it.
type SessionSnapshot struct {
	SessionKey string `json:"sessionKey"`

	Tasks            []types.Task                `json:"tasks"`
	Actors           map[string]types.Actor      `json:"actors"`
	Jobs             map[string]types.Job        `json:"jobs"`
	Nodes            map[string]types.Node       `json:"nodes"`
	LogEventsByJobID map[string][]types.LogEvent `json:"logEventsByJobId"`
}

// BuildSnapshot builds a SessionSnapshot from the handler's per-session state.
func (h *EventHandler) BuildSnapshot(session utils.ClusterInfo) *SessionSnapshot {
	clusterSessionKey := utils.BuildClusterSessionKey(session.Name, session.Namespace, session.SessionName)
	snapshot := &SessionSnapshot{
		SessionKey:       clusterSessionKey,
		Tasks:            h.getTasks(clusterSessionKey),
		Actors:           h.getActorsMap(clusterSessionKey),
		Jobs:             h.getJobsMap(clusterSessionKey),
		Nodes:            h.getNodeMap(clusterSessionKey),
		LogEventsByJobID: h.getLogEventsByJobID(clusterSessionKey),
	}
	reconcileTasksOfFinishedJobs(snapshot)
	return snapshot
}

// isTerminalTaskState reports whether a task state can no longer change.
func isTerminalTaskState(state types.TaskStatus) bool {
	return state == types.FINISHED || state == types.FAILED
}

// reconcileTasksOfFinishedJobs marks non-terminal tasks of finished jobs as FAILED.
// Snapshots are only built for dead sessions, so a task that never reported a
// terminal state cannot still be running once its job finished; its terminal
// lifecycle event was lost when the cluster was torn down. Ray core applies the
// same semantics at job end in GcsTaskManager::OnJobFinished.
func reconcileTasksOfFinishedJobs(snapshot *SessionSnapshot) {
	for i := range snapshot.Tasks {
		task := &snapshot.Tasks[i]
		if isTerminalTaskState(task.State) {
			continue
		}
		job, ok := snapshot.Jobs[task.JobID]
		if !ok || job.State != types.JOBFINISHED {
			continue
		}
		task.State = types.FAILED
		task.EndTime = job.EndTime
		// Append the FAILED transition so the task's serialized state history
		// (stateTransitions) ends in the same state as the top-level State.
		task.StateTransitions = append(task.StateTransitions, types.TaskStateTransition{
			State:     types.FAILED,
			Timestamp: job.EndTime,
		})
	}
}
