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
	return &SessionSnapshot{
		SessionKey:       clusterSessionKey,
		Tasks:            h.getTasks(clusterSessionKey),
		Actors:           h.getActorsMap(clusterSessionKey),
		Jobs:             h.getJobsMap(clusterSessionKey),
		Nodes:            h.getNodeMap(clusterSessionKey),
		LogEventsByJobID: h.getLogEventsByJobID(clusterSessionKey),
	}
}
