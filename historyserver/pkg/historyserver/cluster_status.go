package historyserver

import (
	"fmt"
	"maps"
	"math"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/ray-project/kuberay/historyserver/pkg/eventserver/types"
)

const (
	timestampDisplayFormat = "2006-01-02 15:04:05.000000"
	sessionTimestampFormat = "2006-01-02_15-04-05"
	// maxFailuresDisplayed is the maximum number of failed nodes to display,
	// matching Ray's AUTOSCALER_MAX_FAILURES_DISPLAYED constant.
	// Ref: https://github.com/ray-project/ray/blob/d99d5d375c9c4e6533c15edb37d93a3ee9066be4/python/ray/autoscaler/_private/constants.py#L73
	maxFailuresDisplayed = 20
)

// ResourceDemand represents a pending resource request with count
type ResourceDemand struct {
	Resources map[string]float64
	Count     int
}

// FailedNode represents a node that transitioned to DEAD state.
type FailedNode struct {
	NodeID        string
	NodeIPAddress string
	InstanceID    string // Empty until Ray exports it. Ref: https://github.com/ray-project/ray/issues/60129
	NodeType      string // labels["ray.io/node-group"]
	Timestamp     time.Time
}

// ClusterStatusBuilder aggregates data from multiple sources to build cluster status
type ClusterStatusBuilder struct {
	ActiveNodes    map[string]int
	IdleNodes      map[string]int
	TotalResources map[string]float64
	UsedResources  map[string]float64
	FailedNodes    []FailedNode
	PendingDemands []ResourceDemand // Demands from tasks/actors
	Timestamp      time.Time
}

// NewClusterStatusBuilder creates a new builder instance
func NewClusterStatusBuilder() *ClusterStatusBuilder {
	return &ClusterStatusBuilder{
		ActiveNodes:    make(map[string]int),
		IdleNodes:      make(map[string]int),
		TotalResources: make(map[string]float64),
		UsedResources:  make(map[string]float64),
		PendingDemands: []ResourceDemand{},
	}
}

// ParseSessionTimestamp parses the session timestamp from sessionName.
// Expected format: "session_2006-01-02_15-04-05_123456" or "session_2006-01-02_15-04-05_123456_1"
// Returns zero time if parsing fails.
func ParseSessionTimestamp(sessionName string) time.Time {
	rest, ok := strings.CutPrefix(sessionName, "session_")
	if !ok {
		return time.Time{}
	}

	parts := strings.Split(rest, "_")
	if len(parts) < 2 {
		return time.Time{}
	}

	tsStr := parts[0] + "_" + parts[1]
	ts, err := time.Parse(sessionTimestampFormat, tsStr)
	if err != nil {
		return time.Time{}
	}

	return ts
}

// GetLastTimestamp returns the latest EndTime from tasks and actors.
// This represents when the cluster was last active.
// Returns zero time if no timestamps are available.
func GetLastTimestamp(tasks []types.Task, actors []types.Actor) time.Time {
	var latest time.Time

	for _, task := range tasks {
		if task.EndTime.After(latest) {
			latest = task.EndTime
		}
	}

	for _, actor := range actors {
		if actor.EndTime.After(latest) {
			latest = actor.EndTime
		}
	}

	return latest
}

// AddNodeFromDebugState adds node information from a parsed debug_state.txt
func (b *ClusterStatusBuilder) AddNodeFromDebugState(state *NodeDebugState) {
	if state == nil {
		return
	}

	nodeGroup := state.NodeGroup
	if nodeGroup == "" {
		nodeGroup = "default"
	}

	if state.IsIdle {
		b.IdleNodes[nodeGroup]++
	} else {
		b.ActiveNodes[nodeGroup]++
	}

	for key, value := range state.Total {
		b.TotalResources[key] += value
	}

	usedResources := state.GetUsedResources()
	for key, value := range usedResources {
		b.UsedResources[key] += value
	}
}

// AddFailedNodesFromNodes extracts node failures from node state transitions.
// A node is considered failed if it has a DEAD state transition.
func (b *ClusterStatusBuilder) AddFailedNodesFromNodes(nodes map[string]types.Node) {
	for _, node := range nodes {
		// only take the latest DEAD transition per node
		for i := len(node.StateTransitions) - 1; i >= 0; i-- {
			tr := node.StateTransitions[i]
			if tr.State != types.NODE_DEAD ||
				(tr.DeathInfo != nil && (tr.DeathInfo.Reason == types.EXPECTED_TERMINATION ||
					tr.DeathInfo.Reason == types.AUTOSCALER_DRAIN_IDLE)) {
				continue
			}

			// Ray's autoscaler always has ray_node_type_name because it launches the nodes itself.
			// The history server reads labels from NODE_DEFINITION_EVENT, which may be absent
			// if the user has not enabled this export event. Fall back to NodeID.
			nodeType := node.Labels["ray.io/node-group"]
			if nodeType == "" {
				nodeType = node.NodeID
			}

			failed := FailedNode{
				NodeID:        node.NodeID,
				NodeIPAddress: node.NodeIPAddress,
				InstanceID:    node.InstanceID,
				NodeType:      nodeType,
				Timestamp:     tr.Timestamp,
			}

			b.FailedNodes = append(b.FailedNodes, failed)
			break
		}
	}

	// Sort failed nodes by timestamp in descending order
	slices.SortFunc(b.FailedNodes, func(node1, node2 FailedNode) int {
		return node2.Timestamp.Compare(node1.Timestamp)
	})

	// Cap at maxFailuresDisplayed.
	if len(b.FailedNodes) > maxFailuresDisplayed {
		b.FailedNodes = b.FailedNodes[:maxFailuresDisplayed]
	}
}

// AddPendingDemandsFromTasks calculates pending demands from tasks
func (b *ClusterStatusBuilder) AddPendingDemandsFromTasks(tasks []types.Task) {
	demandMap := make(map[string]*ResourceDemand)

	for _, task := range tasks {
		if !isPendingTaskState(task.State) || len(task.RequiredResources) == 0 {
			continue
		}

		key := resourceKey(task.RequiredResources)
		if key == "" {
			continue
		}

		if _, exists := demandMap[key]; !exists {
			demandMap[key] = &ResourceDemand{
				Resources: task.RequiredResources,
				Count:     0,
			}
		}
		demandMap[key].Count++
	}

	b.mergeDemands(demandMap)
}

// AddPendingDemandsFromActors calculates pending demands from actors
func (b *ClusterStatusBuilder) AddPendingDemandsFromActors(actors []types.Actor) {
	demandMap := make(map[string]*ResourceDemand)

	for _, actor := range actors {
		if !isPendingActorState(actor.State) || len(actor.RequiredResources) == 0 {
			continue
		}

		key := resourceKey(actor.RequiredResources)
		if key == "" {
			continue
		}

		if _, exists := demandMap[key]; !exists {
			demandMap[key] = &ResourceDemand{
				Resources: actor.RequiredResources,
				Count:     0,
			}
		}
		demandMap[key].Count++
	}

	b.mergeDemands(demandMap)
}

// mergeDemands merges new demands into PendingDemands, summing counts for demands with the same resource shape.
func (b *ClusterStatusBuilder) mergeDemands(demandMap map[string]*ResourceDemand) {
	indexByKey := make(map[string]int, len(b.PendingDemands))

	for i, existing := range b.PendingDemands {
		indexByKey[resourceKey(existing.Resources)] = i
	}

	for _, demand := range demandMap {
		key := resourceKey(demand.Resources)
		if idx, found := indexByKey[key]; found {
			b.PendingDemands[idx].Count += demand.Count
		} else {
			indexByKey[key] = len(b.PendingDemands)
			b.PendingDemands = append(b.PendingDemands, *demand)
		}
	}
}

// isPendingTaskState returns true if the task state represents an unmet resource demand.
//
// Ray's autoscaler collects pending demands from the scheduler's lease queues
// (SchedulerResourceReporter::FillResourceUsage), not from task states. Since the
// history server does not have access to scheduler queue data, we approximate by
// inferring from task states. Only states where resources have NOT been allocated
// are counted as pending demands.
//
// Task lifecycle timeline:
//
//	PENDING_ARGS_AVAIL -> PENDING_NODE_ASSIGNMENT -> SUBMITTED_TO_WORKER -> RUNNING -> FINISHED
//	       |                    |    |                      |
//	       |                    |    +- PENDING_OBJ_STORE_MEM_AVAIL (sub-state)
//	       |                    |    +- PENDING_ARGS_FETCH          (sub-state)
//	       |                    |
//	       |              in scheduler                  on worker
//	       |              lease queue                   resources allocated
//	       |
//	  not yet submitted
//	  to scheduler
//	  (waiting for deps)
//
// Actor task lifecycle (actor already exists):
//
//	PENDING_ARGS_AVAIL -> SUBMITTED_TO_WORKER -> PENDING_ACTOR_TASK_ARGS_FETCH -> RUNNING
//	                             |                       |
//	                       sent to actor worker     actor already holds resources
//	                       actor's resources         task queued inside actor
//
//	+--------------------------------------------+---------------------+----------------------+-------------------------+
//	|                   State                    | In scheduler queue? | Resources allocated? | Count as pending demand? |
//	+--------------------------------------------+---------------------+----------------------+-------------------------+
//	| PENDING_ARGS_AVAIL                         | No (not submitted)  | No                   | Yes (resources needed)   |
//	| PENDING_NODE_ASSIGNMENT                    | Yes                 | No                   | Yes                      |
//	| PENDING_OBJ_STORE_MEM_AVAIL                | Borderline          | Tentative            | Yes (borderline)         |
//	| PENDING_ARGS_FETCH                         | Borderline          | Tentative            | Yes (borderline)         |
//	| SUBMITTED_TO_WORKER                        | No (granted)        | Yes                  | No                       |
//	| PENDING_ACTOR_TASK_ARGS_FETCH              | No (at actor)       | Yes (actor's)        | No                       |
//	| PENDING_ACTOR_TASK_ORDERING_OR_CONCURRENCY | No (at actor)       | Yes (actor's)        | No                       |
//	+--------------------------------------------+---------------------+----------------------+-------------------------+
func isPendingTaskState(state types.TaskStatus) bool {
	switch state {
	case types.PENDING_ARGS_AVAIL,
		types.PENDING_NODE_ASSIGNMENT,
		types.PENDING_OBJ_STORE_MEM_AVAIL,
		types.PENDING_ARGS_FETCH:
		return true
	default:
		return false
	}
}

func isPendingActorState(state types.StateType) bool {
	switch state {
	case types.PENDING_CREATION, types.DEPENDENCIES_UNREADY:
		return true
	default:
		return false
	}
}

// resourceKey returns a stable, order-independent signature for resource maps
// so equivalent demands can be grouped together.
func resourceKey(resources map[string]float64) string {
	if len(resources) == 0 {
		return ""
	}

	var parts []string
	for k, v := range resources {
		// The precision of the fractional resource requirement is 0.0001
		// Ref: https://docs.ray.io/en/latest/ray-core/scheduling/resources.html
		parts = append(parts, fmt.Sprintf("%s:%.4f", k, v))
	}

	slices.Sort(parts)

	return strings.Join(parts, ",")
}

func sortedKeys[V any](m map[string]V) []string {
	return slices.Sorted(maps.Keys(m))
}

// formatResourceValue formats a single resource value for display,
// matching Python's default float formatting behavior used in Ray's autoscaler.
// Memory resources are formatted as human-readable bytes (e.g. "10.0 GiB"),
// while others use minimum-digits numeric format (e.g. "1.0", "0.25", "0.1").
// Ref: https://github.com/ray-project/ray/blob/d99d5d375c9c4e6533c15edb37d93a3ee9066be4/python/ray/autoscaler/_private/util.py#L741
func formatResourceValue(resource string, value float64) string {
	// e.g. memory, object_store_memory
	if strings.Contains(strings.ToLower(resource), "memory") {
		return formatMemory(value)
	}

	return formatPythonFloat(value)
}

// formatResourceMapForDisplay formats a resource map matching Python's dict repr output,
// which is what Ray's format_resource_demand_summary uses: f" {dict(bundle)}: {count}+ ..."
// e.g. "{'CPU': 1.0, 'memory': 1073741824.0}"
// Ref: https://github.com/ray-project/ray/blob/d99d5d375c9c4e6533c15edb37d93a3ee9066be4/python/ray/autoscaler/_private/util.py#L776
func formatResourceMapForDisplay(resources map[string]float64) string {
	if len(resources) == 0 {
		return "{}"
	}

	var parts []string
	for _, k := range sortedKeys(resources) {
		parts = append(parts, fmt.Sprintf("'%s': %s", k, formatPythonFloat(resources[k])))
	}

	return fmt.Sprintf("{%s}", strings.Join(parts, ", "))
}

// formatPythonFloat formats a float64 matching Python's default float repr behavior.
// Python: f"{1.0}" → "1.0", f"{0.1}" → "0.1", f"{1073741824.0}" → "1073741824.0"
func formatPythonFloat(value float64) string {
	if math.Trunc(value) == value {
		return fmt.Sprintf("%.1f", value)
	}
	return strconv.FormatFloat(value, 'f', -1, 64)
}

// FormatStatus formats the cluster status as a string matching Ray's format
// Ref: https://github.com/ray-project/ray/blob/d99d5d375c9c4e6533c15edb37d93a3ee9066be4/python/ray/autoscaler/_private/util.py#L790
func (b *ClusterStatusBuilder) FormatStatus() string {
	var sb strings.Builder

	// Header with timestamp, separator length matches header length.
	// Ref: https://github.com/ray-project/ray/blob/d99d5d375c9c4e6533c15edb37d93a3ee9066be4/python/ray/autoscaler/_private/util.py#L801
	timestampStr := b.Timestamp.Format(timestampDisplayFormat)
	if b.Timestamp.IsZero() {
		timestampStr = "time unknown"
	}
	header := fmt.Sprintf("======== Autoscaler status: %s ========", timestampStr)
	separator := strings.Repeat("-", len(header))
	sb.WriteString(header + "\n")

	// Node status section
	sb.WriteString("Node status\n")
	sb.WriteString(separator + "\n")

	// Active nodes
	sb.WriteString("Active:\n")
	if len(b.ActiveNodes) == 0 {
		sb.WriteString(" (no active nodes)\n")
	} else {
		for _, group := range sortedKeys(b.ActiveNodes) {
			fmt.Fprintf(&sb, " %d %s\n", b.ActiveNodes[group], group)
		}
	}

	// Idle nodes
	sb.WriteString("Idle:\n")
	if len(b.IdleNodes) == 0 {
		sb.WriteString(" (no idle nodes)\n")
	} else {
		for _, group := range sortedKeys(b.IdleNodes) {
			fmt.Fprintf(&sb, " %d %s\n", b.IdleNodes[group], group)
		}
	}

	// Pending nodes (not available from debug_state.txt)
	sb.WriteString("Pending:\n")
	// TODO Reconstruct pending nodes when autoscaler events are exported by Ray.
	// Ref: https://github.com/ray-project/ray/issues/60129#issuecomment-3940436320
	sb.WriteString(" (unavailable in history server)\n")

	// Recent failures
	sb.WriteString("Recent failures:\n")
	if len(b.FailedNodes) == 0 {
		sb.WriteString(" (no failures)\n")
	} else {
		for _, node := range b.FailedNodes {
			// Use instance_id when available (autoscaler v2 format), fall back to ip (v1 format).
			if node.InstanceID != "" {
				fmt.Fprintf(&sb, " %s: NodeTerminated (instance_id: %s)\n", node.NodeType, node.InstanceID)
			} else {
				fmt.Fprintf(&sb, " %s: NodeTerminated (ip: %s)\n", node.NodeType, node.NodeIPAddress)
			}
		}
	}

	sb.WriteString("\n")

	// Resources section
	sb.WriteString("Resources\n")
	sb.WriteString(separator + "\n")

	// Total Usage
	sb.WriteString("Total Usage:\n")
	if len(b.TotalResources) == 0 {
		sb.WriteString(" (no resources)\n")
	} else {
		for _, key := range sortedKeys(b.TotalResources) {
			// Skip accelerator_type resources, matching Ray's non-verbose behavior.
			// Ref: https://github.com/ray-project/ray/blob/d99d5d375c9c4e6533c15edb37d93a3ee9066be4/python/ray/autoscaler/_private/util.py#L760
			if strings.HasPrefix(key, "accelerator_type:") {
				continue
			}
			total := b.TotalResources[key]
			used := b.UsedResources[key]
			fmt.Fprintf(&sb, " %s/%s %s\n",
				formatResourceValue(key, used),
				formatResourceValue(key, total),
				key)
		}
	}

	sb.WriteString("\n")

	sb.WriteString("From request_resources:\n")
	// TODO Reconstruct request_resources when autoscaler events are exported by Ray.
	// Ref: https://github.com/ray-project/ray/issues/60129#issuecomment-3940436320
	sb.WriteString(" (unavailable in history server)\n")

	// Pending Demands
	sb.WriteString("Pending Demands:\n")
	if len(b.PendingDemands) == 0 {
		sb.WriteString(" (no resource demands)")
	} else {
		for _, demand := range b.PendingDemands {
			// Ref: https://github.com/ray-project/ray/blob/d99d5d375c9c4e6533c15edb37d93a3ee9066be4/python/ray/autoscaler/_private/util.py#L776 (get_demand_report)
			// Ref: https://github.com/ray-project/ray/blob/d99d5d375c9c4e6533c15edb37d93a3ee9066be4/python/ray/autoscaler/v2/utils.py#L635 (ClusterStatusFormatter._demand_report)
			fmt.Fprintf(&sb, " %s: %d+ pending tasks/actors\n",
				formatResourceMapForDisplay(demand.Resources), demand.Count)
		}
	}

	return sb.String()
}
