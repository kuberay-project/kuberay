package types

import (
	"maps"
	"sync"
	"time"
)

// NodeState is the state of a node.
type NodeState string

const (
	NODE_ALIVE NodeState = "ALIVE"
	NODE_DEAD  NodeState = "DEAD"
)

// NodeDeathInfoReason specifies the reason why a node died.
type NodeDeathInfoReason string

const (
	EXPECTED_TERMINATION       NodeDeathInfoReason = "EXPECTED_TERMINATION"
	UNEXPECTED_TERMINATION     NodeDeathInfoReason = "UNEXPECTED_TERMINATION"
	AUTOSCALER_DRAIN_PREEMPTED NodeDeathInfoReason = "AUTOSCALER_DRAIN_PREEMPTED"
	AUTOSCALER_DRAIN_IDLE      NodeDeathInfoReason = "AUTOSCALER_DRAIN_IDLE"
)

type NodeDeathInfo struct {
	Reason        NodeDeathInfoReason `json:"reason"`
	ReasonMessage string              `json:"reasonMessage"`
}

// NodeStateTransition represents a change in a node's state at a specific timestamp.
type NodeStateTransition struct {
	// State of a node (ALIVE, DEAD).
	State NodeState `json:"state"`

	// Timestamp when the state transition occurred.
	Timestamp time.Time `json:"timestamp"`

	// Resources available on a node (cpu, gpu, etc.), available only in the ALIVE state.
	Resources map[string]float64 `json:"resources,omitempty"`

	// Reason why a node died (EXPECTED_TERMINATION, UNEXPECTED_TERMINATION, AUTOSCALER_DRAIN_PREEMPTED, AUTOSCALER_DRAIN_IDLE),
	// available only in the DEAD state.
	DeathInfo *NodeDeathInfo `json:"deathInfo,omitempty"`
}

// GetState returns the state of the node.
func (n NodeStateTransition) GetState() string {
	return string(n.State)
}

// GetTimestamp returns the timestamp of the node state transition.
func (n NodeStateTransition) GetTimestamp() time.Time {
	return n.Timestamp
}

// Node represents a Ray node in a cluster session. The fields are populated from both
// the NODE_DEFINITION_EVENT and the NODE_LIFECYCLE_EVENT.
type Node struct {
	// NodeID is the hexadecimal representation of the node ID.
	NodeID         string            `json:"nodeId"`
	NodeIPAddress  string            `json:"nodeIpAddress"`
	StartTimestamp time.Time         `json:"startTimestamp"`
	Labels         map[string]string `json:"labels"`

	// Wait for Ray to export the following fields.
	// Ref: https://github.com/ray-project/ray/issues/60129
	Hostname         string `json:"hostname,omitempty"`
	NodeName         string `json:"nodeName,omitempty"`
	InstanceID       string `json:"instanceId,omitempty"`
	InstanceTypeName string `json:"instanceTypeName,omitempty"`

	// StateTransitions contains the history of node state changes.
	// This field is populated only when at least one NODE_LIFECYCLE_EVENT has been processed.
	// Each transition includes the state (ALIVE/DEAD), timestamp, and state-specific data
	// (Resources for ALIVE, DeathInfo for DEAD).
	StateTransitions []NodeStateTransition `json:"stateTransitions,omitempty"`
}

type NodeMap struct {
	NodeMap map[string]Node
	Mu      sync.Mutex
}

func (n *NodeMap) Lock() {
	n.Mu.Lock()
}

func (n *NodeMap) Unlock() {
	n.Mu.Unlock()
}

func NewNodeMap() *NodeMap {
	return &NodeMap{
		NodeMap: make(map[string]Node),
	}
}

type ClusterNodeMap struct {
	// ClusterNodeMap is a map of cluster session ID to NodeMap.
	ClusterNodeMap map[string]*NodeMap
	Mu             sync.RWMutex
}

func (c *ClusterNodeMap) RLock() {
	c.Mu.RLock()
}

func (c *ClusterNodeMap) RUnlock() {
	c.Mu.RUnlock()
}

func (c *ClusterNodeMap) Lock() {
	c.Mu.Lock()
}

func (c *ClusterNodeMap) Unlock() {
	c.Mu.Unlock()
}

// GetOrCreateNodeMap retrieves the NodeMap for the given cluster session, creating it if it doesn't exist.
func (c *ClusterNodeMap) GetOrCreateNodeMap(clusterSessionID string) *NodeMap {
	c.Lock()
	defer c.Unlock()

	nodeMap, exists := c.ClusterNodeMap[clusterSessionID]
	if !exists {
		nodeMap = NewNodeMap()
		c.ClusterNodeMap[clusterSessionID] = nodeMap
	}
	return nodeMap
}

// CreateOrMergeNode retrieves or creates a Node and applies the merge function.
func (n *NodeMap) CreateOrMergeNode(nodeId string, mergeFn func(*Node)) {
	n.Lock()
	defer n.Unlock()

	node, exists := n.NodeMap[nodeId]
	if !exists {
		node = Node{NodeID: nodeId}
	}

	mergeFn(&node)
	n.NodeMap[nodeId] = node
}

// DeepCopy returns a deep copy of the Node, which prevents race conditions.
func (n Node) DeepCopy() Node {
	cp := n

	if len(n.Labels) > 0 {
		cp.Labels = make(map[string]string, len(n.Labels))
		maps.Copy(cp.Labels, n.Labels)
	}

	if len(n.StateTransitions) > 0 {
		cp.StateTransitions = make([]NodeStateTransition, len(n.StateTransitions))
		for i, tr := range n.StateTransitions {
			cp.StateTransitions[i] = tr

			if len(tr.Resources) > 0 {
				cp.StateTransitions[i].Resources = make(map[string]float64, len(tr.Resources))
				maps.Copy(cp.StateTransitions[i].Resources, tr.Resources)
			}

			if tr.DeathInfo != nil {
				cp.StateTransitions[i].DeathInfo = &NodeDeathInfo{
					Reason:        tr.DeathInfo.Reason,
					ReasonMessage: tr.DeathInfo.ReasonMessage,
				}
			}
		}
	}

	return cp
}
