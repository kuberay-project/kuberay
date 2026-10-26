package historyserver

import (
	"bufio"
	"io"
	"regexp"
	"strconv"
	"strings"

	"github.com/sirupsen/logrus"
)

// NodeDebugState represents parsed data from a node's debug_state.txt file.
//
// The debug_state.txt is written by Ray's Raylet process. The resource data line is produced by:
//
//	ClusterResourceScheduler::DebugString()
//	  → LocalResourceManager::DebugString()
//	    → NodeResourceInstanceSet::DebugString()  — outputs "total":{...}, "available":{...}
//	    → appends " is_draining: 0/1 is_idle: 0/1"
//	  → ClusterResourceManager::DebugString()     — outputs Cluster resources (same format)
//
// Ref: https://github.com/ray-project/ray/blob/d99d5d375c9c4e6533c15edb37d93a3ee9066be4/src/ray/raylet/scheduling/cluster_resource_scheduler.cc#L262-L268
// Ref: https://github.com/ray-project/ray/blob/d99d5d375c9c4e6533c15edb37d93a3ee9066be4/src/ray/raylet/scheduling/local_resource_manager.cc#L78-L83
type NodeDebugState struct {
	NodeID    string
	NodeName  string
	NodeGroup string
	IsIdle    bool
	Total     map[string]float64
	Available map[string]float64
}

var (
	// Extracts the node group label from "ray.io/node-group":"<group name>"
	reNodeGroup = regexp.MustCompile(`"ray\.io/node-group"\s*:\s*"([^"]+)"`)
	// Matches the first "total": {...} on the line, which is the Local resources section.
	// FindStringSubmatch returns only the first match, so this captures Local resources
	// (which appears before Cluster resources on the same line).
	// Ref: https://github.com/ray-project/ray/blob/d99d5d375c9c4e6533c15edb37d93a3ee9066be4/src/ray/common/scheduling/resource_instance_set.cc#L447-L460
	reTotal = regexp.MustCompile(`"total"\s*:\s*\{([^}]+)\}`)
	// Matches the first "available": {...} on the line, which is the Local resources section.
	reAvailable = regexp.MustCompile(`"available"\s*:\s*\{([^}]+)\}`)
	// Matches resource key-value pairs like "memory: [100000000000000]", "CPU: [10000]",
	// or multi-instance resources like "CPU: [10000, 10000, 10000, 10000]".
	// Values are wrapped in brackets because Ray's FixedPointVectorToString outputs "[i_, i_, ...]"
	// where each i_ is a raw FixedPoint int64 (value × 10000). Each element represents one
	// resource instance (e.g., one CPU core). Multi-instance values are summed in parseResourceMap.
	// Ref: https://github.com/ray-project/ray/blob/d99d5d375c9c4e6533c15edb37d93a3ee9066be4/src/ray/common/scheduling/fixed_point.cc#L37-L47
	// Ref: https://github.com/ray-project/ray/blob/d99d5d375c9c4e6533c15edb37d93a3ee9066be4/src/ray/common/scheduling/fixed_point.h#L113-L115
	reResourceKV = regexp.MustCompile(`([a-zA-Z0-9_:./\-]+):\s*\[([0-9,\s]+)\]`)
)

// rayResourceScale is the fixed-point scaling factor used by Ray internally.
// Ray stores resource values as int64 = actual_value × 10000 for fixed-point arithmetic.
// Ref: https://github.com/ray-project/ray/blob/d99d5d375c9c4e6533c15edb37d93a3ee9066be4/src/ray/common/constants.h#L23
const rayResourceScale = 10000.0

// ParseDebugState parses the content of a debug_state.txt file
// and extracts cluster resource scheduler state information.
// It rebuilds the Resources section exactly, and Idle section most of the time
func ParseDebugState(rd io.Reader) (*NodeDebugState, error) {
	state := &NodeDebugState{
		Total:     make(map[string]float64),
		Available: make(map[string]float64),
	}

	reader := bufio.NewReader(rd)
	for {
		line, err := reader.ReadString('\n')
		if err != nil && err != io.EOF {
			return nil, err
		}

		// Parse Node ID and Node name from the first few lines
		if after, ok := strings.CutPrefix(line, "Node ID:"); ok {
			state.NodeID = strings.TrimSpace(after)
		} else if after, ok := strings.CutPrefix(line, "Node name:"); ok {
			state.NodeName = strings.TrimSpace(after)
		} else if strings.Contains(line, "cluster_resource_scheduler state:") {
			// The actual data is on the next line
			nextLine, nextErr := reader.ReadString('\n')
			if nextErr != nil && nextErr != io.EOF {
				return nil, nextErr
			}
			parseClusterResourceSchedulerLine(nextLine, state)
			break
		}

		if err == io.EOF {
			break
		}
	}

	return state, nil
}

// parseClusterResourceSchedulerLine parses the complex line containing resource info
func parseClusterResourceSchedulerLine(line string, state *NodeDebugState) {
	// Extract is_idle
	state.IsIdle = strings.Contains(line, "is_idle: 1")

	// Extract node group name from "ray.io/node-group":"<group_name>"
	if m := reNodeGroup.FindStringSubmatch(line); len(m) > 1 {
		state.NodeGroup = m[1]
	}
	// Extract total resources from "total":{...} block
	if m := reTotal.FindStringSubmatch(line); len(m) > 1 {
		state.Total = parseResourceMap(m[1])
	}
	// Extract available resources from "available":{...} block
	if m := reAvailable.FindStringSubmatch(line); len(m) > 1 {
		state.Available = parseResourceMap(m[1])
	}
}

// parseResourceMap parses a resource string like "memory: [100000000000000], CPU: [10000, 10000]".
// Multi-instance resources (e.g., CPU with 4 cores: [10000, 10000, 10000, 10000]) are summed.
// Values are scaled by 10000 in Ray's internal FixedPoint format, so we divide accordingly.
func parseResourceMap(resourceStr string) map[string]float64 {
	resources := make(map[string]float64)

	matches := reResourceKV.FindAllStringSubmatch(resourceStr, -1)
	for _, match := range matches {
		if len(match) > 2 {
			key := strings.TrimSpace(match[1]) // e.g., "memory" or "object_store_memory"
			valueStr := match[2]               // e.g., "100000000000000" or "10000, 10000, 10000, 10000"

			// Skip node-specific resources like "node:10.244.0.9"
			if strings.HasPrefix(key, "node:") {
				continue
			}

			// Sum all instance values. Single-instance resources (e.g., memory: [100000000000000])
			// have one element; multi-instance resources (e.g., CPU: [10000, 10000]) have multiple.
			var total float64
			for part := range strings.SplitSeq(valueStr, ",") {
				part = strings.TrimSpace(part)
				if part == "" {
					continue
				}
				v, err := strconv.ParseFloat(part, 64)
				if err != nil {
					logrus.Debugf("Failed to parse resource value %q in key %q: %v", part, key, err)
					continue
				}
				total += v
			}

			// Ray scales resources by 10000 internally
			resources[key] = total / rayResourceScale
		}
	}

	return resources
}

// GetUsedResources calculates used = total - available
func (s *NodeDebugState) GetUsedResources() map[string]float64 {
	used := make(map[string]float64)
	for key, total := range s.Total {
		available := s.Available[key]
		usedVal := total - available
		if usedVal < 0 {
			usedVal = 0
		}
		used[key] = usedVal
	}
	return used
}
