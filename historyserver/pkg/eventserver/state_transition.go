package eventserver

import (
	"sort"
	"time"
)

// StateTransition defines a common interface for state transitions.
type StateTransition interface {
	GetState() string
	GetTimestamp() time.Time
}

// StateTransitionKey is a unique key for deduplication.
type StateTransitionKey struct {
	State     string
	Timestamp int64
}

// MergeStateTransitions merges new state transitions into existing ones, avoiding duplicates and maintaining chronological order.
func MergeStateTransitions[T StateTransition](existingStateTransitions []T, newStateTransitions []T) []T {
	// Build a map of existing transition keys for deduplication.
	existingKeys := make(map[StateTransitionKey]bool, len(existingStateTransitions))
	for _, tr := range existingStateTransitions {
		key := StateTransitionKey{
			State:     tr.GetState(),
			Timestamp: tr.GetTimestamp().UnixNano(),
		}
		existingKeys[key] = true
	}

	// Append new state transitions that haven't seen before.
	mergedStateTransitions := make([]T, 0, len(existingStateTransitions)+len(newStateTransitions))
	mergedStateTransitions = append(mergedStateTransitions, existingStateTransitions...)
	for _, tr := range newStateTransitions {
		key := StateTransitionKey{
			State:     tr.GetState(),
			Timestamp: tr.GetTimestamp().UnixNano(),
		}
		if !existingKeys[key] {
			mergedStateTransitions = append(mergedStateTransitions, tr)
			existingKeys[key] = true
		}
	}

	// Sort state transitions by timestamp to maintain chronological order.
	sort.Slice(mergedStateTransitions, func(i, j int) bool {
		return mergedStateTransitions[i].GetTimestamp().Before(mergedStateTransitions[j].GetTimestamp())
	})

	return mergedStateTransitions
}
