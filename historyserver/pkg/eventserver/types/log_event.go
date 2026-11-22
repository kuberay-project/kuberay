// Package types provides data structures for the History Server Events API.
//
// This file implements the Event types that match Ray Dashboard's /events API.
// Events are read from logs/{nodeId}/events/event_*.log files stored in object storage.
//
// Reference:
//   - Ray Dashboard event_head.py: python/ray/dashboard/modules/event/event_head.py
//   - Ray event.proto: src/ray/protobuf/event.proto
//   - Ray event_logger.py: python/ray/_private/event/event_logger.py
package types

import (
	"sort"
	"strconv"
	"strings"
	"sync"
	"unicode"
)

// MaxEventsPerJob is the maximum number of events to cache per job.
// This matches Ray Dashboard's MAX_EVENTS_TO_CACHE constant.
const MaxEventsPerJob = 10000

// LogEvent represents an event from logs/events/event_*.log files.
// This matches the format defined in Ray's event.proto and written by EventLoggerAdapter.
//
// Storage format: snake_case JSON (as written to event_*.log files)
// API response format: camelCase JSON (converted by ToAPIResponse)
type LogEvent struct {
	EventID        string         `json:"event_id"`
	SourceType     string         `json:"source_type"`     // COMMON, CORE_WORKER, GCS, RAYLET, CLUSTER_LIFECYCLE, AUTOSCALER, JOBS
	SourceHostname string         `json:"source_hostname"` // Hostname where event originated
	SourcePid      int            `json:"source_pid"`      // Process ID that generated the event
	Severity       string         `json:"severity"`        // INFO, WARNING, ERROR, FATAL, DEBUG, TRACE
	Label          string         `json:"label"`           // Event label (usually empty)
	Message        string         `json:"message"`         // Human-readable message
	Timestamp      string         `json:"timestamp"`       // Unix seconds as string (e.g., "1770635705")
	CustomFields   map[string]any `json:"custom_fields"`   // Custom key-value pairs (contains job_id, submission_id, etc.)
}

// GetJobID extracts the job_id from custom_fields for event grouping.
// Returns "global" if no job_id is present, matching Ray Dashboard behavior.
func (e *LogEvent) GetJobID() string {
	if e.CustomFields == nil {
		return "global"
	}
	if jobID, ok := e.CustomFields["job_id"]; ok && jobID != nil {
		if jobIDStr, ok := jobID.(string); ok && jobIDStr != "" {
			return jobIDStr
		}
	}
	return "global"
}

func (e LogEvent) DeepCopy() LogEvent {
	cp := e
	cp.CustomFields = cloneMapAny(e.CustomFields)
	return cp
}

func cloneMapAny(m map[string]any) map[string]any {
	if m == nil {
		return nil
	}
	out := make(map[string]any, len(m))
	for k, v := range m {
		out[k] = cloneAny(v)
	}
	return out
}

func cloneAny(v any) any {
	switch typed := v.(type) {
	case map[string]any:
		return cloneMapAny(typed)
	case []any:
		out := make([]any, len(typed))
		for i, item := range typed {
			out[i] = cloneAny(item)
		}
		return out
	default:
		return v
	}
}

// RestoreNewline restores escaped newlines in the message field.
// This matches Ray Dashboard's _restore_newline() function in event_utils.py.
func (e *LogEvent) RestoreNewline() {
	e.Message = strings.ReplaceAll(e.Message, "\\n", "\n")
	e.Message = strings.ReplaceAll(e.Message, "\\r", "\n")
}

// ToAPIResponse converts the LogEvent to a map with camelCase keys.
// This matches Ray Dashboard's to_google_style() function and rest_response() behavior.
func (e *LogEvent) ToAPIResponse() map[string]any {
	result := map[string]any{
		"eventId":        e.EventID,
		"sourceType":     e.SourceType,
		"sourceHostname": e.SourceHostname,
		"sourcePid":      e.SourcePid,
		"severity":       e.Severity,
		"label":          e.Label,
		"message":        e.Message,
		"timestamp":      e.Timestamp,
	}

	if e.CustomFields != nil {
		result["customFields"] = toGoogleStyle(e.CustomFields)
	}

	return result
}

// toCamelCase converts a snake_case string to camelCase.
// Example: "source_hostname" -> "sourceHostname"
// This matches Ray Dashboard's to_camel_case() function in utils.py.
func toCamelCase(s string) string {
	if s == "" {
		return s
	}

	parts := strings.Split(s, "_")
	if len(parts) == 1 {
		return s
	}

	var result strings.Builder
	result.WriteString(parts[0])
	for _, part := range parts[1:] {
		if len(part) > 0 {
			runes := []rune(part)
			runes[0] = unicode.ToUpper(runes[0])
			result.WriteString(string(runes))
		}
	}
	return result.String()
}

// toGoogleStyle recursively converts all keys in a map from snake_case to camelCase.
// This matches Ray Dashboard's to_google_style() function in utils.py.
func toGoogleStyle(d map[string]any) map[string]any {
	result := make(map[string]any, len(d))

	for k, v := range d {
		camelKey := toCamelCase(k)

		switch typedV := v.(type) {
		case map[string]any:
			result[camelKey] = toGoogleStyle(typedV)
		case []any:
			newList := make([]any, len(typedV))
			for i, item := range typedV {
				if itemMap, ok := item.(map[string]any); ok {
					newList[i] = toGoogleStyle(itemMap)
				} else {
					newList[i] = item
				}
			}
			result[camelKey] = newList
		default:
			result[camelKey] = v
		}
	}

	return result
}

// JobEventMap stores events for a single cluster session, grouped by job_id.
// Events are deduplicated by event_id (using map[eventId]*LogEvent).
// This matches Ray Dashboard's self.events dict structure: {job_id: {event_id: event}}.
type JobEventMap struct {
	// events maps job_id -> event_id -> LogEvent
	events map[string]map[string]*LogEvent
	mu     sync.RWMutex
}

// NewJobEventMap creates a new JobEventMap instance.
func NewJobEventMap() *JobEventMap {
	return &JobEventMap{
		events: make(map[string]map[string]*LogEvent),
	}
}

// AddEvent adds an event to the map, deduplicating by event_id.
// Enforces the per-job event limit matching Ray Dashboard's MAX_EVENTS_TO_CACHE.
func (m *JobEventMap) AddEvent(event *LogEvent) {
	m.mu.Lock()
	defer m.mu.Unlock()

	jobID := event.GetJobID()

	if m.events[jobID] == nil {
		m.events[jobID] = make(map[string]*LogEvent)
	}

	m.events[jobID][event.EventID] = event

	// Enforce limit: if exceeds 110% of max, trim to max
	// This matches Ray Dashboard's eviction logic in _update_events()
	if len(m.events[jobID]) > int(float64(MaxEventsPerJob)*1.1) {
		m.trimToLimit(jobID)
	}
}

// timestampLess compares two Unix epoch second timestamps numerically.
func timestampLess(a, b string) bool {
	ai, _ := strconv.ParseInt(a, 10, 64)
	bi, _ := strconv.ParseInt(b, 10, 64)
	return ai < bi
}

// trimToLimit reduces events for a job to MaxEventsPerJob, keeping the newest.
// Must be called with lock held.
func (m *JobEventMap) trimToLimit(jobID string) {
	events := make([]*LogEvent, 0, len(m.events[jobID]))
	for _, e := range m.events[jobID] {
		events = append(events, e)
	}

	// Sort by timestamp ascending (numeric comparison of Unix epoch seconds)
	sort.Slice(events, func(i, j int) bool {
		return timestampLess(events[i].Timestamp, events[j].Timestamp)
	})

	// Keep only the newest MaxEventsPerJob events
	if len(events) > MaxEventsPerJob {
		events = events[len(events)-MaxEventsPerJob:]
	}

	// Rebuild the map
	m.events[jobID] = make(map[string]*LogEvent, len(events))
	for _, e := range events {
		m.events[jobID][e.EventID] = e
	}
}

// GetAllEvents returns all events grouped by job_id, converted to API response format.
// Response format: {"global": [{event1}, {event2}], "jobId1": [...], ...}
// This matches Ray Dashboard's get_event() response when job_id is not specified.
func (m *JobEventMap) GetAllEvents() map[string][]map[string]any {
	m.mu.RLock()
	defer m.mu.RUnlock()

	result := make(map[string][]map[string]any, len(m.events))

	for jobID, eventMap := range m.events {
		// Convert map to sorted slice
		events := make([]*LogEvent, 0, len(eventMap))
		for _, e := range eventMap {
			events = append(events, e)
		}

		// Sort by timestamp ascending (numeric comparison of Unix epoch seconds)
		sort.Slice(events, func(i, j int) bool {
			return timestampLess(events[i].Timestamp, events[j].Timestamp)
		})

		// Convert to API response format
		eventList := make([]map[string]any, len(events))
		for i, e := range events {
			eventList[i] = e.ToAPIResponse()
		}

		result[jobID] = eventList
	}

	return result
}

// GetEventsByJobID returns events for a specific job, converted to API response format.
// Response format: [{event1}, {event2}, ...]
// This matches Ray Dashboard's get_event() response when job_id is specified.
func (m *JobEventMap) GetEventsByJobID(jobID string) []map[string]any {
	m.mu.RLock()
	defer m.mu.RUnlock()

	eventMap, ok := m.events[jobID]
	if !ok {
		return []map[string]any{}
	}

	// Convert map to sorted slice
	events := make([]*LogEvent, 0, len(eventMap))
	for _, e := range eventMap {
		events = append(events, e)
	}

	// Sort by timestamp ascending (numeric comparison of Unix epoch seconds)
	sort.Slice(events, func(i, j int) bool {
		return timestampLess(events[i].Timestamp, events[j].Timestamp)
	})

	// Convert to API response format
	result := make([]map[string]any, len(events))
	for i, e := range events {
		result[i] = e.ToAPIResponse()
	}

	return result
}

// LogEventStore is the top-level store for Log Events.
// It stores JobEventMaps per cluster session.
// Key format: "{clusterName}_{namespace}_{sessionName}"
type LogEventStore struct {
	sessions map[string]*JobEventMap
	mu       sync.RWMutex
}

// ClusterLogEventMap is an alias for LogEventStore.
// This maintains compatibility with existing eventserver.go and router.go code.
// The naming follows the pattern of other cluster-level maps (e.g., ClusterTaskMap).
type ClusterLogEventMap = LogEventStore

// NewLogEventStore creates a new LogEventStore instance.
func NewLogEventStore() *LogEventStore {
	return &LogEventStore{
		sessions: make(map[string]*JobEventMap),
	}
}

// NewClusterLogEventMap creates a new ClusterLogEventMap instance.
// This is an alias for NewLogEventStore for compatibility.
func NewClusterLogEventMap() *ClusterLogEventMap {
	return NewLogEventStore()
}

// GetOrCreateJobEventMap returns the JobEventMap for the given cluster session, creating it if needed.
func (s *LogEventStore) GetOrCreateJobEventMap(clusterSessionKey string) *JobEventMap {
	s.mu.Lock()
	defer s.mu.Unlock()

	if m, ok := s.sessions[clusterSessionKey]; ok {
		return m
	}
	m := NewJobEventMap()
	s.sessions[clusterSessionKey] = m
	return m
}

// GetAllEvents returns all events for a cluster session, grouped by job_id.
// Returns empty map if cluster session not found.
func (s *LogEventStore) GetAllEvents(clusterSessionKey string) map[string][]map[string]any {
	s.mu.RLock()
	m, ok := s.sessions[clusterSessionKey]
	s.mu.RUnlock()

	if !ok {
		return map[string][]map[string]any{}
	}
	return m.GetAllEvents()
}

// GetEventsByJobID returns events for a specific job in a cluster session.
// Returns empty slice if cluster session or job not found.
func (s *LogEventStore) GetEventsByJobID(clusterSessionKey, jobID string) []map[string]any {
	s.mu.RLock()
	m, ok := s.sessions[clusterSessionKey]
	s.mu.RUnlock()

	if !ok {
		return []map[string]any{}
	}
	return m.GetEventsByJobID(jobID)
}

// GetLogEventsByJobID returns a deep copy of stored log events grouped by job ID.
func (s *LogEventStore) GetLogEventsByJobID(clusterSessionKey string) map[string][]LogEvent {
	s.mu.RLock()
	m, ok := s.sessions[clusterSessionKey]
	s.mu.RUnlock()

	if !ok {
		return map[string][]LogEvent{}
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	out := make(map[string][]LogEvent, len(m.events))
	for jobID, eventMap := range m.events {
		events := make([]LogEvent, 0, len(eventMap))
		for _, e := range eventMap {
			events = append(events, e.DeepCopy())
		}
		sort.Slice(events, func(i, j int) bool {
			return timestampLess(events[i].Timestamp, events[j].Timestamp)
		})
		out[jobID] = events
	}
	return out
}
