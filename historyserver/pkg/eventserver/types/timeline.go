package types

// ChromeTraceEvent represents a trace event in Chrome Tracing Format.
// Timestamp and Duration are in microseconds, as required by the format
// (https://docs.google.com/document/d/1CvAClvFfyA5R-PhYUmn5OOQtYMH4h6I0nSsKchNAySU).
//
// Phase values:
//   - "M": Metadata event (e.g., process_name, thread_name)
//   - "X": Complete event (duration events with start and end times)
type ChromeTraceEvent struct {
	Category  string         `json:"cat,omitempty"`
	Name      string         `json:"name"`
	PID       int            `json:"pid"`
	TID       *int           `json:"tid"`
	Timestamp *float64       `json:"ts,omitempty"`  // microseconds
	Duration  *float64       `json:"dur,omitempty"` // microseconds
	Color     string         `json:"cname,omitempty"`
	Args      map[string]any `json:"args"`
	Phase     string         `json:"ph"` // Event phase: "M" (metadata) or "X" (complete)
}
