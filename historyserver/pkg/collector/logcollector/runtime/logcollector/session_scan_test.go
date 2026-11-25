package logcollector

import (
	"path/filepath"
	"testing"
)

// One scan pass uploads every active log at the object path the shutdown
// upload would use: debug_state.txt in full, everything else by chunk.
func TestCollectSessionLogsUploadsActiveLogs(t *testing.T) {
	logsDir := t.TempDir()
	writer := NewMockStorageWriter()
	handler := newRotatedTestHandler(writer)

	writeLogFile(t, filepath.Join(logsDir, "job-driver-x.log"), "driver\n")
	writeLogFile(t, filepath.Join(logsDir, "events", "event_AUTOSCALER.log"), "event\n")
	writeLogFile(t, filepath.Join(logsDir, "debug_state.txt"), "state\n")
	writeLogFile(t, filepath.Join(logsDir, "raylet.out"), "raylet\n")

	handler.collectSessionLogsUnder(logsDir, testSessionID, testNodeID, nil)

	assertWritten(t, writer, map[string]string{
		testLogPrefix + "job-driver-x.log.chunks/00000000000000000000":            "driver\n",
		testLogPrefix + "events/event_AUTOSCALER.log.chunks/00000000000000000000": "event\n",
		testLogPrefix + "debug_state.txt":                                         "state\n",
		testLogPrefix + "raylet.out.chunks/00000000000000000000":                  "raylet\n",
	})
}
