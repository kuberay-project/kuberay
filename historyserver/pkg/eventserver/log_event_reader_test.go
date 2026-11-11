package eventserver

import (
	"bufio"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ray-project/kuberay/historyserver/pkg/eventserver/types"
	"github.com/ray-project/kuberay/historyserver/pkg/utils"
)

// --- Test-local mock reader for LogEventReader tests ---

type logEventMockReader struct {
	files map[string]map[string]string
	dirs  map[string]map[string][]string
}

// mockClusterID is the single cluster prefix every fixture in this file lives under.
const mockClusterID = "cluster-history/raycluster/ns/cluster"

func newLogEventMockReader() *logEventMockReader {
	return &logEventMockReader{
		files: make(map[string]map[string]string),
		dirs:  make(map[string]map[string][]string),
	}
}

func (m *logEventMockReader) addFile(filePath, content string) {
	if m.files[mockClusterID] == nil {
		m.files[mockClusterID] = make(map[string]string)
	}
	m.files[mockClusterID][filePath] = content
}

func (m *logEventMockReader) addDir(dirPath string, entries []string) {
	if m.dirs[mockClusterID] == nil {
		m.dirs[mockClusterID] = make(map[string][]string)
	}
	m.dirs[mockClusterID][dirPath] = entries
}

func (m *logEventMockReader) List() []utils.ClusterInfo { return nil }

func (m *logEventMockReader) GetContent(prefix string, fileName string) io.Reader {
	if cd, ok := m.files[prefix]; ok {
		if content, ok := cd[fileName]; ok {
			return strings.NewReader(content)
		}
	}
	return nil
}

func (m *logEventMockReader) ListFiles(prefix string, dir string) []string {
	if cd, ok := m.dirs[prefix]; ok {
		if entries, ok := cd[dir]; ok {
			return entries
		}
	}
	return []string{}
}

// --- Tests ---

func TestReadLineWithLimit(t *testing.T) {
	t.Run("normal line", func(t *testing.T) {
		r := bufio.NewReader(strings.NewReader("hello world\n"))
		line, n, tooLong, err := readLineWithLimit(r, 1024)
		require.NoError(t, err)
		assert.Equal(t, "hello world\n", string(line))
		assert.Equal(t, 12, n)
		assert.False(t, tooLong)
	})

	t.Run("line exceeding limit is skipped and reader recovers", func(t *testing.T) {
		content := strings.Repeat("x", 200) + "\nshort\n"
		r := bufio.NewReader(strings.NewReader(content))

		line1, _, tooLong1, err1 := readLineWithLimit(r, 100)
		require.NoError(t, err1)
		assert.True(t, tooLong1)
		assert.Nil(t, line1)

		line2, _, tooLong2, err2 := readLineWithLimit(r, 100)
		require.NoError(t, err2)
		assert.False(t, tooLong2)
		assert.Equal(t, "short\n", string(line2))
	})

	t.Run("EOF without trailing newline", func(t *testing.T) {
		r := bufio.NewReader(strings.NewReader("no newline"))
		line, n, tooLong, err := readLineWithLimit(r, 1024)
		require.ErrorIs(t, err, io.EOF)
		assert.Equal(t, "no newline", string(line))
		assert.Equal(t, 10, n)
		assert.False(t, tooLong)
	})

	t.Run("boundary: exactly at limit vs one over", func(t *testing.T) {
		atLimit := strings.Repeat("a", 50) + "\n"
		r1 := bufio.NewReader(strings.NewReader(atLimit))
		_, _, tooLong1, _ := readLineWithLimit(r1, 51)
		assert.False(t, tooLong1, "line exactly at limit should not be too long")

		overLimit := strings.Repeat("a", 51) + "\n"
		r2 := bufio.NewReader(strings.NewReader(overLimit))
		_, _, tooLong2, _ := readLineWithLimit(r2, 51)
		assert.True(t, tooLong2, "line one byte over limit should be too long")
	})
}

// Realistic event JSON lines, modeled after actual Ray event log files.
const (
	testJobEvent     = `{"event_id":"1Ac5EF8dC3B20F78","source_type":"JOBS","source_hostname":"raycluster-head-6gvqk","source_pid":568,"severity":"INFO","message":"Started a ray job rayjob-abc.","timestamp":"1770635705","custom_fields":{"job_id":"01000000","submission_id":"rayjob-abc"}}`
	testGlobalEvent  = `{"event_id":"C798f8dC4Dce8B8b","source_type":"GCS","source_hostname":"raycluster-head-6gvqk","source_pid":568,"severity":"WARNING","message":"Node disconnected.","timestamp":"1770635720"}`
	testNewlineEvent = `{"event_id":"D1E2F3A4B5C6D7E8","source_type":"GCS","source_hostname":"raycluster-head-6gvqk","source_pid":568,"severity":"INFO","message":"line1\\nline2\\rline3","timestamp":"1770635740"}`
)

func TestReadEventFile(t *testing.T) {
	t.Run("groups by job_id and deduplicates by event_id", func(t *testing.T) {
		mock := newLogEventMockReader()
		content := strings.Join([]string{
			testJobEvent,
			testGlobalEvent,
			testJobEvent, // duplicate event_id — should be deduped
		}, "\n") + "\n"
		mock.addFile("session/node1/logs/events/event_JOBS.log", content)

		reader := NewLogEventReader(mock)
		jobEventMap := types.NewJobEventMap()
		err := reader.readEventFile(mockClusterID, "session/node1/logs/events/event_JOBS.log", jobEventMap)
		require.NoError(t, err)

		events := jobEventMap.GetAllEvents()
		assert.Len(t, events["01000000"], 1, "should deduplicate by event_id")
		assert.Len(t, events["global"], 1, "event without job_id goes to global")
	})

	t.Run("skips invalid JSON and events without event_id", func(t *testing.T) {
		mock := newLogEventMockReader()
		noIDEvent := `{"source_type":"GCS","severity":"INFO","message":"no id","timestamp":"1770635730"}`
		content := strings.Join([]string{
			testGlobalEvent,
			"INVALID JSON",
			noIDEvent,
		}, "\n") + "\n"
		mock.addFile("session/node1/logs/events/event_GCS.log", content)

		reader := NewLogEventReader(mock)
		jobEventMap := types.NewJobEventMap()
		err := reader.readEventFile(mockClusterID, "session/node1/logs/events/event_GCS.log", jobEventMap)
		require.NoError(t, err)

		events := jobEventMap.GetAllEvents()
		assert.Len(t, events["global"], 1, "should skip invalid JSON and events without event_id")
	})

	t.Run("restores escaped newlines in message", func(t *testing.T) {
		mock := newLogEventMockReader()
		mock.addFile("session/node1/logs/events/event_GCS.log", testNewlineEvent+"\n")

		reader := NewLogEventReader(mock)
		jobEventMap := types.NewJobEventMap()
		err := reader.readEventFile(mockClusterID, "session/node1/logs/events/event_GCS.log", jobEventMap)
		require.NoError(t, err)

		events := jobEventMap.GetAllEvents()
		require.Len(t, events["global"], 1)
		assert.Equal(t, "line1\nline2\nline3", events["global"][0]["message"])
	})

	t.Run("handles empty file and missing file", func(t *testing.T) {
		mock := newLogEventMockReader()
		mock.addFile("session/node1/logs/events/event_GCS.log", "")
		reader := NewLogEventReader(mock)

		jobEventMap := types.NewJobEventMap()
		err := reader.readEventFile(mockClusterID, "session/node1/logs/events/event_GCS.log", jobEventMap)
		require.NoError(t, err)
		assert.Empty(t, jobEventMap.GetAllEvents())

		err = reader.readEventFile(mockClusterID, "nonexistent", types.NewJobEventMap())
		assert.Error(t, err)
	})
}

func TestReadLogEvents(t *testing.T) {
	t.Run("reads events from multiple nodes, skips non-event files", func(t *testing.T) {
		mock := newLogEventMockReader()

		mock.addDir("session1", []string{"node1/", "node2/", "stray_file.txt"})
		mock.addDir("session1/node1/logs/events",
			[]string{"event_GCS.log", "debug.log", "event_GCS.rotated.1788398100000000000-4390125.log"})
		mock.addDir("session1/node2/logs/events", []string{"event_RAYLET.log"})

		mock.addFile("session1/node1/logs/events/event_GCS.log",
			`{"event_id":"e1","source_type":"GCS","severity":"INFO","message":"from node1","timestamp":"1770635700"}`+"\n")
		mock.addFile("session1/node2/logs/events/event_RAYLET.log",
			`{"event_id":"e2","source_type":"RAYLET","severity":"WARNING","message":"from node2","timestamp":"1770635800"}`+"\n")
		// A collected rotated generation of event_GCS.log is read like any other event file.
		mock.addFile("session1/node1/logs/events/event_GCS.rotated.1788398100000000000-4390125.log",
			`{"event_id":"e3","source_type":"GCS","severity":"INFO","message":"rotated generation","timestamp":"1770635600"}`+"\n")

		reader := NewLogEventReader(mock)
		store := types.NewClusterLogEventMap()
		clusterInfo := utils.ClusterInfo{Name: "cluster", Namespace: "ns", SessionName: "session1"}

		err := reader.ReadLogEvents(clusterInfo, "cluster_ns_session1", store)
		require.NoError(t, err)

		events := store.GetAllEvents("cluster_ns_session1")
		assert.Len(t, events["global"], 3, "should read active and rotated event files from both nodes, skipping non-event files")
	})

	t.Run("handles empty cluster with no nodes", func(t *testing.T) {
		mock := newLogEventMockReader()
		mock.addDir("session1", []string{})

		reader := NewLogEventReader(mock)
		store := types.NewClusterLogEventMap()
		clusterInfo := utils.ClusterInfo{Name: "cluster", Namespace: "ns", SessionName: "session1"}

		err := reader.ReadLogEvents(clusterInfo, "cluster_ns_session1", store)
		require.NoError(t, err)
		assert.Empty(t, store.GetAllEvents("cluster_ns_session1"))
	})

	t.Run("returns error when every listed file fails to read", func(t *testing.T) {
		// Intentionally skip addFile.
		mock := newLogEventMockReader()
		mock.addDir("session1", []string{"node1/"})
		mock.addDir("session1/node1/logs/events", []string{"event_GCS.log", "event_RAYLET.log"})

		reader := NewLogEventReader(mock)
		store := types.NewClusterLogEventMap()
		clusterInfo := utils.ClusterInfo{Name: "cluster", Namespace: "ns", SessionName: "session1"}

		err := reader.ReadLogEvents(clusterInfo, "cluster_ns_session1", store)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "read 0 of 2")
	})

	t.Run("partial read surfaces error but preserves successful events", func(t *testing.T) {
		mock := newLogEventMockReader()
		mock.addDir("session1", []string{"node1/"})
		mock.addDir("session1/node1/logs/events", []string{"event_GCS.log", "event_RAYLET.log"})
		mock.addFile("session1/node1/logs/events/event_GCS.log",
			`{"event_id":"e1","source_type":"GCS","severity":"INFO","message":"ok","timestamp":"1770635700"}`+"\n")

		reader := NewLogEventReader(mock)
		store := types.NewClusterLogEventMap()
		clusterInfo := utils.ClusterInfo{Name: "cluster", Namespace: "ns", SessionName: "session1"}

		err := reader.ReadLogEvents(clusterInfo, "cluster_ns_session1", store)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "read 1 of 2")
		events := store.GetAllEvents("cluster_ns_session1")
		assert.Len(t, events["global"], 1)
	})
}
