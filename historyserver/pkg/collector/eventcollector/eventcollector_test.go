package eventcollector

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/emicklei/go-restful/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---------- Test-local MockWriter (thread-safe) ----------

type mockWriter struct {
	mu    sync.Mutex
	dirs  map[string]bool
	files map[string][]byte
	// failNext causes the next WriteFile call to fail.
	failNext bool
}

func newMockWriter() *mockWriter {
	return &mockWriter{
		dirs:  make(map[string]bool),
		files: make(map[string][]byte),
	}
}

func (m *mockWriter) CreateDirectory(p string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.dirs[p] = true
	return nil
}

func (m *mockWriter) WriteFile(file string, reader io.ReadSeeker) error {
	m.mu.Lock()
	if m.failNext {
		m.failNext = false
		m.mu.Unlock()
		return assertErr("mock write failure")
	}
	m.mu.Unlock()

	b, err := io.ReadAll(reader)
	if err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.files[file] = b
	return nil
}

func (m *mockWriter) get(file string) ([]byte, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	v, ok := m.files[file]
	return v, ok
}

func (m *mockWriter) fileKeys() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	keys := make([]string, 0, len(m.files))
	for k := range m.files {
		keys = append(keys, k)
	}
	return keys
}

func (m *mockWriter) failPending() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.failNext
}

type assertErr string

func (e assertErr) Error() string { return string(e) }

// ---------- Helpers ----------

// newTestCollector builds an EventCollector without starting Run(), so tests
// do not spawn the nodeID watcher or HTTP server.
func newTestCollector(t *testing.T, writer *mockWriter, opts Options) *EventCollector {
	t.Helper()
	if opts.DataDir == "" {
		opts.DataDir = t.TempDir()
	}
	if opts.RotationInterval == 0 {
		opts.RotationInterval = 5 * time.Minute
	}
	if opts.MaxFileSizeBytes == 0 {
		opts.MaxFileSizeBytes = defaultMaxFileSizeBytes
	}
	if opts.MaxDiskBytes == 0 {
		opts.MaxDiskBytes = defaultMaxDiskBytes
	}

	return &EventCollector{
		storageWriter:      writer,
		root:               "history",
		sessionDir:         "/tmp/ray/session_latest",
		nodeID:             "node-1",
		clusterName:        "cluster",
		clusterNamespace:   "ns",
		sessionName:        "session_abc",
		currentSessionName: "session_abc",
		currentNodeID:      "node-1",
		stopProducers:      make(chan struct{}),
		dataDir:            opts.DataDir,
		rotationInterval:   opts.RotationInterval,
		maxFileSizeBytes:   opts.MaxFileSizeBytes,
		maxDiskBytes:       opts.MaxDiskBytes,
		compressionEnabled: opts.CompressionEnabled,
		activeFiles:        make(map[string]*activeFileState),
		rotationQueue:      make(chan rotationTask, rotationQueueSize),
	}
}

// ---------- Pure helper function tests ----------

func TestIsNodeEvent(t *testing.T) {
	assert.True(t, isNodeEvent(map[string]any{"eventType": "NODE_LIFECYCLE_EVENT"}))
	assert.True(t, isNodeEvent(map[string]any{"eventType": "NODE_DEFINITION_EVENT"}))
	assert.False(t, isNodeEvent(map[string]any{"eventType": "TASK_DEFINITION_EVENT"}))
	assert.False(t, isNodeEvent(map[string]any{}))
}

func TestGetJobID(t *testing.T) {
	// Payload job IDs arrive base64-encoded and are normalized to hex.
	evt := map[string]any{
		"driverJobLifecycleEvent": map[string]any{"jobId": "AQAAAA=="},
	}
	assert.Equal(t, "01000000", getJobID(evt))

	evt2 := map[string]any{
		"taskDefinitionEvent": map[string]any{"jobId": 7},
	}
	assert.Equal(t, "7", getJobID(evt2))

	assert.Empty(t, getJobID(map[string]any{}))
	assert.Empty(t, getJobID(map[string]any{"taskDefinitionEvent": map[string]any{}}))
}

func TestCategorize(t *testing.T) {
	ec := &EventCollector{}
	assert.Equal(t, categoryNodeEvents,
		ec.categorize(map[string]any{"eventType": "NODE_LIFECYCLE_EVENT"}))

	assert.Equal(t, path.Join(categoryJobPrefix, "job-1"),
		ec.categorize(map[string]any{
			"eventType":           "TASK_DEFINITION_EVENT",
			"taskDefinitionEvent": map[string]any{"jobId": "job-1"},
		}))

	// fallback: neither node event nor recognizable jobID → node_events
	assert.Equal(t, categoryNodeEvents,
		ec.categorize(map[string]any{"eventType": "UNKNOWN"}))
}

func TestSanitizeFileComponent(t *testing.T) {
	assert.Equal(t, "unknown", sanitizeFileComponent(""))
	assert.Equal(t, "a_b", sanitizeFileComponent("a/b"))
	assert.Equal(t, "a_b_c", sanitizeFileComponent("a b/c"))
}

func TestGzipFile(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "in.txt")
	require.NoError(t, os.WriteFile(src, []byte("hello world\nhello world\n"), 0o600))
	dst := src + ".gz"

	require.NoError(t, gzipFile(src, dst))

	raw, err := os.ReadFile(dst)
	require.NoError(t, err)
	gr, err := gzip.NewReader(bytes.NewReader(raw))
	require.NoError(t, err)
	defer gr.Close()
	out, err := io.ReadAll(gr)
	require.NoError(t, err)
	assert.Equal(t, "hello world\nhello world\n", string(out))
}

func TestDirSize(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "a"), []byte("12345"), 0o600))
	sub := filepath.Join(dir, "sub")
	require.NoError(t, os.Mkdir(sub, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(sub, "b"), []byte("abcdefg"), 0o600))

	size, err := dirSize(dir)
	require.NoError(t, err)
	assert.Equal(t, int64(12), size)
}

// ---------- buildEventStorageKey ----------

func TestBuildEventStorageKey_NodeEventsGzip(t *testing.T) {
	ec := newTestCollector(t, newMockWriter(), Options{CompressionEnabled: true})
	ts := time.Date(2026, 5, 13, 10, 30, 0, 0, time.UTC)
	key := ec.buildEventStorageKey(rotationTask{
		category:    categoryNodeEvents,
		sessionName: "session_abc",
		nodeID:      "node-1",
		createdAt:   ts,
	})
	assert.Equal(t, fmt.Sprintf("history/cluster-history/raycluster/ns/cluster/session_abc/node-1/node_events/node-1-2026-05-13-10-%d.jsonl.gz", ts.UnixNano()), key)
}

func TestBuildEventStorageKey_JobEventsNoCompression(t *testing.T) {
	ec := newTestCollector(t, newMockWriter(), Options{CompressionEnabled: false})
	ts := time.Date(2026, 1, 2, 3, 0, 0, 0, time.UTC)
	key := ec.buildEventStorageKey(rotationTask{
		category:    path.Join(categoryJobPrefix, "job-7"),
		sessionName: "session_xyz",
		nodeID:      "nX",
		createdAt:   ts,
	})
	assert.Equal(t, fmt.Sprintf("history/cluster-history/raycluster/ns/cluster/session_xyz/nX/job_events/job-7/nX-2026-01-02-03-%d.jsonl", ts.UnixNano()), key)
}

// ---------- openNewActiveFile / rotateFileLocked ----------

func TestOpenNewActiveFile_CreatesJSONLFile(t *testing.T) {
	ec := newTestCollector(t, newMockWriter(), Options{})
	state, err := ec.openNewActiveFileLocked(categoryNodeEvents, ec.currentSessionName)
	require.NoError(t, err)
	require.NotNil(t, state)
	defer state.file.Close()

	assert.True(t, strings.HasSuffix(state.path, ".jsonl"))
	assert.Contains(t, state.path,
		filepath.Join(ec.dataDir, "cluster_ns", ec.currentSessionName, categoryNodeEvents))

	// File must exist on disk.
	_, err = os.Stat(state.path)
	require.NoError(t, err)
	assert.Equal(t, "node-1", state.nodeID)
}

func TestRotateFileLocked_EmptyFileIsDropped(t *testing.T) {
	writer := newMockWriter()
	ec := newTestCollector(t, writer, Options{})

	state, err := ec.openNewActiveFileLocked(categoryNodeEvents, ec.currentSessionName)
	require.NoError(t, err)

	jsonlPath := state.path
	require.NoError(t, ec.rotateFileLocked(categoryNodeEvents, false))

	// Empty file should be removed, and no rotation task should have been queued.
	_, err = os.Stat(jsonlPath)
	assert.True(t, os.IsNotExist(err))
	assert.Empty(t, ec.rotationQueue)
}

// ---------- End-to-end: PersistEvents writes JSONL & rotation uploads gzip ----------

func callPersistEvents(t *testing.T, ec *EventCollector, body []byte) *httptest.ResponseRecorder {
	t.Helper()
	httpReq := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/v1/events", bytes.NewReader(body))
	httpReq.Header.Set("Content-Type", restful.MIME_JSON)
	rec := httptest.NewRecorder()

	req := restful.NewRequest(httpReq)
	resp := restful.NewResponse(rec)
	resp.SetRequestAccepts(restful.MIME_JSON)

	ec.PersistEvents(req, resp)
	return rec
}

func TestPersistEvents_AppendsJSONLToDisk(t *testing.T) {
	ec := newTestCollector(t, newMockWriter(), Options{CompressionEnabled: true})

	events := []map[string]any{
		{
			"eventId":     "e1",
			"eventType":   "NODE_LIFECYCLE_EVENT",
			"timestamp":   time.Now().UTC().Format(time.RFC3339Nano),
			"sessionName": ec.currentSessionName,
		},
		{
			"eventId":   "e2",
			"eventType": "TASK_DEFINITION_EVENT",
			"taskDefinitionEvent": map[string]any{
				"jobId": "job-1",
			},
			"timestamp":   time.Now().UTC().Format(time.RFC3339Nano),
			"sessionName": ec.currentSessionName,
		},
	}
	body, err := json.Marshal(events)
	require.NoError(t, err)

	rec := callPersistEvents(t, ec, body)
	assert.Equal(t, http.StatusOK, rec.Code)

	// Two active files expected: node_events and job_events/job-1.
	nodeState, ok := ec.activeFiles[categoryNodeEvents]
	require.True(t, ok)
	jobState, ok := ec.activeFiles[path.Join(categoryJobPrefix, "job-1")]
	require.True(t, ok)

	// Read node file from disk.
	nodeBytes, err := os.ReadFile(nodeState.path)
	require.NoError(t, err)
	lines := strings.Split(strings.TrimRight(string(nodeBytes), "\n"), "\n")
	require.Len(t, lines, 1)
	var m map[string]any
	require.NoError(t, json.Unmarshal([]byte(lines[0]), &m))
	assert.Equal(t, "e1", m["eventId"])

	// Job file exists and has one line.
	jobBytes, err := os.ReadFile(jobState.path)
	require.NoError(t, err)
	assert.Contains(t, string(jobBytes), `"eventId":"e2"`)

	// Disk accounting reflects the total bytes written.
	assert.Equal(t, int64(len(nodeBytes)+len(jobBytes)), ec.totalDiskUsed.Load())
}

func TestPersistEvents_SessionChangeRotates(t *testing.T) {
	writer := newMockWriter()
	ec := newTestCollector(t, writer, Options{CompressionEnabled: true})
	// Start compression/upload worker so rotation actually lands in storage.
	ec.consumerWG.Add(1)
	go ec.compressionUploadWorker()
	defer ec.shutdown()

	first := []map[string]any{{
		"eventId":     "s1",
		"eventType":   "NODE_LIFECYCLE_EVENT",
		"timestamp":   time.Now().UTC().Format(time.RFC3339Nano),
		"sessionName": "session_abc",
	}}
	body, _ := json.Marshal(first)
	callPersistEvents(t, ec, body)

	second := []map[string]any{{
		"eventId":     "s2",
		"eventType":   "NODE_LIFECYCLE_EVENT",
		"timestamp":   time.Now().UTC().Format(time.RFC3339Nano),
		"sessionName": "session_new",
	}}
	body, _ = json.Marshal(second)
	callPersistEvents(t, ec, body)

	// Wait for upload worker to drain the rotated file.
	require.Eventually(t, func() bool {
		return len(writer.fileKeys()) >= 1
	}, 2*time.Second, 20*time.Millisecond)

	// Uploaded key should be under the old session.
	keys := writer.fileKeys()
	found := false
	for _, k := range keys {
		if strings.Contains(k, "/session_abc/node-1/node_events/") && strings.HasSuffix(k, ".jsonl.gz") {
			found = true
			break
		}
	}
	assert.True(t, found, "expected uploaded key under old session, got: %v", keys)
}

// A single POST can span a session boundary. Events before the boundary must
// land in the old session, events from the boundary on in the new one, and
// none of them may be dropped (ray-project/kuberay#4442: the first event of the
// new session used to be flushed with the old session, and the rest of the
// batch was lost to an early return).
func TestPersistEvents_SessionChangeMidBatchKeepsAllEvents(t *testing.T) {
	writer := newMockWriter()
	ec := newTestCollector(t, writer, Options{CompressionEnabled: false})
	ec.consumerWG.Add(1)
	go ec.compressionUploadWorker()

	const oldSession, newSession = "session_abc", "session_new"
	nodeEvent := func(id, session string) map[string]any {
		return map[string]any{
			"eventId":     id,
			"eventType":   "NODE_LIFECYCLE_EVENT",
			"timestamp":   time.Now().UTC().Format(time.RFC3339Nano),
			"sessionName": session,
		}
	}
	jobEvent := func(id, session string) map[string]any {
		return map[string]any{
			"eventId":             id,
			"eventType":           "TASK_DEFINITION_EVENT",
			"taskDefinitionEvent": map[string]any{"jobId": "job-1"},
			"timestamp":           time.Now().UTC().Format(time.RFC3339Nano),
			"sessionName":         session,
		}
	}

	body, err := json.Marshal([]map[string]any{
		nodeEvent("n-old", oldSession),
		jobEvent("j-old", oldSession),
		nodeEvent("n-new-1", newSession),
		jobEvent("j-new", newSession),
		nodeEvent("n-new-2", newSession),
	})
	require.NoError(t, err)

	rec := callPersistEvents(t, ec, body)
	require.Equal(t, http.StatusOK, rec.Code)

	// Shutdown rotates the new session's still-active files and drains the queue.
	ec.shutdown()

	// eventIDs[key-suffix] collects the event IDs uploaded under each
	// "<session>/<category>" pair.
	eventIDs := make(map[string][]string)
	for _, key := range writer.fileKeys() {
		var session, category string
		switch {
		case strings.Contains(key, "/"+oldSession+"/"):
			session = oldSession
		case strings.Contains(key, "/"+newSession+"/"):
			session = newSession
		default:
			t.Fatalf("uploaded key %q is under neither session", key)
		}
		if strings.Contains(key, "/job_events/") {
			category = "job_events"
		} else {
			category = "node_events"
		}
		data, ok := writer.get(key)
		require.True(t, ok)
		for _, line := range strings.Split(strings.TrimRight(string(data), "\n"), "\n") {
			var e map[string]any
			require.NoError(t, json.Unmarshal([]byte(line), &e))
			assert.Equal(t, session, e["sessionName"],
				"event %v is stored under %s but belongs to %v", e["eventId"], session, e["sessionName"])
			id := session + "/" + category
			eventIDs[id] = append(eventIDs[id], e["eventId"].(string))
		}
	}

	assert.Equal(t, []string{"n-old"}, eventIDs[oldSession+"/node_events"])
	assert.Equal(t, []string{"j-old"}, eventIDs[oldSession+"/job_events"])
	assert.Equal(t, []string{"n-new-1", "n-new-2"}, eventIDs[newSession+"/node_events"])
	assert.Equal(t, []string{"j-new"}, eventIDs[newSession+"/job_events"])
}

func TestPersistEvents_RejectsUnderDiskPressure(t *testing.T) {
	ec := newTestCollector(t, newMockWriter(), Options{
		MaxDiskBytes:       100,
		CompressionEnabled: true,
	})
	// 80% of 100 = 80. Load to 90 to trip the watermark.
	ec.totalDiskUsed.Store(90)

	body := []byte(`[{"eventId":"x","eventType":"NODE_LIFECYCLE_EVENT","timestamp":"2026-05-13T10:00:00Z","sessionName":"session_abc"}]`)
	rec := callPersistEvents(t, ec, body)

	assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
	assert.Equal(t, "10", rec.Header().Get("Retry-After"))
}

func TestPersistEvents_BadJSONReturns400(t *testing.T) {
	ec := newTestCollector(t, newMockWriter(), Options{})
	rec := callPersistEvents(t, ec, []byte("not json"))
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestPersistEvents_MissingTimestampReturns400(t *testing.T) {
	ec := newTestCollector(t, newMockWriter(), Options{})
	body := []byte(`[{"eventId":"x","eventType":"NODE_LIFECYCLE_EVENT","sessionName":"session_abc"}]`)
	rec := callPersistEvents(t, ec, body)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// ---------- underDiskPressure ----------

func TestUnderDiskPressure_Threshold(t *testing.T) {
	ec := &EventCollector{maxDiskBytes: 100}
	ec.totalDiskUsed.Store(79)
	assert.False(t, ec.underDiskPressure())
	ec.totalDiskUsed.Store(80)
	assert.True(t, ec.underDiskPressure())

	ec2 := &EventCollector{maxDiskBytes: 0}
	ec2.totalDiskUsed.Store(1 << 30)
	assert.False(t, ec2.underDiskPressure(), "disabled when maxDiskBytes <= 0")
}

// ---------- processRotatedFile uploads gzip ----------

func TestProcessRotatedFile_GzipsAndUploadsThenCleansUp(t *testing.T) {
	writer := newMockWriter()
	ec := newTestCollector(t, writer, Options{CompressionEnabled: true})

	// Create a pre-rotated JSONL file.
	dir := filepath.Join(ec.dataDir, ec.clusterKey(), categoryNodeEvents)
	require.NoError(t, os.MkdirAll(dir, 0o755))
	jsonlPath := filepath.Join(dir, "node-1_123.jsonl")
	payload := []byte(`{"eventId":"a"}` + "\n" + `{"eventId":"b"}` + "\n")
	require.NoError(t, os.WriteFile(jsonlPath, payload, 0o600))
	ec.totalDiskUsed.Store(int64(len(payload)))

	task := rotationTask{
		path:        jsonlPath,
		category:    categoryNodeEvents,
		sessionName: ec.currentSessionName,
		nodeID:      "node-1",
		createdAt:   time.Date(2026, 5, 13, 10, 0, 0, 0, time.UTC),
		size:        int64(len(payload)),
	}
	ec.processRotatedFile(task)

	// Local files should be gone.
	_, err := os.Stat(jsonlPath)
	assert.True(t, os.IsNotExist(err))
	_, err = os.Stat(jsonlPath + ".gz")
	assert.True(t, os.IsNotExist(err))

	// Upload happened with gzip payload.
	expectedKey := fmt.Sprintf("history/cluster-history/raycluster/ns/cluster/session_abc/node-1/node_events/node-1-2026-05-13-10-%d.jsonl.gz", task.createdAt.UnixNano())
	raw, ok := writer.get(expectedKey)
	require.True(t, ok, "uploaded keys: %v", writer.fileKeys())

	gr, err := gzip.NewReader(bytes.NewReader(raw))
	require.NoError(t, err)
	defer gr.Close()
	decoded, err := io.ReadAll(gr)
	require.NoError(t, err)
	assert.Equal(t, string(payload), string(decoded))
}

func TestProcessRotatedFile_NoCompressionUsesJSONLExtension(t *testing.T) {
	writer := newMockWriter()
	ec := newTestCollector(t, writer, Options{CompressionEnabled: false})

	dir := filepath.Join(ec.dataDir, ec.clusterKey(), categoryNodeEvents)
	require.NoError(t, os.MkdirAll(dir, 0o755))
	jsonlPath := filepath.Join(dir, "node-1_123.jsonl")
	payload := []byte(`{"eventId":"a"}` + "\n")
	require.NoError(t, os.WriteFile(jsonlPath, payload, 0o600))

	task := rotationTask{
		path:        jsonlPath,
		category:    categoryNodeEvents,
		sessionName: ec.currentSessionName,
		nodeID:      "node-1",
		createdAt:   time.Date(2026, 5, 13, 10, 0, 0, 0, time.UTC),
		size:        int64(len(payload)),
	}
	ec.processRotatedFile(task)

	expectedKey := fmt.Sprintf("history/cluster-history/raycluster/ns/cluster/session_abc/node-1/node_events/node-1-2026-05-13-10-%d.jsonl", task.createdAt.UnixNano())
	raw, ok := writer.get(expectedKey)
	require.True(t, ok, "uploaded keys: %v", writer.fileKeys())
	assert.Equal(t, payload, raw)
}

// A resumed .jsonl.gz leftover must be uploaded as-is: gzipping it a second
// time would make the object undecodable for the history server.
func TestProcessRotatedFile_PrecompressedSkipsGzip(t *testing.T) {
	writer := newMockWriter()
	ec := newTestCollector(t, writer, Options{CompressionEnabled: true})

	dir := filepath.Join(ec.dataDir, ec.clusterKey(), categoryNodeEvents)
	require.NoError(t, os.MkdirAll(dir, 0o755))
	payload := []byte(`{"eventId":"a"}` + "\n")

	// Write a real gzip file, as a crashed prior run would have left behind.
	gzPath := filepath.Join(dir, "node-1_123.jsonl.gz")
	var buf bytes.Buffer
	gw := gzip.NewWriter(&buf)
	_, err := gw.Write(payload)
	require.NoError(t, err)
	require.NoError(t, gw.Close())
	require.NoError(t, os.WriteFile(gzPath, buf.Bytes(), 0o600))

	task := rotationTask{
		path:        gzPath,
		category:    categoryNodeEvents,
		sessionName: ec.currentSessionName,
		nodeID:      "node-1",
		createdAt:   time.Date(2026, 5, 13, 10, 0, 0, 0, time.UTC),
		size:        int64(buf.Len()),
	}
	ec.processRotatedFile(task)

	// Local file should be gone, and no double-compressed artifact left behind.
	_, err = os.Stat(gzPath)
	assert.True(t, os.IsNotExist(err))
	_, err = os.Stat(gzPath + ".gz")
	assert.True(t, os.IsNotExist(err))

	// Uploaded bytes must gunzip back to the original payload in one pass.
	expectedKey := fmt.Sprintf("history/cluster-history/raycluster/ns/cluster/session_abc/node-1/node_events/node-1-2026-05-13-10-%d.jsonl.gz", task.createdAt.UnixNano())
	raw, ok := writer.get(expectedKey)
	require.True(t, ok, "uploaded keys: %v", writer.fileKeys())
	gr, err := gzip.NewReader(bytes.NewReader(raw))
	require.NoError(t, err)
	defer gr.Close()
	decoded, err := io.ReadAll(gr)
	require.NoError(t, err)
	assert.Equal(t, string(payload), string(decoded))
}

// A write failure must not wedge the category: bufio errors are sticky, so
// the broken writer is evicted (rotated) and the next request gets a fresh
// file instead of failing until the timed rotation.
func TestPersistEvents_WriteFailureEvictsWedgedWriter(t *testing.T) {
	ec := newTestCollector(t, newMockWriter(), Options{})

	// Open an active file, then break it: close the fd and shrink the buffer
	// so the next write hits the closed fd immediately.
	state, err := ec.openNewActiveFileLocked(categoryNodeEvents, ec.currentSessionName)
	require.NoError(t, err)
	require.NoError(t, state.file.Close())
	state.writer = bufio.NewWriterSize(state.file, 1)

	event := func(id string) []byte {
		body, merr := json.Marshal([]map[string]any{{
			"eventId":     id,
			"eventType":   "NODE_LIFECYCLE_EVENT",
			"timestamp":   time.Now().UTC().Format(time.RFC3339Nano),
			"sessionName": ec.currentSessionName,
		}})
		require.NoError(t, merr)
		return body
	}

	// First request hits the broken writer and fails.
	rec := callPersistEvents(t, ec, event("e1"))
	assert.Equal(t, http.StatusInternalServerError, rec.Code)

	// The wedged state must be evicted so it cannot poison later requests.
	_, stillActive := ec.activeFiles[categoryNodeEvents]
	assert.False(t, stillActive, "wedged writer should be evicted from activeFiles")

	// Next request opens a fresh file and succeeds immediately.
	rec = callPersistEvents(t, ec, event("e2"))
	assert.Equal(t, http.StatusOK, rec.Code)
}

// A task whose upload failed before shutdown must still reach storage: the
// retry goroutine's final attempt is covered by consumerWG, and the queue is
// only closed after all producers are gone (pipeline shutdown ordering).
func TestShutdown_DrainsRetriedTasks(t *testing.T) {
	writer := newMockWriter()
	ec := newTestCollector(t, writer, Options{})

	dir := filepath.Join(ec.dataDir, ec.clusterKey(), categoryNodeEvents)
	require.NoError(t, os.MkdirAll(dir, 0o755))
	jsonlPath := filepath.Join(dir, "node-1_123.jsonl")
	payload := []byte(`{"eventId":"a"}` + "\n")
	require.NoError(t, os.WriteFile(jsonlPath, payload, 0o600))

	// First upload attempt fails, scheduling a backoff retry.
	writer.failNext = true

	task := rotationTask{
		path:        jsonlPath,
		category:    categoryNodeEvents,
		sessionName: ec.currentSessionName,
		nodeID:      "node-1",
		createdAt:   time.Date(2026, 5, 13, 10, 0, 0, 0, time.UTC),
		size:        int64(len(payload)),
	}

	// When this returns, retryProcess has already registered the backoff goroutine
	// on consumerWG, so shutdown below will wake it for a final attempt.
	ec.processRotatedFile(task)
	// Check that the injected failure was hit and nothing was uploaded.
	require.False(t, writer.failPending(), "first attempt should consume the injected failure")
	require.Empty(t, writer.fileKeys(), "first attempt must not have uploaded")

	// Run's real shutdown sequence. Closing stopProducers cuts the retry's
	// backoff short and triggers its final attempt.
	ec.shutdown()

	expectedKey := fmt.Sprintf("history/cluster-history/raycluster/ns/cluster/session_abc/node-1/node_events/node-1-2026-05-13-10-%d.jsonl", task.createdAt.UnixNano())
	raw, ok := writer.get(expectedKey)
	require.True(t, ok, "retried task never uploaded; keys: %v", writer.fileKeys())
	assert.Equal(t, payload, raw)

	_, err := os.Stat(jsonlPath)
	assert.True(t, os.IsNotExist(err), "local file should be removed after successful upload")
}

// A crash mid-compression leaves a partial gzip under a .tmp name. Resume
// must drop it and enqueue the intact .jsonl source.
func TestResumePendingFiles_DropsPartialGzipKeepsSource(t *testing.T) {
	ec := newTestCollector(t, newMockWriter(), Options{CompressionEnabled: true})

	dir := filepath.Join(ec.dataDir, ec.clusterKey(), "session_abc", categoryNodeEvents)
	require.NoError(t, os.MkdirAll(dir, 0o755))

	payload := []byte(`{"eventId":"a"}` + "\n")
	jsonlPath := filepath.Join(dir, "node-1_123.jsonl")
	require.NoError(t, os.WriteFile(jsonlPath, payload, 0o600))
	// Truncated gzip, the way a crash mid-compression leaves it.
	tmpPath := jsonlPath + ".gz.tmp"
	require.NoError(t, os.WriteFile(tmpPath, []byte("\x1f\x8b\x08trunc"), 0o600))

	ec.resumePendingFiles()

	// The partial gzip is gone; the valid source survived and was enqueued.
	_, err := os.Stat(tmpPath)
	assert.True(t, os.IsNotExist(err), "partial .tmp gzip should be removed")
	_, err = os.Stat(jsonlPath)
	require.NoError(t, err, "valid .jsonl source must survive")

	select {
	case task := <-ec.rotationQueue:
		assert.Equal(t, jsonlPath, task.path) //nolint:testifylint // encoded-compare false positive: jsonlPath is a filesystem path, not a JSON document.
	default:
		t.Fatal("expected the .jsonl to be enqueued for upload")
	}
}

// ---------- Rotation by size triggers on checkRotation ----------

func TestCheckRotation_SizeTrigger(t *testing.T) {
	writer := newMockWriter()
	ec := newTestCollector(t, writer, Options{
		RotationInterval: time.Hour, // ensure time won't trip
		MaxFileSizeBytes: 10,
	})

	state, err := ec.openNewActiveFileLocked(categoryNodeEvents, ec.currentSessionName)
	require.NoError(t, err)
	_, err = state.writer.Write(bytes.Repeat([]byte("x"), 50))
	require.NoError(t, err)
	require.NoError(t, state.writer.Flush())
	state.size = 50

	ec.checkRotation()

	// The category's active file must have been rotated away.
	_, stillActive := ec.activeFiles[categoryNodeEvents]
	assert.False(t, stillActive)

	// A rotation task must be queued.
	select {
	case task := <-ec.rotationQueue:
		assert.Equal(t, state.path, task.path)
	default:
		t.Fatal("expected rotation task to be enqueued")
	}
}

func TestCheckRotation_AgeTrigger(t *testing.T) {
	ec := newTestCollector(t, newMockWriter(), Options{
		RotationInterval: time.Hour,
		MaxFileSizeBytes: 1 << 30, // ensure size won't trip
	})

	state, err := ec.openNewActiveFileLocked(categoryNodeEvents, ec.currentSessionName)
	require.NoError(t, err)
	_, err = state.writer.Write([]byte("{}\n"))
	require.NoError(t, err)
	require.NoError(t, state.writer.Flush())
	state.size = 3

	// A young file below the size limit stays active.
	ec.checkRotation()
	_, stillActive := ec.activeFiles[categoryNodeEvents]
	require.True(t, stillActive, "a young, small file must not be rotated")

	// Once it is older than the rotation interval it is rotated and queued.
	state.createdAt = time.Now().Add(-2 * time.Hour)
	ec.checkRotation()
	_, stillActive = ec.activeFiles[categoryNodeEvents]
	assert.False(t, stillActive)
	select {
	case task := <-ec.rotationQueue:
		assert.Equal(t, state.path, task.path)
	default:
		t.Fatal("expected rotation task to be enqueued")
	}
}

// A node ID change is the collector's primary flush boundary: events written
// before it must be uploaded under the old node ID, later ones under the new
// one, and a no-op update must not rotate anything.
func TestUpdateNodeID_RotatesActiveFiles(t *testing.T) {
	writer := newMockWriter()
	ec := newTestCollector(t, writer, Options{CompressionEnabled: false})
	ec.consumerWG.Add(1)
	go ec.compressionUploadWorker()

	post := func(id string) {
		body, err := json.Marshal([]map[string]any{{
			"eventId":     id,
			"eventType":   "NODE_LIFECYCLE_EVENT",
			"timestamp":   time.Now().UTC().Format(time.RFC3339Nano),
			"sessionName": ec.currentSessionName,
		}})
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, callPersistEvents(t, ec, body).Code)
	}

	post("before")

	ec.UpdateNodeID("node-1") // same ID: nothing to do
	_, active := ec.activeFiles[categoryNodeEvents]
	require.True(t, active, "an unchanged node ID must not rotate the active file")

	ec.UpdateNodeID("node-2")
	_, active = ec.activeFiles[categoryNodeEvents]
	require.False(t, active, "a node ID change must rotate the active file")

	post("after")
	ec.shutdown()

	var oldNodeBody, newNodeBody string
	for _, key := range writer.fileKeys() {
		data, _ := writer.get(key)
		switch {
		case strings.Contains(key, "/node-1/node_events/"):
			oldNodeBody += string(data)
		case strings.Contains(key, "/node-2/node_events/"):
			newNodeBody += string(data)
		default:
			t.Fatalf("uploaded key %q is under neither node", key)
		}
	}
	assert.Contains(t, oldNodeBody, `"eventId":"before"`)
	assert.NotContains(t, oldNodeBody, `"eventId":"after"`)
	assert.Contains(t, newNodeBody, `"eventId":"after"`)
	assert.NotContains(t, newNodeBody, `"eventId":"before"`)
}

// Each rotation must land in its own object. A fixed key per session/node/hour
// (the old PutObject-per-flush layout) would let a later flush overwrite an
// earlier one.
func TestRotatedFilesInSameHourDoNotOverwrite(t *testing.T) {
	writer := newMockWriter()
	ec := newTestCollector(t, writer, Options{CompressionEnabled: false})
	ec.consumerWG.Add(1)
	go ec.compressionUploadWorker()

	rotate := func() {
		ec.writeMu.Lock()
		defer ec.writeMu.Unlock()
		require.NoError(t, ec.rotateFileLocked(categoryNodeEvents, false))
	}
	post := func(id string) {
		body, err := json.Marshal([]map[string]any{{
			"eventId":     id,
			"eventType":   "NODE_LIFECYCLE_EVENT",
			"timestamp":   time.Now().UTC().Format(time.RFC3339Nano),
			"sessionName": ec.currentSessionName,
		}})
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, callPersistEvents(t, ec, body).Code)
	}

	post("first")
	rotate()
	post("second")
	rotate()
	ec.shutdown()

	keys := writer.fileKeys()
	require.Len(t, keys, 2, "each rotation must produce its own object, got: %v", keys)

	var bodies []string
	for _, key := range keys {
		data, _ := writer.get(key)
		bodies = append(bodies, string(data))
	}
	joined := strings.Join(bodies, "")
	assert.Contains(t, joined, `"eventId":"first"`)
	assert.Contains(t, joined, `"eventId":"second"`)
}

// ---------- Sanity: atomic disk counter types ----------

func TestAtomicInt64Type(t *testing.T) {
	var x atomic.Int64
	x.Store(5)
	assert.Equal(t, int64(5), x.Load())
}
