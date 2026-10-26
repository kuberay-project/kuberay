package historyserver

import (
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ray-project/kuberay/historyserver/pkg/eventserver"
	eventtypes "github.com/ray-project/kuberay/historyserver/pkg/eventserver/types"
	"github.com/ray-project/kuberay/historyserver/pkg/utils"
)

type taskLogStorageReader struct {
	filename string
	content  string
	files    []string
}

func (*taskLogStorageReader) List() []utils.ClusterInfo { return nil }

func (r *taskLogStorageReader) GetContent(_ string, filename string) io.Reader {
	r.filename = filename
	return strings.NewReader(r.content)
}

func (r *taskLogStorageReader) ListFiles(_, _ string) []string { return r.files }

func TestTaskLogBasename(t *testing.T) {
	tests := map[string]struct {
		filename string
		want     string
	}{
		"absolute Ray path": {
			filename: "/tmp/ray/session_latest/logs/worker-abc-123.out",
			want:     "worker-abc-123.out",
		},
		"stored basename": {
			filename: "worker-abc-123.err",
			want:     "worker-abc-123.err",
		},
		"path traversal": {
			filename: "../../worker-abc-123.out",
			want:     "worker-abc-123.out",
		},
		"empty": {filename: "", want: ""},
		"directory": {
			filename: "/",
			want:     "",
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			if got := taskLogBasename(test.filename); got != test.want {
				t.Fatalf("taskLogBasename(%q) = %q, want %q", test.filename, got, test.want)
			}
		})
	}
}

func TestGetTaskLogFileUsesStoredBasename(t *testing.T) {
	const clusterSessionKey = "raycluster_default_session"
	loader := newTestLoader(t, &fakeProcessor{}, loaderTestConfig{cacheSize: 1})
	require.NoError(t, loader.putSnapshot(clusterSessionKey, &eventserver.SessionSnapshot{Tasks: []eventtypes.Task{{
		TaskID:      "task-id",
		TaskAttempt: 0,
		NodeID:      "node-id",
		TaskLogInfo: &eventtypes.TaskLogInfo{
			StdoutFile: "/tmp/ray/session_latest/logs/worker-abc-123.out",
			StdoutEnd:  int64(len("Processing 1\n")),
		},
	}}}))

	reader := &taskLogStorageReader{content: "Processing 1\n"}
	handler := &ServerHandler{sessionLoader: loader, reader: reader}
	content, err := handler._getNodeLogFile(
		clusterSessionKey,
		"cluster-prefix",
		"session",
		GetLogFileOptions{TaskID: "task-id", Suffix: "out", Lines: -1},
	)
	if err != nil {
		t.Fatalf("_getNodeLogFile() error = %v", err)
	}
	if got, want := string(content), "Processing 1\n"; got != want {
		t.Fatalf("_getNodeLogFile() content = %q, want %q", got, want)
	}
	if got, want := reader.filename, "session/node-id/logs/worker-abc-123.out"; got != want {
		t.Fatalf("storage filename = %q, want %q", got, want)
	}
}

func TestGetTaskLogFileAppliesRangeBeforeLineLimit(t *testing.T) {
	const clusterSessionKey = "raycluster_default_session"
	const content = "before\ntask line 1\ntask line 2\nafter\n"
	const taskContent = "task line 1\ntask line 2\n"
	start := int64(strings.Index(content, taskContent))
	end := start + int64(len(taskContent))

	loader := newTestLoader(t, &fakeProcessor{}, loaderTestConfig{cacheSize: 1})
	require.NoError(t, loader.putSnapshot(clusterSessionKey, &eventserver.SessionSnapshot{Tasks: []eventtypes.Task{{
		TaskID:      "task-id",
		TaskAttempt: 0,
		NodeID:      "node-id",
		WorkerID:    "worker-id",
		TaskLogInfo: &eventtypes.TaskLogInfo{
			StdoutFile:  "/tmp/ray/session_latest/logs/worker-abc-123.out",
			StdoutStart: start,
			StdoutEnd:   end,
		},
	}}}))

	reader := &taskLogStorageReader{content: content}
	handler := &ServerHandler{sessionLoader: loader, reader: reader}
	got, err := handler._getNodeLogFile(
		clusterSessionKey,
		"cluster-prefix",
		"session",
		GetLogFileOptions{TaskID: "task-id", Suffix: "out", Lines: 1},
	)
	if err != nil {
		t.Fatalf("_getNodeLogFile() error = %v", err)
	}
	if want := "task line 2"; string(got) != want {
		t.Fatalf("_getNodeLogFile() content = %q, want %q", got, want)
	}
}

func TestGetTaskLogFileRejectsIncompleteTaskLogInfo(t *testing.T) {
	const clusterSessionKey = "raycluster_default_session"
	loader := newTestLoader(t, &fakeProcessor{}, loaderTestConfig{cacheSize: 1})
	require.NoError(t, loader.putSnapshot(clusterSessionKey, &eventserver.SessionSnapshot{Tasks: []eventtypes.Task{{
		TaskID:      "task-id",
		TaskAttempt: 0,
		NodeID:      "aa",
		WorkerID:    "bb",
		TaskLogInfo: &eventtypes.TaskLogInfo{StdoutEnd: 20},
	}}}))

	reader := &taskLogStorageReader{
		content: "other task\ntarget task\n",
		files:   []string{"worker-bb-job-123.out"},
	}
	handler := &ServerHandler{sessionLoader: loader, reader: reader}
	_, err := handler._getNodeLogFile(
		clusterSessionKey,
		"cluster-prefix",
		"session",
		GetLogFileOptions{TaskID: "task-id", Suffix: "out", Lines: -1},
	)
	if err == nil {
		t.Fatal("_getNodeLogFile() should reject TaskLogInfo without a filename")
	}
	var httpErr *utils.HTTPError
	require.ErrorAs(t, err, &httpErr)
	assert.Equal(t, 404, httpErr.StatusCode())
	assert.Empty(t, reader.filename, "incomplete task metadata must not read a guessed worker log")
}

func TestGetTaskLogFileRejectsAmbiguousZeroEnd(t *testing.T) {
	const clusterSessionKey = "raycluster_default_session"
	loader := newTestLoader(t, &fakeProcessor{}, loaderTestConfig{cacheSize: 1})
	require.NoError(t, loader.putSnapshot(clusterSessionKey, &eventserver.SessionSnapshot{Tasks: []eventtypes.Task{{
		TaskID:      "task-id",
		TaskAttempt: 0,
		NodeID:      "node-id",
		TaskLogInfo: &eventtypes.TaskLogInfo{StdoutFile: "worker.out"},
	}}}))

	handler := &ServerHandler{sessionLoader: loader, reader: &taskLogStorageReader{content: "other task\n"}}
	_, err := handler._getNodeLogFile(
		clusterSessionKey,
		"cluster-prefix",
		"session",
		GetLogFileOptions{TaskID: "task-id", Suffix: "out", Lines: -1},
	)
	var httpErr *utils.HTTPError
	require.ErrorAs(t, err, &httpErr)
	assert.Equal(t, 404, httpErr.StatusCode())
}

func TestApplyLogByteRange(t *testing.T) {
	tests := map[string]struct {
		byteRange *logByteRange
		want      string
		wantErr   bool
	}{
		"unbounded":       {byteRange: nil, want: "0123456789"},
		"half-open range": {byteRange: &logByteRange{start: 2, end: 5}, want: "234"},
		"empty range":     {byteRange: &logByteRange{start: 4, end: 4}, want: ""},
		"end past EOF":    {byteRange: &logByteRange{start: 8, end: 20}, wantErr: true},
		"start past EOF":  {byteRange: &logByteRange{start: 20, end: 30}, wantErr: true},
		"negative start":  {byteRange: &logByteRange{start: -1, end: 2}, wantErr: true},
		"negative end":    {byteRange: &logByteRange{start: 0, end: -1}, wantErr: true},
		"reversed":        {byteRange: &logByteRange{start: 5, end: 4}, wantErr: true},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			reader, err := applyLogByteRange(strings.NewReader("0123456789"), test.byteRange)
			var got []byte
			if err == nil {
				got, err = io.ReadAll(reader)
			}
			if test.wantErr {
				assert.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, test.want, string(got))
		})
	}
}

func TestFormatTaskForResponseTaskLogInfo(t *testing.T) {
	task := eventtypes.Task{TaskLogInfo: &eventtypes.TaskLogInfo{
		StdoutFile:  "worker.out",
		StderrFile:  "worker.err",
		StdoutStart: 10,
		StdoutEnd:   20,
		StderrStart: 30,
		StderrEnd:   40,
	}}

	response := formatTaskForResponse(task, true)
	assert.Equal(t, map[string]any{
		"stdout_file":  "worker.out",
		"stderr_file":  "worker.err",
		"stdout_start": int64(10),
		"stdout_end":   int64(20),
		"stderr_start": int64(30),
		"stderr_end":   int64(40),
	}, response["task_log_info"])

	response = formatTaskForResponse(eventtypes.Task{}, true)
	assert.Nil(t, response["task_log_info"])
}
