package historyserver

import (
	"errors"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ray-project/kuberay/historyserver/pkg/eventserver"
	eventtypes "github.com/ray-project/kuberay/historyserver/pkg/eventserver/types"
	"github.com/ray-project/kuberay/historyserver/pkg/utils"
)

const (
	rotatedWorkerOut   = "worker-abc123-01000000-123.rotated.1788398100000000000-4390125.out"
	rotatedWorkerErr   = "worker-abc123-01000000-123.rotated.1788398100000000000-4390125.err"
	canonicalWorkerOut = "worker-abc123-01000000-123.out"
	rotatedRayletOut   = "raylet.rotated.1788398100000000000-4390126.out"
	// A rotated object whose inode happens to equal the pid under lookup: without
	// the exclusion this is what a pid search would match first.
	rotatedPidCollision = "worker-def456-01000000-99.rotated.1788398100000000000-123.out"
)

func TestResolvePidLogFilenameIgnoresRotatedObjects(t *testing.T) {
	tests := map[string]struct {
		files   []string
		want    string
		wantErr bool
	}{
		"canonical file wins over a preceding rotated object": {
			files: []string{rotatedPidCollision, canonicalWorkerOut},
			want:  canonicalWorkerOut,
		},
		"canonical file wins over a following rotated object": {
			files: []string{canonicalWorkerOut, rotatedPidCollision},
			want:  canonicalWorkerOut,
		},
		"only rotated objects is a miss": {
			files:   []string{rotatedPidCollision, rotatedWorkerOut},
			wantErr: true,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			handler := &ServerHandler{reader: &taskLogStorageReader{files: test.files}}
			_, filename, err := handler.resolvePidLogFilename("cluster-prefix", "session", "abcd", 123, "out")
			assertResolved(t, filename, err, test.want, test.wantErr)
		})
	}
}

func TestFindWorkerLogFileIgnoresRotatedObjects(t *testing.T) {
	tests := map[string]struct {
		files   []string
		suffix  string
		want    string
		wantErr bool
	}{
		"stdout prefers the canonical stream": {
			files:  []string{rotatedWorkerOut, canonicalWorkerOut},
			suffix: "out",
			want:   canonicalWorkerOut,
		},
		"stderr prefers the canonical stream": {
			files:  []string{rotatedWorkerErr, "worker-abc123-01000000-123.err"},
			suffix: "err",
			want:   "worker-abc123-01000000-123.err",
		},
		"only rotated generations is a miss": {
			files:   []string{rotatedWorkerOut, "worker-abc123-01000000-123.rotated.1788398200000000000-4390200.out"},
			suffix:  "out",
			wantErr: true,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			handler := &ServerHandler{reader: &taskLogStorageReader{files: test.files}}
			_, filename, err := handler.findWorkerLogFile("cluster-prefix", "session", "abcd", "abc123", test.suffix)
			assertResolved(t, filename, err, test.want, test.wantErr)
		})
	}
}

func TestResolveActorLogFilenameIgnoresRotatedObjects(t *testing.T) {
	const clusterSessionKey = "raycluster_default_session"
	loader := newTestLoader(t, &fakeProcessor{}, loaderTestConfig{cacheSize: 1})
	require.NoError(t, loader.putSnapshot(clusterSessionKey, &eventserver.SessionSnapshot{Actors: map[string]eventtypes.Actor{
		"actor-id": {
			ActorID: "actor-id",
			Address: eventtypes.Address{NodeID: "abcd", WorkerID: "abc123"},
		},
	}}))

	reader := &taskLogStorageReader{files: []string{rotatedWorkerOut, canonicalWorkerOut}}
	handler := &ServerHandler{sessionLoader: loader, reader: reader}
	_, filename, err := handler.resolveActorLogFilename(clusterSessionKey, "cluster-prefix", "session", "actor-id", "out")
	assertResolved(t, filename, err, canonicalWorkerOut, false)
}

// A task without task_log_info falls back to the worker file lookup, which must
// not land on a rotated generation either.
func TestResolveTaskLogFilenameFallbackIgnoresRotatedObjects(t *testing.T) {
	const clusterSessionKey = "raycluster_default_session"
	loader := newTestLoader(t, &fakeProcessor{}, loaderTestConfig{cacheSize: 1})
	require.NoError(t, loader.putSnapshot(clusterSessionKey, &eventserver.SessionSnapshot{Tasks: []eventtypes.Task{{
		TaskID:   "task-id",
		NodeID:   "abcd",
		WorkerID: "abc123",
	}}}))

	reader := &taskLogStorageReader{files: []string{rotatedWorkerOut, canonicalWorkerOut}}
	handler := &ServerHandler{sessionLoader: loader, reader: reader}
	_, filename, _, err := handler.resolveTaskLogFilename(clusterSessionKey, "cluster-prefix", "session", "task-id", 0, "out")
	assertResolved(t, filename, err, canonicalWorkerOut, false)
}

func TestGetNodeLogFileServesRotatedObjectByFilename(t *testing.T) {
	reader := &taskLogStorageReader{content: "rotated content\n"}
	handler := &ServerHandler{reader: reader}

	content, err := handler._getNodeLogFile("raycluster_default_session", "cluster-prefix", "session",
		GetLogFileOptions{NodeID: "abcd", Filename: rotatedWorkerOut, Suffix: "out", Lines: -1})
	if err != nil {
		t.Fatalf("_getNodeLogFile() error = %v", err)
	}
	if got, want := string(content), "rotated content\n"; got != want {
		t.Fatalf("content = %q, want %q", got, want)
	}
	if got, want := reader.filename, "session/abcd/logs/"+rotatedWorkerOut; got != want {
		t.Fatalf("storage filename = %q, want %q", got, want)
	}
}

// Rotated objects keep the active extension, so they stay in the same category
// as the stream they came from and remain visible in the log listing.
func TestCategorizeLogFilesIncludesRotatedObjects(t *testing.T) {
	got := categorizeLogFiles([]string{canonicalWorkerOut, rotatedWorkerOut, rotatedWorkerErr, rotatedRayletOut})

	want := map[string][]string{
		"worker_out": {canonicalWorkerOut, rotatedWorkerOut},
		"worker_err": {rotatedWorkerErr},
		"raylet":     {rotatedRayletOut},
	}
	if len(got) != len(want) {
		t.Fatalf("categorizeLogFiles() = %v, want %v", got, want)
	}
	for category, files := range want {
		if len(got[category]) != len(files) {
			t.Fatalf("category %q = %v, want %v", category, got[category], files)
		}
		for i, file := range files {
			if got[category][i] != file {
				t.Fatalf("category %q = %v, want %v", category, got[category], files)
			}
		}
	}
}

func TestIsRotatedLogName(t *testing.T) {
	tests := map[string]bool{
		rotatedWorkerOut:               true,
		rotatedRayletOut:               true,
		"old/" + rotatedWorkerErr:      true,
		canonicalWorkerOut:             false,
		"raylet.out":                   false,
		"rotated/worker-abc-1.out":     false,
		"python-core-worker-abc_1.log": false,
	}

	for name, want := range tests {
		if got := utils.IsRotatedLogName(name); got != want {
			t.Fatalf("IsRotatedLogName(%q) = %v, want %v", name, got, want)
		}
	}
}

func assertResolved(t *testing.T, filename string, err error, want string, wantErr bool) {
	t.Helper()
	if wantErr {
		if err == nil {
			t.Fatalf("resolved %q, want a not-found error", filename)
		}
		var httpErr *utils.HTTPError
		if !errors.As(err, &httpErr) || httpErr.StatusCode() != http.StatusNotFound {
			t.Fatalf("error = %v, want HTTP 404", err)
		}
		return
	}
	if err != nil {
		t.Fatalf("unexpected error = %v", err)
	}
	if filename != want {
		t.Fatalf("resolved %q, want %q", filename, want)
	}
}
