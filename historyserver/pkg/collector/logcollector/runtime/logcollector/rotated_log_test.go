package logcollector

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/ray-project/kuberay/historyserver/pkg/utils"
)

const (
	testSessionID = "session_2026-01-11_19-38-40_123456"
	testNodeID    = "node1"
	testLogPrefix = "root/cluster-history/raycluster/default/rc/" + testSessionID + "/" + testNodeID + "/logs/"
)

func newRotatedTestHandler(writer *MockStorageWriter) *RayLogHandler {
	return &RayLogHandler{
		Writer:              writer,
		RootDir:             "root",
		RayClusterName:      "rc",
		RayClusterNamespace: "default",
		RayNodeName:         testNodeID,
	}
}

// writeLogFile creates path with content and returns its rotated identity.
func writeLogFile(t *testing.T, path, content string) rotatedIdentity {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("MkdirAll(%s) = %v", filepath.Dir(path), err)
	}
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("WriteFile(%s) = %v", path, err)
	}
	return identityOf(t, path)
}

// openDescriptorCount reports how many descriptors this process holds. Names are
// read without stat'ing them, which /dev/fd does not support everywhere.
func openDescriptorCount(t *testing.T) int {
	t.Helper()
	dir, err := os.Open("/dev/fd")
	if err != nil {
		t.Skipf("cannot enumerate open descriptors: %v", err)
	}
	defer dir.Close()
	names, err := dir.Readdirnames(-1)
	if err != nil {
		t.Skipf("cannot enumerate open descriptors: %v", err)
	}
	return len(names)
}

func setModTime(t *testing.T, path string, nanos int64) {
	t.Helper()
	when := time.Unix(0, nanos)
	if err := os.Chtimes(path, when, when); err != nil {
		t.Fatalf("Chtimes(%s) = %v", path, err)
	}
}

func identityOf(t *testing.T, path string) rotatedIdentity {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Stat(%s) = %v", path, err)
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		t.Fatalf("Stat(%s): inode unavailable on this platform", path)
	}
	return rotatedIdentity{modTimeNs: info.ModTime().UnixNano(), inode: stat.Ino}
}

func TestRotationBaseName(t *testing.T) {
	tests := map[string]struct {
		name     string
		wantBase string
		wantOK   bool
	}{
		"first backup":       {name: "raylet.out.1", wantBase: "raylet.out", wantOK: true},
		"second backup":      {name: "raylet.out.2", wantBase: "raylet.out", wantOK: true},
		"multi digit backup": {name: "raylet.out.12", wantBase: "raylet.out", wantOK: true},
		"worker backup":      {name: "worker-abc123-01000000-123.err.3", wantBase: "worker-abc123-01000000-123.err", wantOK: true},
		"no extension base":  {name: "raylet.4", wantBase: "raylet", wantOK: true},
		"active out":         {name: "raylet.out"},
		"active err":         {name: "worker-abc123-01000000-123.err"},
		"no dot":             {name: "raylet"},
		"zero index":         {name: "raylet.out.0"},
		"leading zero index": {name: "raylet.out.01"},
		"trailing dot":       {name: "raylet.out."},
		"non numeric suffix": {name: "raylet.out.gz"},
		"leading dot only":   {name: ".1"},
		"already rotated":    {name: "raylet.rotated.1788398100000000000-42.out"},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			base, ok := rotationBaseName(test.name)
			if ok != test.wantOK || base != test.wantBase {
				t.Fatalf("rotationBaseName(%q) = (%q, %v), want (%q, %v)", test.name, base, ok, test.wantBase, test.wantOK)
			}
		})
	}
}

func TestRotationIndex(t *testing.T) {
	tests := map[string]struct {
		name string
		want int
	}{
		"first backup":       {name: "raylet.out.1", want: 1},
		"multi digit backup": {name: "raylet.out.12", want: 12},
		"no extension base":  {name: "raylet.4", want: 4},
		"active log":         {name: "raylet.out"},
		"zero index":         {name: "raylet.out.0"},
		"non numeric suffix": {name: "raylet.out.gz"},
		"overflowing index":  {name: "raylet.out.99999999999999999999999"},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			if got := rotationIndex(test.name); got != test.want {
				t.Fatalf("rotationIndex(%q) = %d, want %d", test.name, got, test.want)
			}
		})
	}
}

func TestRotatedLogName(t *testing.T) {
	tests := map[string]struct {
		name string
		id   rotatedIdentity
		want string
	}{
		"worker stdout": {
			name: "worker-abc123-01000000-123.out.1",
			id:   rotatedIdentity{modTimeNs: 1788398123456789012, inode: 4390125},
			want: "worker-abc123-01000000-123.rotated.1788398123456789012-4390125.out",
		},
		"worker stderr": {
			name: "worker-abc123-01000000-123.err.5",
			id:   rotatedIdentity{modTimeNs: 17, inode: 4390125},
			want: "worker-abc123-01000000-123.rotated.17-4390125.err",
		},
		"component stdout": {
			name: "raylet.out.2",
			id:   rotatedIdentity{modTimeNs: 99, inode: 4390126},
			want: "raylet.rotated.99-4390126.out",
		},
		"dot log": {
			name: "python-core-worker-abc_123.log.3",
			id:   rotatedIdentity{modTimeNs: 11, inode: 7},
			want: "python-core-worker-abc_123.rotated.11-7.log",
		},
		"no extension": {
			name: "raylet.4",
			id:   rotatedIdentity{modTimeNs: 13, inode: 11},
			want: "raylet.rotated.13-11",
		},
		"dotfile base": {
			name: ".out.1",
			id:   rotatedIdentity{modTimeNs: 15, inode: 13},
			want: ".out.rotated.15-13",
		},
		"not a rotation backup": {name: "raylet.out", id: rotatedIdentity{modTimeNs: 1, inode: 1}},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			got, ok := rotatedLogName(test.name, test.id)
			if got != test.want || ok != (test.want != "") {
				t.Fatalf("rotatedLogName(%q, %v) = (%q, %v), want %q", test.name, test.id, got, ok, test.want)
			}
		})
	}
}

// Linux reuses the inode of a generation it drops from the ring, so without the
// modification time the next generation would share its object name.
func TestRotatedLogNameSeparatesReusedInode(t *testing.T) {
	const backupName = "worker-abc123-01000000-123.out.1"
	first := rotatedIdentity{modTimeNs: 1788398100000000000, inode: 4390125}
	reused := rotatedIdentity{modTimeNs: 1788398200000000000, inode: first.inode}

	firstName, _ := rotatedLogName(backupName, first)
	reusedName, _ := rotatedLogName(backupName, reused)
	if firstName == reusedName {
		t.Fatalf("inode reuse collides on %q", firstName)
	}
}

// One generation keeps its identity as Ray renames it down the ring, so every
// index must map to a single object name.
func TestRotatedLogNameIsStableAcrossRotationIndex(t *testing.T) {
	id := rotatedIdentity{modTimeNs: 1788398100000000000, inode: 4390125}
	want := "raylet.rotated.1788398100000000000-4390125.out"
	for _, backupName := range []string{"raylet.out.1", "raylet.out.2", "raylet.out.3"} {
		if got, _ := rotatedLogName(backupName, id); got != want {
			t.Fatalf("rotatedLogName(%q) = %q, want %q", backupName, got, want)
		}
	}
}

func TestCollectRotatedLogUploadsDeterministicObject(t *testing.T) {
	logsDir := t.TempDir()
	writer := NewMockStorageWriter()
	handler := newRotatedTestHandler(writer)

	backup := filepath.Join(logsDir, "raylet.out.1")
	id := writeLogFile(t, backup, "rotated raylet")
	nested := filepath.Join(logsDir, "old", "worker-abc123-01000000-123.err.2")
	nestedID := writeLogFile(t, nested, "rotated worker")
	active := filepath.Join(logsDir, "raylet.out")
	writeLogFile(t, active, "active raylet")

	handler.collectRotatedLogsUnder(logsDir, testSessionID, testNodeID, nil)

	want := map[string]string{
		testLogPrefix + mustRotatedName(t, "raylet.out.1", id):                                    "rotated raylet",
		testLogPrefix + "old/" + mustRotatedName(t, "worker-abc123-01000000-123.err.2", nestedID): "rotated worker",
	}
	assertWritten(t, writer, want)
}

func TestCollectRotatedLogSkipsActiveFiles(t *testing.T) {
	logsDir := t.TempDir()
	writer := NewMockStorageWriter()
	handler := newRotatedTestHandler(writer)

	for _, name := range []string{"raylet.out", "raylet.err", "monitor.log", "worker-abc-01000000-1.out"} {
		writeLogFile(t, filepath.Join(logsDir, name), "active")
	}

	handler.collectRotatedLogsUnder(logsDir, testSessionID, testNodeID, nil)
	assertWritten(t, writer, map[string]string{})
}

// A generation keeps its inode as Ray shifts it down the rotation ring, so it
// must not be uploaded a second time under a different index.
func TestCollectRotatedLogIgnoresRotationIndexChange(t *testing.T) {
	logsDir := t.TempDir()
	writer := NewMockStorageWriter()
	handler := newRotatedTestHandler(writer)

	first := filepath.Join(logsDir, "raylet.out.1")
	id := writeLogFile(t, first, "generation one")
	handler.collectRotatedLogsUnder(logsDir, testSessionID, testNodeID, nil)

	second := filepath.Join(logsDir, "raylet.out.2")
	if err := os.Rename(first, second); err != nil {
		t.Fatalf("Rename() = %v", err)
	}
	handler.collectRotatedLogsUnder(logsDir, testSessionID, testNodeID, nil)

	assertWritten(t, writer, map[string]string{
		testLogPrefix + mustRotatedName(t, "raylet.out.1", id): "generation one",
	})
}

// Ray reuses .1 for the next generation, often on the inode it just evicted, so
// the two generations must still reach distinct objects.
func TestCollectRotatedLogUploadsNewGenerationReusingIndex(t *testing.T) {
	logsDir := t.TempDir()
	writer := NewMockStorageWriter()
	handler := newRotatedTestHandler(writer)

	// Equal-length content models rotation at a fixed byte threshold.
	const firstContent, secondContent = "generation one", "generation two"
	backup := filepath.Join(logsDir, "raylet.out.1")

	writeLogFile(t, backup, firstContent)
	setModTime(t, backup, 1788398100000000000)
	firstID := identityOf(t, backup)
	handler.collectRotatedLogsUnder(logsDir, testSessionID, testNodeID, nil)

	if err := os.Remove(backup); err != nil {
		t.Fatalf("Remove() = %v", err)
	}
	writeLogFile(t, backup, secondContent)
	setModTime(t, backup, 1788398200000000000)
	secondID := identityOf(t, backup)
	handler.collectRotatedLogsUnder(logsDir, testSessionID, testNodeID, nil)

	if firstID.inode == secondID.inode {
		t.Logf("filesystem reused inode %d, exercising the collision directly", firstID.inode)
	}
	assertWritten(t, writer, map[string]string{
		testLogPrefix + mustRotatedName(t, "raylet.out.1", firstID):  firstContent,
		testLogPrefix + mustRotatedName(t, "raylet.out.1", secondID): secondContent,
	})
}

// Ray evicts the highest rotation index next, so that backup must upload first.
func TestCollectRotatedLogsUploadsHighestIndexFirst(t *testing.T) {
	logsDir := t.TempDir()
	writer := NewMockStorageWriter()
	handler := newRotatedTestHandler(writer)

	ids := make(map[int]rotatedIdentity)
	// Written out of order, and lexically ascending, so only the sort can order them.
	for _, index := range []int{1, 5, 3} {
		name := fmt.Sprintf("foo.out.%d", index)
		ids[index] = writeLogFile(t, filepath.Join(logsDir, name), name)
	}

	handler.collectRotatedLogsUnder(logsDir, testSessionID, testNodeID, nil)

	want := []string{
		testLogPrefix + mustRotatedName(t, "foo.out.5", ids[5]),
		testLogPrefix + mustRotatedName(t, "foo.out.3", ids[3]),
		testLogPrefix + mustRotatedName(t, "foo.out.1", ids[1]),
	}
	if got := writer.order(); !slices.Equal(got, want) {
		t.Fatalf("upload order = %v, want %v", got, want)
	}
}

// The scan must give up promptly once shutdown starts; the shutdown collection
// picks up whatever it left behind.
func TestCollectRotatedLogsStopsBeforeNextCandidate(t *testing.T) {
	logsDir := t.TempDir()
	writer := NewMockStorageWriter()
	handler := newRotatedTestHandler(writer)

	for _, index := range []int{1, 2, 3} {
		name := fmt.Sprintf("foo.out.%d", index)
		writeLogFile(t, filepath.Join(logsDir, name), name)
	}

	stop := make(chan struct{})
	var stopOnce sync.Once
	writer.beforeWrite = func() { stopOnce.Do(func() { close(stop) }) }

	before := openDescriptorCount(t)
	handler.collectRotatedLogsUnder(logsDir, testSessionID, testNodeID, stop)

	if got := writer.order(); len(got) != 1 {
		t.Fatalf("uploaded %v after stop, want only the in-flight candidate", got)
	}
	if after := openDescriptorCount(t); after > before {
		t.Fatalf("open descriptors grew from %d to %d", before, after)
	}

	// Shutdown collection ignores stop and still reaches the skipped candidates.
	objectPrefix := handler.rotatedObjectPrefix(testSessionID, testNodeID)
	for _, index := range []int{1, 2, 3} {
		if _, err := handler.collectIfRotatedLog(filepath.Join(logsDir, fmt.Sprintf("foo.out.%d", index)), logsDir, objectPrefix); err != nil {
			t.Fatalf("collectIfRotatedLog() = %v", err)
		}
	}
	if got := writer.order(); len(got) != 3 {
		t.Fatalf("uploaded %v after shutdown collection, want all three", got)
	}
}

// Descriptors must not leak on the success, failure or already-uploaded paths.
func TestCollectRotatedLogsClosesEveryDescriptor(t *testing.T) {
	logsDir := t.TempDir()
	writer := NewMockStorageWriter()
	handler := newRotatedTestHandler(writer)

	objectPrefix := handler.rotatedObjectPrefix(testSessionID, testNodeID)
	writeLogFile(t, filepath.Join(logsDir, "a-stream.out.1"), "uploaded")

	before := openDescriptorCount(t)

	// Success, then a pass where the generation is already uploaded.
	handler.collectRotatedLogsUnder(logsDir, testSessionID, testNodeID, nil)
	handler.collectRotatedLogsUnder(logsDir, testSessionID, testNodeID, nil)

	// A generation not uploaded yet, so both walkers reach the failing WriteFile.
	failing := filepath.Join(logsDir, "b-stream.out.1")
	writeLogFile(t, failing, "upload fails")
	errUnavailable := errors.New("object store unavailable")
	writer.setWriteErr(errUnavailable)
	var failedWrites int
	writer.beforeWrite = func() { failedWrites++ }
	handler.collectRotatedLogsUnder(logsDir, testSessionID, testNodeID, nil)
	if _, err := handler.collectIfRotatedLog(failing, logsDir, objectPrefix); !errors.Is(err, errUnavailable) {
		t.Fatalf("collectIfRotatedLog() = %v, want %v", err, errUnavailable)
	}
	if failedWrites != 2 {
		t.Fatalf("WriteFile failed %d times, want 2", failedWrites)
	}

	writer.beforeWrite = nil
	writer.setWriteErr(nil)
	if _, err := handler.collectIfRotatedLog(failing, logsDir, objectPrefix); err != nil {
		t.Fatalf("collectIfRotatedLog() = %v", err)
	}

	if after := openDescriptorCount(t); after > before {
		t.Fatalf("open descriptors grew from %d to %d", before, after)
	}
}

func TestCollectRotatedLogRetriesAfterWriteFailure(t *testing.T) {
	logsDir := t.TempDir()
	writer := NewMockStorageWriter()
	writer.setWriteErr(errors.New("object store unavailable"))
	handler := newRotatedTestHandler(writer)

	backup := filepath.Join(logsDir, "raylet.out.1")
	id := writeLogFile(t, backup, "rotated raylet")

	handler.collectRotatedLogsUnder(logsDir, testSessionID, testNodeID, nil)
	assertWritten(t, writer, map[string]string{})

	writer.setWriteErr(nil)
	handler.collectRotatedLogsUnder(logsDir, testSessionID, testNodeID, nil)
	assertWritten(t, writer, map[string]string{
		testLogPrefix + mustRotatedName(t, "raylet.out.1", id): "rotated raylet",
	})
}

// The upload must keep reading from the descriptor it opened, even once Ray has
// removed the path it came from.
func TestCollectRotatedLogReadsThroughUnlinkedPath(t *testing.T) {
	logsDir := t.TempDir()
	writer := NewMockStorageWriter()
	handler := newRotatedTestHandler(writer)

	backup := filepath.Join(logsDir, "raylet.out.1")
	id := writeLogFile(t, backup, "rotated raylet")
	writer.beforeWrite = func() {
		if err := os.Remove(backup); err != nil {
			t.Errorf("Remove() = %v", err)
		}
	}

	handler.collectRotatedLogsUnder(logsDir, testSessionID, testNodeID, nil)
	assertWritten(t, writer, map[string]string{
		testLogPrefix + mustRotatedName(t, "raylet.out.1", id): "rotated raylet",
	})
}

func TestCollectRotatedLogToleratesVanishedPath(t *testing.T) {
	logsDir := t.TempDir()
	writer := NewMockStorageWriter()
	handler := newRotatedTestHandler(writer)

	objectPrefix := handler.rotatedObjectPrefix(testSessionID, testNodeID)
	handled, err := handler.collectIfRotatedLog(filepath.Join(logsDir, "raylet.out.1"), logsDir, objectPrefix)
	if !handled || err != nil {
		t.Fatalf("collectIfRotatedLog() = (%v, %v), want (true, nil) for a vanished rotation backup", handled, err)
	}
	assertWritten(t, writer, map[string]string{})
}

func TestCollectRotatedLogAttributesSessionAndNode(t *testing.T) {
	logsDir := t.TempDir()
	writer := NewMockStorageWriter()
	handler := newRotatedTestHandler(writer)

	backup := filepath.Join(logsDir, "raylet.out.1")
	id := writeLogFile(t, backup, "rotated raylet")

	handler.collectRotatedLogsUnder(logsDir, "session-old", "node-old", nil)
	handler.collectRotatedLogsUnder(logsDir, "session-new", "node-new", nil)

	name := mustRotatedName(t, "raylet.out.1", id)
	assertWritten(t, writer, map[string]string{
		"root/cluster-history/raycluster/default/rc/session-old/node-old/logs/" + name: "rotated raylet",
		"root/cluster-history/raycluster/default/rc/session-new/node-new/logs/" + name: "rotated raylet",
	})
}

func TestCollectRotatedLogSkipsUnknownSessionOrNode(t *testing.T) {
	logsDir := t.TempDir()
	writer := NewMockStorageWriter()
	handler := newRotatedTestHandler(writer)
	writeLogFile(t, filepath.Join(logsDir, "raylet.out.1"), "rotated raylet")

	handler.collectRotatedLogsUnder(logsDir, testSessionID, "", nil)
	handler.collectRotatedLogsUnder(logsDir, "", testNodeID, nil)
	assertWritten(t, writer, map[string]string{})
}

// A restarted collector has an empty uploaded set, so it re-uploads what is still
// on disk; the object key is unchanged, which keeps the re-upload idempotent.
func TestCollectRotatedLogRestartKeepsObjectName(t *testing.T) {
	logsDir := t.TempDir()
	backup := filepath.Join(logsDir, "raylet.out.1")
	id := writeLogFile(t, backup, "rotated raylet")
	want := map[string]string{testLogPrefix + mustRotatedName(t, "raylet.out.1", id): "rotated raylet"}

	beforeRestart := NewMockStorageWriter()
	newRotatedTestHandler(beforeRestart).collectRotatedLogsUnder(logsDir, testSessionID, testNodeID, nil)
	assertWritten(t, beforeRestart, want)

	afterRestart := NewMockStorageWriter()
	newRotatedTestHandler(afterRestart).collectRotatedLogsUnder(logsDir, testSessionID, testNodeID, nil)
	assertWritten(t, afterRestart, want)
}

func TestCollectRotatedLogIsUploadedOnceUnderConcurrency(t *testing.T) {
	logsDir := t.TempDir()
	writer := NewMockStorageWriter()
	handler := newRotatedTestHandler(writer)

	backup := filepath.Join(logsDir, "raylet.out.1")
	id := writeLogFile(t, backup, "rotated raylet")

	var uploads int
	var mu sync.Mutex
	writer.beforeWrite = func() {
		mu.Lock()
		uploads++
		mu.Unlock()
	}

	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			handler.collectRotatedLogsUnder(logsDir, testSessionID, testNodeID, nil)
		})
	}
	wg.Wait()

	mu.Lock()
	defer mu.Unlock()
	if uploads != 1 {
		t.Fatalf("WriteFile called %d times, want 1", uploads)
	}
	assertWritten(t, writer, map[string]string{
		testLogPrefix + mustRotatedName(t, "raylet.out.1", id): "rotated raylet",
	})
}

// The first scan must not wait for the ticker: a collector restarting under a
// live Ray node would otherwise miss a whole interval of rotations.
func TestScanRotatedLogsScansBeforeFirstTick(t *testing.T) {
	rayRoot := t.TempDir()
	t.Setenv("RAY_TMP_ROOT", rayRoot)
	logsDir := linkSessionLatest(t, rayRoot, testSessionID)
	id := writeLogFile(t, filepath.Join(logsDir, "raylet.out.1"), "rotated raylet")

	writer := NewMockStorageWriter()
	handler := newRotatedTestHandler(writer)
	// Long enough that only the immediate scan can produce the upload.
	handler.RotatedLogScanInterval = time.Hour

	stop := make(chan struct{})
	defer close(stop)
	go handler.scanRotatedLogs(stop)

	deadline := time.Now().Add(10 * time.Second)
	for len(writer.written()) == 0 {
		if time.Now().After(deadline) {
			t.Fatal("no rotated log uploaded before the first ticker interval")
		}
		time.Sleep(10 * time.Millisecond)
	}
	assertWritten(t, writer, map[string]string{
		testLogPrefix + mustRotatedName(t, "raylet.out.1", id): "rotated raylet",
	})
}

func TestScanRotatedLogsStopsOnSignal(t *testing.T) {
	t.Setenv("RAY_TMP_ROOT", t.TempDir())
	handler := newRotatedTestHandler(NewMockStorageWriter())
	handler.RotatedLogScanInterval = time.Millisecond

	stop := make(chan struct{})
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		handler.scanRotatedLogs(stop)
	}()

	close(stop)
	select {
	case <-stopped:
	case <-time.After(5 * time.Second):
		t.Fatal("scanRotatedLogs did not exit after stop")
	}
}

// Shutdown is the first pass to see this generation, so it must still use the
// deterministic name rather than the raw rotation index.
func TestProcessSessionLatestLogsUsesRotatedName(t *testing.T) {
	rayRoot := t.TempDir()
	t.Setenv("RAY_TMP_ROOT", rayRoot)
	logsDir := linkSessionLatest(t, rayRoot, testSessionID)

	id := writeLogFile(t, filepath.Join(logsDir, "raylet.out.1"), "rotated raylet")
	writeLogFile(t, filepath.Join(logsDir, "raylet.out"), "active raylet")

	writer := NewMockStorageWriter()
	handler := newRotatedTestHandler(writer)
	handler.processSessionLatestLogs()

	assertWritten(t, writer, map[string]string{
		testLogPrefix + mustRotatedName(t, "raylet.out.1", id): "rotated raylet",
		testLogPrefix + "raylet.out":                           "active raylet",
	})
}

func TestProcessSessionLatestLogsSkipsAlreadyUploadedRotation(t *testing.T) {
	rayRoot := t.TempDir()
	t.Setenv("RAY_TMP_ROOT", rayRoot)
	logsDir := linkSessionLatest(t, rayRoot, testSessionID)
	id := writeLogFile(t, filepath.Join(logsDir, "raylet.out.1"), "rotated raylet")

	writer := NewMockStorageWriter()
	handler := newRotatedTestHandler(writer)
	handler.collectRotatedLogsUnder(logsDir, testSessionID, testNodeID, nil)

	var uploadsAfterScan int
	writer.beforeWrite = func() { uploadsAfterScan++ }
	handler.processSessionLatestLogs()

	if uploadsAfterScan != 0 {
		t.Fatalf("shutdown uploaded %d objects, want 0", uploadsAfterScan)
	}
	assertWritten(t, writer, map[string]string{
		testLogPrefix + mustRotatedName(t, "raylet.out.1", id): "rotated raylet",
	})
}

func TestProcessPrevLogsDirUsesRotatedName(t *testing.T) {
	rayRoot := t.TempDir()
	t.Setenv("RAY_TMP_ROOT", rayRoot)

	nodeDir := filepath.Join(rayRoot, "prev-logs", testSessionID, testNodeID)
	logsDir := filepath.Join(nodeDir, utils.RAY_SESSIONDIR_LOGDIR_NAME)
	id := writeLogFile(t, filepath.Join(logsDir, "raylet.out.1"), "rotated raylet")
	writeLogFile(t, filepath.Join(logsDir, "raylet.out"), "active raylet")

	writer := NewMockStorageWriter()
	handler := newRotatedTestHandler(writer)
	handler.prevLogsDir = utils.GetRayPrevLogsPath()
	handler.persistCompleteLogsDir = utils.GetRayPersistCompletePath()
	handler.processPrevLogsDir(nodeDir)

	assertWritten(t, writer, map[string]string{
		testLogPrefix + mustRotatedName(t, "raylet.out.1", id): "rotated raylet",
		testLogPrefix + "raylet.out":                           "active raylet",
	})
}

// prev-logs holds the only copy of a previous session's rotated log, so a failed
// upload must leave the directory for a later pass to retry.
func TestProcessPrevLogsDirKeepsDirectoryAfterRotatedUploadFailure(t *testing.T) {
	rayRoot := t.TempDir()
	t.Setenv("RAY_TMP_ROOT", rayRoot)

	nodeDir := filepath.Join(rayRoot, "prev-logs", testSessionID, testNodeID)
	backup := filepath.Join(nodeDir, utils.RAY_SESSIONDIR_LOGDIR_NAME, "raylet.out.1")
	id := writeLogFile(t, backup, "rotated raylet")
	object := testLogPrefix + mustRotatedName(t, "raylet.out.1", id)

	writer := NewMockStorageWriter()
	handler := newRotatedTestHandler(writer)
	handler.prevLogsDir = utils.GetRayPrevLogsPath()
	handler.persistCompleteLogsDir = utils.GetRayPersistCompletePath()

	var failedWrites int
	writer.beforeWrite = func() { failedWrites++ }
	writer.setWriteErr(errors.New("object store unavailable"))
	handler.processPrevLogsDir(nodeDir)

	if failedWrites != 1 {
		t.Fatalf("WriteFile failed %d times, want 1", failedWrites)
	}
	assertWritten(t, writer, map[string]string{})
	for _, kept := range []string{nodeDir, backup} {
		if _, err := os.Stat(kept); err != nil {
			t.Fatalf("Stat(%s) after failed upload = %v, want it kept", kept, err)
		}
	}

	writer.beforeWrite = nil
	writer.setWriteErr(nil)
	handler.processPrevLogsDir(nodeDir)

	assertWritten(t, writer, map[string]string{object: "rotated raylet"})
	if got := writeCount(writer, object); got != 1 {
		t.Fatalf("wrote %q %d times, want 1", object, got)
	}
	if _, err := os.Stat(nodeDir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("Stat(%s) after retry = %v, want it removed", nodeDir, err)
	}
}

// The uploaded set must not grow for the lifetime of the collector: an entry is
// kept while its generation is on disk and dropped once Ray evicts it.
func TestCollectRotatedLogsPrunesEvictedGenerations(t *testing.T) {
	logsDir := t.TempDir()
	writer := NewMockStorageWriter()
	handler := newRotatedTestHandler(writer)

	backup := filepath.Join(logsDir, "raylet.out.1")
	id := writeLogFile(t, backup, "generation one")
	object := testLogPrefix + mustRotatedName(t, "raylet.out.1", id)

	handler.collectRotatedLogsUnder(logsDir, testSessionID, testNodeID, nil)
	assertRotatedUploaded(t, handler, []string{object})

	// Still on disk, so the entry survives and the object is not written again.
	handler.collectRotatedLogsUnder(logsDir, testSessionID, testNodeID, nil)
	assertRotatedUploaded(t, handler, []string{object})
	if got := writer.order(); len(got) != 1 {
		t.Fatalf("wrote %v, want a single upload", got)
	}

	if err := os.Remove(backup); err != nil {
		t.Fatalf("Remove() = %v", err)
	}
	handler.collectRotatedLogsUnder(logsDir, testSessionID, testNodeID, nil)
	assertRotatedUploaded(t, handler, nil)
}

// A walk that could not read the directory says nothing about which generations
// Ray still holds, so it must leave the uploaded set intact.
func TestCollectRotatedLogsKeepsStateAfterIncompleteWalk(t *testing.T) {
	logsDir := t.TempDir()
	writer := NewMockStorageWriter()
	handler := newRotatedTestHandler(writer)

	id := writeLogFile(t, filepath.Join(logsDir, "raylet.out.1"), "generation one")
	handler.collectRotatedLogsUnder(logsDir, testSessionID, testNodeID, nil)
	object := testLogPrefix + mustRotatedName(t, "raylet.out.1", id)
	assertRotatedUploaded(t, handler, []string{object})

	handler.collectRotatedLogsUnder(filepath.Join(logsDir, "gone"), testSessionID, testNodeID, nil)
	assertRotatedUploaded(t, handler, []string{object})
}

// A session change splits rotated-log collection across two paths: the active
// scan of S2 and the prev-logs pass over S1. Pruning must stay inside the
// session it scanned, or the S1 entries vanish and prev-logs writes them again.
func TestScanOfNewSessionKeepsPrevSessionUploads(t *testing.T) {
	rayRoot := t.TempDir()
	t.Setenv("RAY_TMP_ROOT", rayRoot)

	const (
		sessionOne = "session_2026-01-11_19-38-40_000001"
		sessionTwo = "session_2026-01-11_20-15-02_000002"
	)

	writer := NewMockStorageWriter()
	handler := newRotatedTestHandler(writer)
	handler.prevLogsDir = utils.GetRayPrevLogsPath()
	handler.persistCompleteLogsDir = utils.GetRayPersistCompletePath()

	// S1 is the active session and its rotation backup is uploaded.
	logsOne := filepath.Join(rayRoot, sessionOne, utils.RAY_SESSIONDIR_LOGDIR_NAME)
	idOne := writeLogFile(t, filepath.Join(logsOne, "raylet.out.1"), "s1 rotated")
	pointSessionLatest(t, rayRoot, sessionOne)
	handler.collectActiveSessionRotatedLogs(nil)

	objectOne := logsPrefixOf(sessionOne) + mustRotatedName(t, "raylet.out.1", idOne)
	assertRotatedUploaded(t, handler, []string{objectOne})

	// The session rolls over: S1's logs move to prev-logs and S2 starts rotating.
	sessionTwoDir := filepath.Join(rayRoot, sessionTwo)
	logsTwo := filepath.Join(sessionTwoDir, utils.RAY_SESSIONDIR_LOGDIR_NAME)
	idTwo := writeLogFile(t, filepath.Join(logsTwo, "raylet.out.1"), "s2 rotated")
	pointSessionLatest(t, rayRoot, sessionTwo)
	if err := utils.MoveLeftoverSessionLogs(sessionTwoDir, testNodeID); err != nil {
		t.Fatalf("MoveLeftoverSessionLogs() = %v", err)
	}

	handler.collectActiveSessionRotatedLogs(nil)
	objectTwo := logsPrefixOf(sessionTwo) + mustRotatedName(t, "raylet.out.1", idTwo)
	assertRotatedUploaded(t, handler, []string{objectOne, objectTwo})

	// S1 is already on the object store, so the prev-logs pass must not write it
	// a second time.
	handler.processPrevLogsDir(filepath.Join(rayRoot, "prev-logs", sessionOne, testNodeID))
	if got := writeCount(writer, objectOne); got != 1 {
		t.Fatalf("wrote %q %d times, want 1 (order: %v)", objectOne, got, writer.order())
	}

	// prev-logs removed the directory, so S1 can never be seen again and its
	// entries would otherwise leak for the collector's lifetime.
	assertRotatedUploaded(t, handler, []string{objectTwo})
}

// Object keys are compared by path segment so one node's scan cannot prune a
// node whose ID it merely prefixes.
func TestPruneRotatedUploadedIsScopedToNode(t *testing.T) {
	writer := NewMockStorageWriter()
	handler := newRotatedTestHandler(writer)

	logsOne, logsTen := t.TempDir(), t.TempDir()
	idOne := writeLogFile(t, filepath.Join(logsOne, "raylet.out.1"), "node1 rotated")
	writeLogFile(t, filepath.Join(logsTen, "raylet.out.1"), "node10 rotated")

	handler.collectRotatedLogsUnder(logsOne, testSessionID, "node1", nil)
	handler.collectRotatedLogsUnder(logsTen, testSessionID, "node10", nil)

	// node10 loses its generation; node1 keeps its own.
	if err := os.Remove(filepath.Join(logsTen, "raylet.out.1")); err != nil {
		t.Fatalf("Remove() = %v", err)
	}
	handler.collectRotatedLogsUnder(logsTen, testSessionID, "node10", nil)

	nodeOnePrefix := "root/cluster-history/raycluster/default/rc/" + testSessionID + "/node1/logs/"
	assertRotatedUploaded(t, handler, []string{nodeOnePrefix + mustRotatedName(t, "raylet.out.1", idOne)})
}

// An empty prefix would scope to "/", which covers every key under an absolute root.
func TestPruneRotatedUploadedIgnoresEmptyPrefix(t *testing.T) {
	logsDir := t.TempDir()
	handler := newRotatedTestHandler(NewMockStorageWriter())
	handler.RootDir = "/root"

	id := writeLogFile(t, filepath.Join(logsDir, "raylet.out.1"), "rotated raylet")
	handler.collectRotatedLogsUnder(logsDir, testSessionID, testNodeID, nil)
	want := []string{"/" + testLogPrefix + mustRotatedName(t, "raylet.out.1", id)}
	assertRotatedUploaded(t, handler, want)

	handler.pruneRotatedUploaded("", nil)
	assertRotatedUploaded(t, handler, want)
}

func logsPrefixOf(sessionID string) string {
	return "root/cluster-history/raycluster/default/rc/" + sessionID + "/" + testNodeID + "/logs/"
}

func writeCount(writer *MockStorageWriter, objectName string) int {
	count := 0
	for _, written := range writer.order() {
		if written == objectName {
			count++
		}
	}
	return count
}

// pointSessionLatest aims the session_latest symlink at sessionID, replacing any
// session it already points to.
func pointSessionLatest(t *testing.T, rayRoot, sessionID string) {
	t.Helper()
	link := filepath.Join(rayRoot, "session_latest")
	if err := os.Remove(link); err != nil && !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("Remove(%s) = %v", link, err)
	}
	if err := os.Symlink(filepath.Join(rayRoot, sessionID), link); err != nil {
		t.Fatalf("Symlink() = %v", err)
	}
}

func assertRotatedUploaded(t *testing.T, handler *RayLogHandler, want []string) {
	t.Helper()
	handler.rotatedMu.Lock()
	defer handler.rotatedMu.Unlock()
	got := keysOf(handler.rotatedUploaded)
	slices.Sort(got)
	slices.Sort(want)
	if !slices.Equal(got, want) {
		t.Fatalf("rotatedUploaded = %v, want %v", got, want)
	}
}

func linkSessionLatest(t *testing.T, rayRoot, sessionID string) string {
	t.Helper()
	logsDir := filepath.Join(rayRoot, sessionID, utils.RAY_SESSIONDIR_LOGDIR_NAME)
	if err := os.MkdirAll(logsDir, 0o755); err != nil {
		t.Fatalf("MkdirAll(%s) = %v", logsDir, err)
	}
	if err := os.Symlink(filepath.Join(rayRoot, sessionID), filepath.Join(rayRoot, "session_latest")); err != nil {
		t.Fatalf("Symlink() = %v", err)
	}
	return logsDir
}

func mustRotatedName(t *testing.T, backupName string, id rotatedIdentity) string {
	t.Helper()
	name, ok := rotatedLogName(backupName, id)
	if !ok {
		t.Fatalf("rotatedLogName(%q) reported no rotation backup", backupName)
	}
	return name
}

func assertWritten(t *testing.T, writer *MockStorageWriter, want map[string]string) {
	t.Helper()
	got := writer.written()
	if len(got) != len(want) {
		t.Fatalf("wrote objects %v, want %v", keysOf(got), keysOf(want))
	}
	for name, content := range want {
		if got[name] != content {
			t.Fatalf("object %q = %q, want %q (wrote %v)", name, got[name], content, keysOf(got))
		}
	}
}

func keysOf[V any](files map[string]V) []string {
	names := make([]string, 0, len(files))
	for name := range files {
		names = append(names, name)
	}
	return names
}
