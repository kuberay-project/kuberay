package logcollector

import (
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"

	. "github.com/onsi/gomega"

	"github.com/ray-project/kuberay/historyserver/pkg/utils"
)

// MockStorageWriter is a mock implementation of storage.StorageWriter for testing
type MockStorageWriter struct {
	mu           sync.Mutex
	createdDirs  []string
	writtenFiles map[string]string // path -> content
	writeOrder   []string          // paths in the order they were written
	// beforeWrite runs while the caller's file handle is still open, letting a
	// test disturb the source path mid-upload.
	beforeWrite func()
	writeErr    error
}

func NewMockStorageWriter() *MockStorageWriter {
	return &MockStorageWriter{
		createdDirs:  make([]string, 0),
		writtenFiles: make(map[string]string),
	}
}

func (m *MockStorageWriter) CreateDirectory(path string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.createdDirs = append(m.createdDirs, path)
	return nil
}

func (m *MockStorageWriter) WriteFile(file string, reader io.ReadSeeker) error {
	m.mu.Lock()
	beforeWrite, writeErr := m.beforeWrite, m.writeErr
	m.mu.Unlock()
	if beforeWrite != nil {
		beforeWrite()
	}
	if writeErr != nil {
		return writeErr
	}
	content, err := io.ReadAll(reader)
	if err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.writtenFiles[file] = string(content)
	m.writeOrder = append(m.writeOrder, file)
	return nil
}

func (m *MockStorageWriter) written() map[string]string {
	m.mu.Lock()
	defer m.mu.Unlock()
	files := make(map[string]string, len(m.writtenFiles))
	maps.Copy(files, m.writtenFiles)
	return files
}

func (m *MockStorageWriter) order() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return slices.Clone(m.writeOrder)
}

func (m *MockStorageWriter) setWriteErr(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.writeErr = err
}

// setupRayTestEnvironment creates test directories under /tmp/ray for realistic testing
// This matches the actual paths used by the logcollector
func setupRayTestEnvironment(t *testing.T) (string, func()) {
	baseDir := filepath.Join("/tmp", "ray-test-"+t.Name())

	// Create base directory
	if err := os.MkdirAll(baseDir, 0o755); err != nil {
		t.Fatalf("Failed to create base dir: %v", err)
	}

	// Create prev-logs and persist-complete-logs directories
	prevLogsDir := filepath.Join(baseDir, "prev-logs")
	persistLogsDir := filepath.Join(baseDir, "persist-complete-logs")

	if err := os.MkdirAll(prevLogsDir, 0o755); err != nil {
		t.Fatalf("Failed to create prev-logs dir: %v", err)
	}
	if err := os.MkdirAll(persistLogsDir, 0o755); err != nil {
		t.Fatalf("Failed to create persist-complete-logs dir: %v", err)
	}

	cleanup := func() {
		os.RemoveAll(baseDir)
	}

	return baseDir, cleanup
}

// createTestLogFile creates a test log file with given content
func createTestLogFile(t *testing.T, path string, content string) {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("Failed to create directory %s: %v", dir, err)
	}

	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("Failed to write file %s: %v", path, err)
	}
}

// TestIsFileAlreadyPersisted tests the file-level persistence check
func TestIsFileAlreadyPersisted(t *testing.T) {
	baseDir, cleanup := setupRayTestEnvironment(t)
	defer cleanup()

	// Use the actual prev-logs directory structure that matches production
	handler := &RayLogHandler{
		prevLogsDir:            filepath.Join(baseDir, "prev-logs"),
		persistCompleteLogsDir: filepath.Join(baseDir, "persist-complete-logs"),
	}

	sessionID := "session-123"
	nodeID := "node-456"

	// Create prev-logs structure
	prevLogsPath := filepath.Join(handler.prevLogsDir, sessionID, nodeID, utils.RAY_SESSIONDIR_LOGDIR_NAME, "worker.log")
	createTestLogFile(t, prevLogsPath, "test log content")

	// Test case 1: File not yet persisted
	if handler.isFileAlreadyPersisted(prevLogsPath, sessionID, nodeID) {
		t.Error("Expected file to not be persisted yet")
	}

	// Create the persisted file in persist-complete-logs
	persistedPath := filepath.Join(baseDir, "persist-complete-logs", sessionID, nodeID, utils.RAY_SESSIONDIR_LOGDIR_NAME, "worker.log")
	createTestLogFile(t, persistedPath, "test log content")

	// Test case 2: File already persisted
	if !handler.isFileAlreadyPersisted(prevLogsPath, sessionID, nodeID) {
		t.Error("Expected file to be detected as persisted")
	}
}

// TestScanAndProcess tests the full lifecycle: partial upload, interruption, and resumption via scan.
//
// This test simulates a crash recovery scenario:
// 1. Two log files exist in prev-logs
// 2. Only file1 is processed (simulating partial success before crash)
// 3. File1 is restored to prev-logs (simulating incomplete rename during crash)
// 4. WatchPrevLogsLoops is started (simulating collector restart)
// 5. Verify that file1 is NOT re-uploaded (idempotency) and file2 is uploaded
// 6. Verify that the node directory is cleaned up after all files are processed
func TestScanAndProcess(t *testing.T) {
	g := NewWithT(t)

	baseDir, cleanup := setupRayTestEnvironment(t)
	defer cleanup()

	mockWriter := NewMockStorageWriter()
	handler := &RayLogHandler{
		Writer:                 mockWriter,
		RootDir:                "/test-root",
		prevLogsDir:            filepath.Join(baseDir, "prev-logs"),
		persistCompleteLogsDir: filepath.Join(baseDir, "persist-complete-logs"),
		ShutdownChan:           make(chan struct{}),
		RayClusterName:         "test-cluster",
		RayClusterNamespace:    "cluster-123",
	}

	sessionID := "session-lifecycle"
	nodeID := "node-1"
	logsDir := filepath.Join(handler.prevLogsDir, sessionID, nodeID, utils.RAY_SESSIONDIR_LOGDIR_NAME)

	// Prepare two log files in prev-logs directory
	f1 := filepath.Join(logsDir, "file1.log")
	f2 := filepath.Join(logsDir, "file2.log")
	createTestLogFile(t, f1, "content1")
	createTestLogFile(t, f2, "content2")

	// --- Step 1: Process file1 only (simulating partial success before crash) ---
	err := handler.processPrevLogFile(f1, logsDir, sessionID, nodeID)
	if err != nil {
		t.Fatalf("Failed to process file1: %v", err)
	}

	// Verify file1 is uploaded to storage
	if len(mockWriter.writtenFiles) != 1 {
		t.Errorf("Expected 1 file in storage, got %d", len(mockWriter.writtenFiles))
	}

	// Manually restore file1 to prev-logs to simulate a crash right after upload
	// but before the rename operation completed
	createTestLogFile(t, f1, "content1")

	// --- Step 2: Start the startup scan in background (simulating collector restart) ---
	go handler.WatchPrevLogsLoops()

	// --- Step 3: Use Eventually to wait for async processing ---
	sessionNodeDir := filepath.Join(handler.prevLogsDir, sessionID, nodeID)

	// Wait until storage has exactly 2 files.
	// file1 should NOT be re-uploaded because it already exists in persist-complete-logs.
	// Only file2 should be newly uploaded.
	g.Eventually(func() int {
		mockWriter.mu.Lock()
		defer mockWriter.mu.Unlock()
		return len(mockWriter.writtenFiles)
	}, 5*time.Second, 100*time.Millisecond).Should(Equal(2),
		"Storage should have 2 unique files (file1 should NOT be re-uploaded due to idempotency check)")

	// Wait until the node directory in prev-logs is removed.
	// After all files are processed and moved to persist-complete-logs,
	// the node directory should be cleaned up.
	g.Eventually(func() bool {
		_, err := os.Stat(sessionNodeDir)
		return os.IsNotExist(err)
	}, 5*time.Second, 100*time.Millisecond).Should(BeTrue(),
		"Node directory should be removed after all files are processed and moved to persist-complete-logs")

	// Signal the background goroutine to exit gracefully
	close(handler.ShutdownChan)
}

// TestProcessLogs_SkipSymlinks verifies that symlinks are skipped during directory scanning in prev-logs (processPrevLogsDir).
func TestProcessLogs_SkipSymlinks(t *testing.T) {
	baseDir, cleanup := setupRayTestEnvironment(t)
	defer cleanup()

	mockWriter := NewMockStorageWriter()
	handler := &RayLogHandler{
		Writer:                 mockWriter,
		RootDir:                "/test-root",
		prevLogsDir:            filepath.Join(baseDir, "prev-logs"),
		persistCompleteLogsDir: filepath.Join(baseDir, "persist-complete-logs"),
		RayClusterName:         "test-cluster",
		RayClusterNamespace:    "cluster-123",
	}

	sessionID := "session-symlinks"
	nodeID := "node-1"
	logsDir := filepath.Join(handler.prevLogsDir, sessionID, nodeID, utils.RAY_SESSIONDIR_LOGDIR_NAME)

	// Create a regular log file and a symlink in prev-logs
	regularFile := filepath.Join(logsDir, "regular.log")
	createTestLogFile(t, regularFile, "regular content")
	symlinkFile := filepath.Join(logsDir, "symlink.log")
	if err := os.Symlink(regularFile, symlinkFile); err != nil {
		t.Fatalf("Failed to create symlink: %v", err)
	}

	// Run processPrevLogsDir synchronously
	handler.processPrevLogsDir(filepath.Join(handler.prevLogsDir, sessionID, nodeID))

	// Verify only regular.log was uploaded, and symlink.log was skipped
	mockWriter.mu.Lock()
	if len(mockWriter.writtenFiles) != 1 {
		t.Errorf("Expected 1 file written to storage, got %d", len(mockWriter.writtenFiles))
	}
	for path := range mockWriter.writtenFiles {
		if filepath.Base(path) == "symlink.log" {
			t.Errorf("Symlink was incorrectly uploaded to storage: %s", path)
		}
	}
	mockWriter.mu.Unlock()
}

func TestPollActiveSessionChanges(t *testing.T) {
	g := NewWithT(t)
	baseDir := t.TempDir()

	originalTmpRoot := os.Getenv("RAY_TMP_ROOT")
	defer os.Setenv("RAY_TMP_ROOT", originalTmpRoot)
	os.Setenv("RAY_TMP_ROOT", baseDir)

	handler := &RayLogHandler{
		SessionDir:             filepath.Join(baseDir, "session_2026-07-08_15-00-00_123456_1"),
		prevLogsDir:            filepath.Join(baseDir, "prev-logs"),
		persistCompleteLogsDir: filepath.Join(baseDir, "persist-complete-logs"),
		ShutdownChan:           make(chan struct{}),
		RayClusterName:         "test-cluster",
		RayClusterNamespace:    "cluster-123",
		RayNodeName:            "node-1",
	}

	sessionNameA := "session_2026-07-08_15-00-00_123456_1"
	sessionDirA := filepath.Join(baseDir, sessionNameA)
	logsDirA := filepath.Join(sessionDirA, "logs")
	createTestLogFile(t, filepath.Join(logsDirA, "raylet.out"), "log content A")

	symlinkPath := filepath.Join(baseDir, "session_latest")
	if err := os.Symlink(sessionNameA, symlinkPath); err != nil {
		t.Fatalf("failed to create symlink: %v", err)
	}

	go handler.PollActiveSessionChanges()

	time.Sleep(500 * time.Millisecond)

	sessionNameB := "session_2026-07-08_16-00-00_123456_1"
	sessionDirB := filepath.Join(baseDir, sessionNameB)
	logsDirB := filepath.Join(sessionDirB, "logs")
	createTestLogFile(t, filepath.Join(logsDirB, "raylet.out"), "log content B")

	os.Remove(symlinkPath)
	if err := os.Symlink(sessionNameB, symlinkPath); err != nil {
		t.Fatalf("failed to create symlink: %v", err)
	}

	expectedPrevLogsDir := filepath.Join(handler.prevLogsDir, sessionNameA, handler.RayNodeName, "logs")
	g.Eventually(func() bool {
		_, err := os.Stat(filepath.Join(expectedPrevLogsDir, "raylet.out"))
		return err == nil
	}, 5*time.Second, 100*time.Millisecond).Should(BeTrue(), "Logs from session_A should be moved to prev-logs")

	_, err := os.Stat(filepath.Join(logsDirA, "raylet.out"))
	g.Expect(os.IsNotExist(err)).To(BeTrue(), "Original logs in session_A/logs should be deleted (moved)")

	close(handler.ShutdownChan)
}

func TestPollActiveSessionChanges_MultipleIntermediateSessions(t *testing.T) {
	g := NewWithT(t)
	baseDir := t.TempDir()

	originalTmpRoot := os.Getenv("RAY_TMP_ROOT")
	defer os.Setenv("RAY_TMP_ROOT", originalTmpRoot)
	os.Setenv("RAY_TMP_ROOT", baseDir)

	handler := &RayLogHandler{
		SessionDir:             filepath.Join(baseDir, "session_2026-07-08_15-00-00_123456_1"),
		prevLogsDir:            filepath.Join(baseDir, "prev-logs"),
		persistCompleteLogsDir: filepath.Join(baseDir, "persist-complete-logs"),
		ShutdownChan:           make(chan struct{}),
		RayClusterName:         "test-cluster",
		RayClusterNamespace:    "cluster-123",
		RayNodeName:            "node-1",
	}

	sessionNameA := "session_2026-07-08_15-00-00_123456_1"
	sessionDirA := filepath.Join(baseDir, sessionNameA)
	createTestLogFile(t, filepath.Join(sessionDirA, "logs", "raylet.out"), "log content A")

	symlinkPath := filepath.Join(baseDir, "session_latest")
	if err := os.Symlink(sessionNameA, symlinkPath); err != nil {
		t.Fatalf("failed to create symlink: %v", err)
	}

	go handler.PollActiveSessionChanges()
	time.Sleep(200 * time.Millisecond)

	// Simulate rapid restart: session B created (intermediate), then session C created before next ticker poll
	sessionNameB := "session_2026-07-08_15-30-00_123456_1"
	sessionDirB := filepath.Join(baseDir, sessionNameB)
	createTestLogFile(t, filepath.Join(sessionDirB, "logs", "raylet.out"), "log content B")

	sessionNameC := "session_2026-07-08_16-00-00_123456_1"
	sessionDirC := filepath.Join(baseDir, sessionNameC)
	createTestLogFile(t, filepath.Join(sessionDirC, "logs", "raylet.out"), "log content C")

	os.Remove(symlinkPath)
	if err := os.Symlink(sessionNameC, symlinkPath); err != nil {
		t.Fatalf("failed to create symlink: %v", err)
	}

	expectedPrevLogsA := filepath.Join(handler.prevLogsDir, sessionNameA, handler.RayNodeName, "logs")
	expectedPrevLogsB := filepath.Join(handler.prevLogsDir, sessionNameB, handler.RayNodeName, "logs")

	g.Eventually(func() bool {
		_, errA := os.Stat(filepath.Join(expectedPrevLogsA, "raylet.out"))
		_, errB := os.Stat(filepath.Join(expectedPrevLogsB, "raylet.out"))
		return errA == nil && errB == nil
	}, 6*time.Second, 100*time.Millisecond).Should(BeTrue(), "Logs from both session_A and intermediate session_B should be moved to prev-logs")

	close(handler.ShutdownChan)
}

func TestNodeIDRefresh(t *testing.T) {
	g := NewWithT(t)
	baseDir := t.TempDir()

	origPodIP := os.Getenv("POD_IP")
	origFQRayIP := os.Getenv("FQ_RAY_IP")
	origTmpRoot := os.Getenv("RAY_TMP_ROOT")
	defer func() {
		os.Setenv("POD_IP", origPodIP)
		os.Setenv("FQ_RAY_IP", origFQRayIP)
		os.Setenv("RAY_TMP_ROOT", origTmpRoot)
	}()

	os.Setenv("POD_IP", "127.0.0.1")
	os.Setenv("RAY_TMP_ROOT", baseDir)

	var mu sync.Mutex
	mockNodeID := "11111111111111111111111111111111"
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v0/nodes" {
			mu.Lock()
			nodeID := mockNodeID
			mu.Unlock()
			resp := fmt.Sprintf(`{
				"data": {
					"result": {
						"result": [
							{
								"node_id": "%s",
								"node_ip": "127.0.0.1",
								"state": "ALIVE"
							}
						]
					}
				}
			}`, nodeID)
			w.Header().Set("Content-Type", "application/json")
			w.Write([]byte(resp))
			return
		}
		http.NotFound(w, r)
	}))
	defer ts.Close()

	os.Setenv("FQ_RAY_IP", ts.URL)

	handler := &RayLogHandler{
		SessionDir:             baseDir,
		prevLogsDir:            filepath.Join(baseDir, "prev-logs"),
		persistCompleteLogsDir: filepath.Join(baseDir, "persist-complete-logs"),
		ShutdownChan:           make(chan struct{}),
		RayNodeName:            "11111111111111111111111111111111",
	}

	symlinkPath := filepath.Join(baseDir, "session_latest")
	if err := os.Symlink(baseDir, symlinkPath); err != nil {
		t.Fatalf("failed to create symlink: %v", err)
	}
	defer os.Remove(symlinkPath)

	go handler.PollActiveSessionChanges()
	defer close(handler.ShutdownChan)

	g.Expect(handler.GetRayNodeName()).To(Equal("11111111111111111111111111111111"))

	mu.Lock()
	mockNodeID = "22222222222222222222222222222222"
	mu.Unlock()

	g.Eventually(func() string {
		return handler.GetRayNodeName()
	}, 10*time.Second, 100*time.Millisecond).Should(Equal("22222222222222222222222222222222"), "GetRayNodeName should update dynamically when node ID changes")
}
