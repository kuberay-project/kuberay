package utils

import (
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSlice(t *testing.T) {
	strs := []string{"1", "2", "3", "4", "5"}
	t.Logf("laster %s", strs[len(strs)-1])
	t.Logf("not laster all %s", strings.Join(strs[:len(strs)-1], ""))
	clusterNameId := "a_s_sdf_sdfsdfsdfisfdf1_2safd-0sdf-sdf-000"
	strs = strings.Split(clusterNameId, "_")
	t.Logf("laster %s", strs[len(strs)-1])
	t.Logf("not laster all %s", strings.Join(strs[:len(strs)-1], "_"))

	clusterNameId = "a-s-sdf-sdfsdfsdfisfdf1A_B2safd-0sdf-sdf-000"
	strs = strings.Split(clusterNameId, "_")
	t.Logf("laster %s", strs[len(strs)-1])
	t.Logf("not laster all %s", strings.Join(strs[:len(strs)-1], "_"))
}

func TestConvertBase64ToHex(t *testing.T) {
	// TODO(chiayi): use real base64 strings from job ids as tests ex: AgAAAA==
	tests := []struct {
		scenario       string
		base64Str      string
		expectedHexStr string
		expectError    bool
	}{
		{
			scenario:       "Successful conversion from base64 to hex",
			base64Str:      "AgAAAA==",
			expectedHexStr: "02000000",
			expectError:    false,
		},
		{
			scenario:       "Failed conversion from base64 to hex - contains '_'",
			base64Str:      "AQAAAA_==",
			expectedHexStr: "AQAAAA_==",
			expectError:    true,
		},
		{
			scenario:       "Already hex with uppercase is normalized to lowercase",
			base64Str:      "ABCDEF01",
			expectedHexStr: "abcdef01",
			expectError:    false,
		},
		{
			scenario:       "Already hex with lowercase is returned as-is",
			base64Str:      "abcdef01",
			expectedHexStr: "abcdef01",
			expectError:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.scenario, func(t *testing.T) {
			actualHexStr, err := ConvertBase64ToHex(tt.base64Str)
			if err == nil && tt.expectError {
				t.Errorf("ConvertToBase64ToHex() expected error for base64 string %s", tt.base64Str)
			}
			if actualHexStr != tt.expectedHexStr {
				t.Errorf("Actual conversion does not match expected result. Actual: %s Expected: %s", actualHexStr, tt.expectedHexStr)
			}
		})
	}
}

func TestGetDateTimeFromSessionID(t *testing.T) {
	tests := []struct {
		name         string
		sessionID    string
		expectErr    bool
		expectedTime time.Time
	}{
		{
			name:         "valid session id",
			sessionID:    "session_2024-05-15_10-30-55_123456",
			expectErr:    false,
			expectedTime: time.Date(2024, time.May, 15, 10, 30, 55, 123456000, time.UTC),
		},
		{
			name:      "invalid prefix",
			sessionID: "s_2024-05-15_10-30-55_123456",
			expectErr: true,
		},
		{
			name:      "missing_time",
			sessionID: "session_2024-05-15_10-30-55",
			expectErr: true,
		},
		{
			name:      "empty_string",
			sessionID: "",
			expectErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			gotTime, err := GetDateTimeFromSessionID(tc.sessionID)

			if tc.expectErr {
				if err == nil {
					t.Errorf("GetDateTimeFromSessionID(%q) succeeded unexpectedly, returned time: %v", tc.sessionID, gotTime)
				}
			} else {
				if err != nil {
					t.Fatalf("GetDateTimeFromSessionID(%q) failed unexpectedly: %v", tc.sessionID, err)
				}
				if !gotTime.Equal(tc.expectedTime) {
					t.Errorf("GetDateTimeFromSessionID(%q) = %v, want %v", tc.sessionID, gotTime, tc.expectedTime)
				}
			}
		})
	}
}

func TestGetNodeRayIDWithFQIP_EnvVars(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v0/nodes" {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"data":{"result":{"result":[{"node_id":"node-id-abc","node_ip":"10.0.0.1","state":"ALIVE"}]}}}`))
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer ts.Close()

	os.Setenv("POD_IP", "10.0.0.1")
	defer os.Unsetenv("POD_IP")

	hostPort := strings.TrimPrefix(ts.URL, "http://")
	parts := strings.Split(hostPort, ":")
	host, port := parts[0], parts[1]

	tests := []struct {
		name        string
		fqRayIP     string
		dashPortEnv string
	}{
		{"Full URL with scheme and port", ts.URL, ""},
		{"Host and port without scheme", hostPort, ""},
		{"Bare host with RAY_DASHBOARD_PORT set", host, port},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			os.Setenv("FQ_RAY_IP", tt.fqRayIP)
			if tt.dashPortEnv != "" {
				os.Setenv("RAY_DASHBOARD_PORT", tt.dashPortEnv)
			} else {
				os.Unsetenv("RAY_DASHBOARD_PORT")
			}

			nodeID, err := GetNodeRayIDWithFQIP()
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if nodeID != "node-id-abc" {
				t.Errorf("expected node-id-abc, got %s", nodeID)
			}
		})
	}
}

// A credential that cannot be resolved never becomes valid, so discovery must
// surface it immediately instead of spending the full 60s retry budget and then
// reporting a misleading timeout.
func TestGetNodeRayIDWithFQIP_FailsFastOnAuthConfigError(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"data":{"result":{"result":[{"node_id":"node-id-abc","node_ip":"10.0.0.1","state":"ALIVE"}]}}}`))
	}))
	defer ts.Close()

	t.Setenv("POD_IP", "10.0.0.1")
	t.Setenv("FQ_RAY_IP", ts.URL)
	// Token auth requested, but neither RAY_AUTH_TOKEN nor the projected token volume is available.
	t.Setenv(RAY_AUTH_MODE_ENV_VAR, RayAuthModeToken)
	t.Setenv(RAY_AUTH_TOKEN_ENV_VAR, "")
	t.Setenv(RAY_ENABLE_K8S_TOKEN_AUTH_ENV_VAR, "")

	start := time.Now()
	_, err := GetNodeRayIDWithFQIP()
	elapsed := time.Since(start)

	require.Error(t, err)
	require.ErrorIs(t, err, errRayAuthConfig)
	assert.NotContains(t, err.Error(), "timeout", "misconfiguration must not be reported as a discovery timeout")
	assert.Less(t, elapsed, 5*time.Second, "must not enter the retry loop")
}

func TestGetSessionDir_Symlinks(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "s-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(tmpDir) })
	t.Setenv("RAY_TMP_ROOT", tmpDir)

	sessionDirName := "session_1"
	absSessionDir := filepath.Join(tmpDir, sessionDirName)
	if err := os.MkdirAll(absSessionDir, 0o755); err != nil {
		t.Fatalf("failed to create session dir: %v", err)
	}

	// Create and start a Unix domain socket listener to mock the active raylet process.
	socketDir := filepath.Join(absSessionDir, "sockets")
	if err := os.MkdirAll(socketDir, 0o755); err != nil {
		t.Fatalf("failed to create sockets dir: %v", err)
	}
	socketPath := filepath.Join(socketDir, "raylet")
	listener, err := (&net.ListenConfig{}).Listen(t.Context(), "unix", socketPath)
	if err != nil {
		t.Fatalf("failed to listen on unix socket: %v", err)
	}
	defer listener.Close()

	symlinkPath := filepath.Join(tmpDir, "session_latest")

	// Test 1: Symlink pointing to relative session name
	if err := os.Symlink(sessionDirName, symlinkPath); err != nil {
		t.Fatalf("failed to create relative symlink: %v", err)
	}

	got, err := GetSessionDir()
	if err != nil {
		t.Fatalf("GetSessionDir with relative symlink failed: %v", err)
	}
	evalAbs, _ := filepath.EvalSymlinks(absSessionDir)
	if got != evalAbs {
		t.Errorf("expected %s, got %s", evalAbs, got)
	}

	// Test 2: Symlink pointing to absolute session dir
	os.Remove(symlinkPath)
	if err := os.Symlink(absSessionDir, symlinkPath); err != nil {
		t.Fatalf("failed to create absolute symlink: %v", err)
	}

	gotAbs, err := GetSessionDir()
	if err != nil {
		t.Fatalf("GetSessionDir with absolute symlink failed: %v", err)
	}
	if gotAbs != evalAbs {
		t.Errorf("expected %s, got %s", evalAbs, gotAbs)
	}
}

func TestGetSessionDir_FatalError_FailsFast(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "s-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(tmpDir) })

	// Create a file (not a directory)
	filePath := filepath.Join(tmpDir, "file")
	if err := os.WriteFile(filePath, []byte("test"), 0o600); err != nil {
		t.Fatalf("failed to create file: %v", err)
	}

	// Set RAY_TMP_ROOT to a sub-path of a file (ENOTDIR error, non-IsNotExist)
	t.Setenv("RAY_TMP_ROOT", filepath.Join(filePath, "sub"))

	start := time.Now()
	_, err = GetSessionDir()
	duration := time.Since(start)

	if err == nil {
		t.Fatalf("expected error, got nil")
	}
	if duration > 1*time.Second {
		t.Errorf("expected fail fast within 1s, took %v", duration)
	}
}

func TestGetSessionDir_WaitsForActive(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "s-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(tmpDir) })
	t.Setenv("RAY_TMP_ROOT", tmpDir)

	sessionDirName := "session_1"
	absSessionDir := filepath.Join(tmpDir, sessionDirName)
	if err := os.MkdirAll(absSessionDir, 0o755); err != nil {
		t.Fatalf("failed to create session dir: %v", err)
	}

	symlinkPath := filepath.Join(tmpDir, "session_latest")
	if err := os.Symlink(sessionDirName, symlinkPath); err != nil {
		t.Fatalf("failed to create symlink: %v", err)
	}

	socketDir := filepath.Join(absSessionDir, "sockets")
	if err := os.MkdirAll(socketDir, 0o755); err != nil {
		t.Fatalf("failed to create sockets dir: %v", err)
	}
	socketPath := filepath.Join(socketDir, "raylet")

	// Start a goroutine that will create the active raylet socket after a short delay (e.g., 2 seconds).
	go func() {
		time.Sleep(2 * time.Second)
		listener, err := (&net.ListenConfig{}).Listen(t.Context(), "unix", socketPath)
		if err == nil {
			t.Cleanup(func() {
				listener.Close()
			})
		}
	}()

	start := time.Now()
	got, err := GetSessionDir()
	duration := time.Since(start)

	if err != nil {
		t.Fatalf("GetSessionDir failed: %v", err)
	}
	evalAbs, _ := filepath.EvalSymlinks(absSessionDir)
	if got != evalAbs {
		t.Errorf("expected %s, got %s", evalAbs, got)
	}
	if duration < 4*time.Second {
		t.Errorf("expected test to take at least 5s, took %v", duration)
	}
}

func TestMoveLeftoverSessionLogs(t *testing.T) {
	tmpDir := t.TempDir()
	originalTmpRoot := os.Getenv("RAY_TMP_ROOT")
	defer os.Setenv("RAY_TMP_ROOT", originalTmpRoot)
	os.Setenv("RAY_TMP_ROOT", tmpDir)

	// 1. Create a prior session directory
	priorSessionName := "session_2026-07-08_14-00-00_123456_1"
	priorSessionDir := filepath.Join(tmpDir, priorSessionName)
	priorLogsDir := filepath.Join(priorSessionDir, "logs")
	if err := os.MkdirAll(priorLogsDir, 0o755); err != nil {
		t.Fatalf("failed to create prior logs dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(priorLogsDir, "raylet.out"), []byte("prior logs"), 0o600); err != nil {
		t.Fatalf("failed to write prior log file: %v", err)
	}

	// 2. Create the active session directory
	activeSessionName := "session_2026-07-08_15-00-00_123456_1"
	activeSessionDir := filepath.Join(tmpDir, activeSessionName)
	activeLogsDir := filepath.Join(activeSessionDir, "logs")
	if err := os.MkdirAll(activeLogsDir, 0o755); err != nil {
		t.Fatalf("failed to create active logs dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(activeLogsDir, "raylet.out"), []byte("active logs"), 0o600); err != nil {
		t.Fatalf("failed to write active log file: %v", err)
	}

	// Create session_latest symlink pointing to active session
	symlinkPath := filepath.Join(tmpDir, "session_latest")
	if err := os.Symlink(activeSessionName, symlinkPath); err != nil {
		t.Fatalf("failed to create symlink: %v", err)
	}

	nodeID := "node-abc"

	// 3. Run MoveLeftoverSessionLogs
	if err := MoveLeftoverSessionLogs(activeSessionDir, nodeID); err != nil {
		t.Fatalf("MoveLeftoverSessionLogs failed: %v", err)
	}

	// 4. Verify that the prior session's logs were moved to prev-logs
	expectedDest := filepath.Join(tmpDir, "prev-logs", priorSessionName, nodeID, "logs")
	if _, err := os.Stat(filepath.Join(expectedDest, "raylet.out")); err != nil {
		t.Errorf("Expected leftover logs to be moved to %s, but got error: %v", expectedDest, err)
	}

	// Verify that the active session's logs were NOT moved
	if _, err := os.Stat(filepath.Join(activeLogsDir, "raylet.out")); err != nil {
		t.Errorf("Expected active logs to remain in %s, but got error: %v", activeLogsDir, err)
	}
}
