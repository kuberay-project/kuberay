package utils

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"
	"unicode"

	"github.com/sirupsen/logrus"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
)

const (
	RAY_SESSIONDIR_LOGDIR_NAME            = "logs"
	RAY_SESSIONDIR_FETCHED_ENDPOINTS_NAME = "fetched_endpoints"
	DATETIME_LAYOUT                       = "2006-01-02_15-04-05.000000"
	// The following regex shouldn't be changed unless the ray session ID changes.
	SESSION_ID_REGEX = `session_(\d{4}-\d{2}-\d{2})_(\d{2}-\d{2}-\d{2})_(\d{6})`
	HEX_REGEX        = `^[0-9a-fA-F]+$`
)

var (
	sessionIDRegex         = regexp.MustCompile(SESSION_ID_REGEX)
	hexRegex               = regexp.MustCompile(HEX_REGEX)
	errDashboardAuthFailed = errors.New("ray dashboard rejected the request: authentication failed")
)

// EndpointPathToStorageKey converts a Ray Dashboard API endpoint path to a
// storage key that follows the existing naming convention used by OssMetaFile_*
// constants (e.g., "restful__api__v0__cluster_metadata").
//
// Examples:
//
//	"/api/v0/cluster_metadata"  -> "restful__api__v0__cluster_metadata"
//	"/api/v0/nodes/summary"     -> "restful__api__v0__nodes__summary"
//	"/api/serve/applications/"  -> "restful__api__serve__applications"
func EndpointPathToStorageKey(endpointPath string) string {
	trimmed := strings.Trim(endpointPath, "/")
	return "restful__" + strings.ReplaceAll(trimmed, "/", "__")
}

const (
	// connector is the separator for creating flat storage keys.
	//
	// Design Philosophy:
	// - Format: "{clusterName}_{namespace}" for router/historyserver/collector
	//
	// Why "_" instead of "/"?
	// Using "/" would create a hierarchical path like "namespace/cluster/session/..."
	// which requires multiple ListObjects API calls to traverse:
	//   1. First list all clusters under a namespace
	//   2. Then list contents of the target cluster
	//
	// Using "_" creates a flat path like "namespace_cluster/session/..."
	// which allows direct access with a single ListObjects call.
	//
	// Why this is SAFE for parsing:
	// - Kubernetes namespace follows DNS-1123 label spec
	// - DNS-1123 only allows: lowercase letters, digits, and hyphens (-)
	// - Namespace CANNOT contain "_", so we can unambiguously split from the LAST "_"
	//
	// DO NOT CHANGE: Would break existing stored data paths
	Connector = "_"
)

// IsSessionDirActive checks if the raylet socket is running. Connection means raylet is active.
func IsSessionDirActive(sessionDir string) bool {
	socketPath := filepath.Join(sessionDir, "sockets", "raylet")
	conn, err := (&net.Dialer{Timeout: 1 * time.Second}).DialContext(context.Background(), "unix", socketPath)
	if err == nil {
		conn.Close()
		return true
	}
	return false
}

func GetSessionDir() (string, error) {
	var lastErr error
	for range 12 {
		symlinkPath := GetRaySessionLatestPath()
		resolvedPath, resolveErr := filepath.EvalSymlinks(symlinkPath)
		if resolveErr != nil {
			// Fallback: readlink directly
			var rp string
			rp, resolveErr = os.Readlink(symlinkPath)
			if resolveErr == nil {
				// If readlink returned a relative target (e.g. session_2026-...), convert to absolute
				if !filepath.IsAbs(rp) {
					rp = filepath.Join(filepath.Dir(symlinkPath), rp)
				}
				resolvedPath = filepath.Clean(rp)
			}
		}

		if resolveErr == nil {
			if IsSessionDirActive(resolvedPath) {
				return resolvedPath, nil
			}
			logrus.Infof("Session directory resolved to %s, but it is not active (raylet socket not ready). Retrying...", resolvedPath)
		} else {
			if !os.IsNotExist(resolveErr) {
				return "", fmt.Errorf("failed to resolve session_latest symlink %s: %w", symlinkPath, resolveErr)
			}
			lastErr = resolveErr
			logrus.Warnf("Failed to read/resolve session_latest symlink: %v. Retrying...", resolveErr)
		}

		time.Sleep(time.Second * 5)
	}
	return "", fmt.Errorf("timeout waiting for active Ray session: %w", lastErr)
}

// MoveSessionLogsToPrevLogs moves the logs/ folder of the specified sessionDir to prev-logs
func MoveSessionLogsToPrevLogs(sessionDir, nodeID string) error {
	if nodeID == "" {
		return fmt.Errorf("nodeID cannot be empty")
	}
	sessionName := filepath.Base(sessionDir)

	oldLogsDir := filepath.Join(sessionDir, "logs")
	if _, err := os.Stat(oldLogsDir); err != nil {
		// No logs directory, nothing to move
		return nil
	}

	dest := filepath.Join(GetTmpRayRoot(), "prev-logs", sessionName, nodeID)
	if err := os.MkdirAll(dest, 0o755); err != nil {
		return fmt.Errorf("failed to create destination path %s: %w", dest, err)
	}

	destLogsDir := filepath.Join(dest, "logs")
	if err := os.Rename(oldLogsDir, destLogsDir); err != nil {
		return fmt.Errorf("failed to move logs from %s to %s: %w", oldLogsDir, destLogsDir, err)
	}
	logrus.Infof("Moved session logs from %s to %s", oldLogsDir, destLogsDir)
	return nil
}

// MoveLeftoverSessionLogs scans /tmp/ray/ for any old session_* directories (dirPath != activeSessionDir)
// and moves their logs/ folder to prev-logs/<sessionName>/<nodeID>/logs for final upload and cleanup.
//
// Parity with Boilerplate Command:
// This function replaces the exact shell check and move previously required in Ray container `command:` override:
//
//	[ -d "$RAY_TMP_ROOT/session_latest" ] && mv "$RAY_TMP_ROOT/session_latest/logs" "$dest/logs"
//
// Both the old command and this Go code move the previous session's logs out of the way before overwrite,
// allowing logcollector to upload them to storage (GCS/S3) and clean up the temporary directory.
//
// Uses MoveSessionLogsToPrevLogs()
func MoveLeftoverSessionLogs(activeSessionDir, nodeID string) error {
	if activeSessionDir == "" || nodeID == "" {
		return fmt.Errorf("activeSessionDir and nodeID cannot be empty")
	}
	activeSessionName := filepath.Base(activeSessionDir)
	entries, err := os.ReadDir(GetTmpRayRoot())
	if err != nil {
		return fmt.Errorf("failed to read Ray temporary root directory %s: %w", GetTmpRayRoot(), err)
	}
	var moveErrs []string
	for _, entry := range entries {
		if entry.Type()&os.ModeSymlink != 0 {
			continue
		}
		if !entry.IsDir() || !strings.HasPrefix(entry.Name(), "session_") {
			continue
		}
		if entry.Name() == activeSessionName || entry.Name() == "session_latest" {
			continue
		}
		dirPath := filepath.Join(GetTmpRayRoot(), entry.Name())
		if err := MoveSessionLogsToPrevLogs(dirPath, nodeID); err != nil {
			logrus.Warnf("Failed to relocate leftover session logs for %s: %v", dirPath, err)
			moveErrs = append(moveErrs, fmt.Sprintf("%s: %v", dirPath, err))
		}
	}
	if len(moveErrs) > 0 {
		return fmt.Errorf("failed to relocate some session logs: %s", strings.Join(moveErrs, "; "))
	}
	return nil
}

type nodeSummaryResp struct {
	Data struct {
		Result struct {
			Result []struct {
				NodeID string `json:"node_id"`
				NodeIP string `json:"node_ip"`
				State  string `json:"state"`
			} `json:"result"`
		} `json:"result"`
	} `json:"data"`
}

var (
	ErrPodIPNotSet   = errors.New("POD_IP environment variable is not set")
	ErrFQRayIPNotSet = errors.New("FQ_RAY_IP environment variable is not set")
)

// FetchCurrentNodeID performs a single non-blocking query to discover the active Ray NodeID for the current Pod IP.
func FetchCurrentNodeID() (string, error) {
	podIP := os.Getenv("POD_IP")
	if podIP == "" {
		return "", ErrPodIPNotSet
	}
	rayheadAddr := os.Getenv("FQ_RAY_IP")
	if rayheadAddr == "" {
		return "", ErrFQRayIPNotSet
	}
	addr := rayheadAddr
	scheme := "http://"
	if after, ok := strings.CutPrefix(addr, "https://"); ok {
		scheme, addr = "https://", after
	}
	addr = strings.TrimPrefix(addr, "http://")
	if !strings.Contains(addr, ":") {
		port := os.Getenv("RAY_DASHBOARD_PORT")
		if port == "" {
			port = "8265"
		}
		addr += ":" + port
	}
	endpoint := fmt.Sprintf("%s%s/api/v0/nodes?limit=10000", scheme, strings.TrimRight(addr, "/"))
	client := &http.Client{Timeout: 1 * time.Second}

	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, endpoint, nil)
	if err != nil {
		return "", err
	}
	if err := SetRayAuthHeader(req); err != nil {
		return "", fmt.Errorf("failed to authenticate request to %s: %w", endpoint, err)
	}

	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if IsAuthFailure(resp.StatusCode) {
		return "", fmt.Errorf("%w: HTTP status %d from endpoint %s", errDashboardAuthFailed, resp.StatusCode, endpoint)
	}
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("HTTP status %d from endpoint %s", resp.StatusCode, endpoint)
	}

	var payload nodeSummaryResp
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return "", err
	}
	for _, node := range payload.Data.Result.Result {
		if node.NodeIP == podIP && node.NodeID != "" && node.State == "ALIVE" {
			return node.NodeID, nil
		}
	}
	return "", fmt.Errorf("no ALIVE Ray node found for Pod IP %s", podIP)
}

func GetNodeRayIDWithFQIP() (string, error) {
	var lastErr error
	// Retry loop waiting for Ray Head to become ready. 12 times so the total timeout is 60 seconds
	for i := range 12 {
		nodeID, err := FetchCurrentNodeID()
		if err == nil {
			return nodeID, nil
		}
		if errors.Is(err, ErrPodIPNotSet) || errors.Is(err, ErrFQRayIPNotSet) ||
			errors.Is(err, errDashboardAuthFailed) || errors.Is(err, errRayAuthConfig) {
			return "", err
		}
		logrus.Warnf("Attempt %d/12 to discover Ray NodeID failed: %v, retrying in 5s", i+1, err)
		lastErr = err
		time.Sleep(5 * time.Second)
	}
	return "", fmt.Errorf("timeout: failed to discover Ray NodeID via HTTP endpoint for Pod IP %s: %w", os.Getenv("POD_IP"), lastErr)
}

// ConvertBase64ToHex converts an ID to hex format.
// Handles both cases:
// 1. Already hex format - returns as-is
// 2. Base64-encoded - decodes to hex
// It tries RawURLEncoding first (Ray's default), falling back to StdEncoding if that fails.
func ConvertBase64ToHex(id string) (string, error) {
	// Check if already hex (only [0-9a-f])
	if hexRegex.MatchString(id) {
		return strings.ToLower(id), nil
	}

	// Try base64 decode
	idBytes, err := base64.RawURLEncoding.DecodeString(id)
	if err != nil {
		// Try standard Base64 if URL-safe fails
		idBytes, err = base64.StdEncoding.DecodeString(id)
		if err != nil {
			return id, fmt.Errorf("failed to decode Base64 ID: %w", err)
		}
	}
	return fmt.Sprintf("%x", idBytes), nil
}

// IsHexNil returns true if hexStr decodes to a non-empty byte slice where every byte is 0xff.
func IsHexNil(hexStr string) (bool, error) {
	s := strings.TrimSpace(hexStr)

	if len(s) == 0 {
		return false, nil
	}

	// Hex string must have even length.
	if len(s)%2 != 0 {
		return false, hex.ErrLength
	}

	bytes, err := hex.DecodeString(s)
	if err != nil {
		return false, err
	}

	for _, v := range bytes {
		if v != 0xff {
			return false, nil
		}
	}
	return true, nil
}

// BuildClusterSessionKey constructs the key used to identify a specific cluster session.
// Format: "{clusterName}_{namespace}_{sessionName}"
// Example: "raycluster-historyserver_default_session_2026-01-11_19-38-40"
func BuildClusterSessionKey(clusterName, namespace, sessionName string) string {
	return clusterName + Connector + namespace + Connector + sessionName
}

// GetDateTimeFromSessionID will convert sessionID string i.e. `session_2026-01-27_10-52-59_373533_1` to time.Time
func GetDateTimeFromSessionID(sessionID string) (time.Time, error) {
	matches := sessionIDRegex.FindStringSubmatch(sessionID)

	if len(matches) < 4 {
		return time.Time{}, fmt.Errorf("Invalid session string format, expected `session_YYYY-MM-DD_HH-MM-SS_MICROSECOND` got: %s", sessionID)
	}

	timeStr := fmt.Sprintf("%s_%s.%s", matches[1], matches[2], matches[3])

	t, err := time.Parse(DATETIME_LAYOUT, timeStr)
	if err != nil {
		return time.Time{}, err
	}

	return t, nil
}

// IsAuthEnabled returns whether Ray auth is enabled.
func IsAuthEnabled(spec *rayv1.RayClusterSpec) bool {
	return spec.AuthOptions != nil && spec.AuthOptions.Mode == rayv1.AuthModeToken
}

func IsK8sAuthEnabled(authOptions *rayv1.AuthOptions) bool {
	return authOptions != nil && authOptions.EnableK8sTokenAuth != nil && *authOptions.EnableK8sTokenAuth
}

// CheckName makes sure the name does not start with a numeric value and the total length is < 63 char
func CheckName(s string) string {
	maxLength := 50 // 63 - (max(8,6) + 5 ) // 6 to 8 char are consumed at the end with "-head-" or -worker- + 5 generated.

	if len(s) > maxLength {
		// shorten the name
		offset := int(math.Abs(float64(maxLength) - float64(len(s))))
		logrus.Debugf("Name is too long: len = %v, we will shorten it by offset = %v", len(s), offset)
		s = s[offset:]
	}

	// cannot start with a numeric value
	if unicode.IsDigit(rune(s[0])) {
		s = "r" + s[1:]
	}

	// cannot start with a punctuation
	if unicode.IsPunct(rune(s[0])) {
		logrus.Debugf("Name starts with a punctuation: %s, we will prefix it with 'r'", s)
		s = "r" + s[1:]
	}

	return s
}
