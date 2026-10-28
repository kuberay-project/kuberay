package utils

import (
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"
)

const (
	defaultTmpRayRoot = "/tmp/ray"

	// Lowercase, normalized CRD kinds. Used both as the comparison key
	// for ToLower(OwnerKind) and as the cluster-metadata path subdir segment.
	RayJobKind     = "rayjob"
	RayServiceKind = "rayservice"
	RayClusterKind = "raycluster"

	// RayContainerIndex is the index of the Ray container in the head pod template.
	RayContainerIndex = 0
	// DashboardPortName is the name the ray-operator gives the dashboard port.
	DashboardPortName = "dashboard"
	// DefaultDashboardPort is used when the head container does not declare a dashboard port.
	DefaultDashboardPort = 8265

	// RotatedLogMarker separates a rotated log generation's identity from the
	// active log name it was rotated out of: <base>.rotated.<time>-<inode><ext>.
	RotatedLogMarker = ".rotated."

	// DefaultLogUploadInterval is how often the collector scans the active
	// session log directory and uploads rotation backups and active log chunks.
	// A shorter interval creates more smaller objects in the object store, which
	// slows down reads that merge the chunks.
	DefaultLogUploadInterval = 5 * time.Minute
)

// IsRotatedLogName reports whether a log file name refers to a rotated
// generation uploaded by the collector rather than an active Ray log stream.
func IsRotatedLogName(name string) bool {
	return strings.Contains(path.Base(name), RotatedLogMarker)
}

func GetTmpRayRoot() string {
	if tmpRoot := os.Getenv("RAY_TMP_ROOT"); tmpRoot != "" {
		return tmpRoot
	}
	return defaultTmpRayRoot
}

func GetRayPrevLogsPath() string {
	return filepath.Join(GetTmpRayRoot(), "prev-logs")
}

func GetRayPersistCompletePath() string {
	return filepath.Join(GetTmpRayRoot(), "persist-complete-logs")
}

func GetRaySessionLatestPath() string {
	return filepath.Join(GetTmpRayRoot(), "session_latest")
}
