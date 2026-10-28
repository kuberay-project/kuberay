package types

import (
	"time"

	"k8s.io/apimachinery/pkg/util/validation/field"
)

type RayHistoryServerConfig struct {
	RootDir string
}

type RayCollectorConfig struct {
	RootDir     string
	SessionDir  string
	RayNodeName string

	Role                string
	RayClusterName      string
	RayClusterNamespace string
	OwnerKind           string
	OwnerName           string
	LogBatching         int
	PushInterval        time.Duration
	DashboardAddress    string

	// AdditionalEndpoints are polled on top of the collector's built-in set.
	AdditionalEndpoints  []string
	EndpointPollInterval time.Duration

	// LogUploadInterval is how often the active session log directory is
	// scanned to upload Ray rotation backups and active log chunks.
	LogUploadInterval time.Duration

	// Event collector disk-first storage configuration.
	EventDataDir          string        // root directory for JSONL event files
	EventRotationInterval time.Duration // time-based rotation trigger
	EventMaxFileSizeMB    int           // size-based rotation trigger (MB)
	EventMaxDiskMB        int           // backpressure threshold (MB)
	// EventCompressionEnabled controls whether rotated JSONL files are
	// gzipped before being uploaded to remote storage. When false, plain
	// JSONL files are uploaded as-is. Events are always written to local
	// disk first regardless of this setting.
	EventCompressionEnabled bool
}

// ValidateRayHanderConfig is
func ValidateRayHanderConfig(c *RayCollectorConfig, fldpath *field.Path) field.ErrorList {
	var allErrs field.ErrorList
	if len(c.SessionDir) == 0 {
		allErrs = append(allErrs, field.Invalid(fldpath, c.SessionDir, "session-dir must be set"))
	}
	if len(c.RayClusterName) == 0 {
		allErrs = append(allErrs, field.Invalid(fldpath, c.RayClusterName, "ray_cluster_name must be set"))
	}
	if len(c.RayNodeName) == 0 {
		allErrs = append(allErrs, field.Invalid(fldpath, c.RayNodeName, "ray_node_name must be set"))
	}
	if len(c.RayClusterNamespace) == 0 {
		allErrs = append(allErrs, field.Invalid(fldpath, c.RayClusterNamespace, "ray_cluster_namespace must be set"))
	}

	if c.Role == "Head" {
		if len(c.RayClusterName) == 0 {
			allErrs = append(allErrs, field.Invalid(fldpath, c.RayClusterName, "ray_cluster_name must be set"))
		}
		if len(c.RayClusterNamespace) == 0 {
			allErrs = append(allErrs, field.Invalid(fldpath, c.RayClusterNamespace, "ray_cluster_namespace must be set"))
		}
	}
	return allErrs
}
