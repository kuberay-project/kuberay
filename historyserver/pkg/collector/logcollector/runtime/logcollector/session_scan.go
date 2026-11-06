package logcollector

import (
	"cmp"
	"io/fs"
	"path/filepath"
	"slices"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/ray-project/kuberay/historyserver/pkg/utils"
)

// scanSessionLogs periodically uploads the active session's rotation and active logs.
func (r *RayLogHandler) scanSessionLogs(stop <-chan struct{}) {
	interval := r.LogUploadInterval
	if interval <= 0 {
		interval = utils.DefaultLogUploadInterval
	}
	logrus.Infof("Started scanning session logs (interval=%v)", interval)
	r.collectActiveSessionLogs(stop)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-stop:
			logrus.Info("Shutdown signaled, stopping session log scan")
			return
		case <-ticker.C:
			r.collectActiveSessionLogs(stop)
		}
	}
}

// collectActiveSessionLogs re-resolves session_latest on every pass so
// uploads are attributed to the session that produced them.
func (r *RayLogHandler) collectActiveSessionLogs(stop <-chan struct{}) {
	sessionDir, err := filepath.EvalSymlinks(utils.GetRaySessionLatestPath())
	if err != nil {
		logrus.Debugf("Session log scan: session_latest is not resolvable yet: %v", err)
		return
	}
	logsDir := filepath.Join(sessionDir, utils.RAY_SESSIONDIR_LOGDIR_NAME)
	r.collectSessionLogsUnder(logsDir, filepath.Base(sessionDir), r.GetRayNodeName(), stop)
}

// collectSessionLogsUnder uploads the rotation backups and active logs below logsDir.
func (r *RayLogHandler) collectSessionLogsUnder(logsDir, sessionID, nodeID string, stop <-chan struct{}) {
	objectPrefix := r.rotatedObjectPrefix(sessionID, nodeID)
	if objectPrefix == "" {
		logrus.Warnf("Skipping session log scan of %s: session or node ID is unknown", logsDir)
		return
	}

	var backups, active []string
	walkComplete := true
	_ = filepath.WalkDir(logsDir, func(absPath string, entry fs.DirEntry, walkErr error) error {
		walkComplete = walkComplete && walkErr == nil
		if walkErr == nil && entry.Type().IsRegular() {
			if _, isBackup := rotationBaseName(entry.Name()); isBackup {
				backups = append(backups, absPath)
			} else {
				active = append(active, absPath)
			}
		}
		return nil
	})
	slices.SortStableFunc(backups, func(a, b string) int {
		return cmp.Compare(rotationIndex(filepath.Base(b)), rotationIndex(filepath.Base(a)))
	})

	seen := make(map[string]struct{}, len(backups))
	for _, absPath := range backups {
		// Shutdown collection picks up whatever this pass leaves behind.
		if stopRequested(stop) {
			logrus.Debug("Shutdown signaled, ending rotated log scan early")
			return
		}
		if objectName, ok := r.collectRotatedLog(absPath, logsDir, objectPrefix); ok {
			seen[objectName] = struct{}{}
		}
	}
	// Only a pass that saw the whole directory can tell which generations Ray has
	// dropped, so a partial walk leaves the uploaded set alone.
	if walkComplete {
		r.pruneRotatedUploaded(objectPrefix, seen)
	}

	for _, absPath := range active {
		if stopRequested(stop) {
			logrus.Debug("Shutdown signaled, ending active log scan early")
			return
		}
		if err := r.collectActiveLog(absPath, logsDir, sessionID, nodeID); err != nil {
			logrus.Errorf("Failed to collect active log %s: %v", absPath, err)
		}
	}
}
