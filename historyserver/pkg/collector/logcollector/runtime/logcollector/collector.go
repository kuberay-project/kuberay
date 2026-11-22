package logcollector

import (
	"fmt"
	"io/fs"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/sirupsen/logrus"

	"github.com/ray-project/kuberay/historyserver/pkg/storage"
	"github.com/ray-project/kuberay/historyserver/pkg/storage/clustermetadata"
	"github.com/ray-project/kuberay/historyserver/pkg/utils"
)

type RayLogHandler struct {
	Writer                 storage.StorageWriter
	LogFiles               chan string
	HttpClient             *http.Client
	ShutdownChan           chan struct{}
	logFilePaths           map[string]bool
	ClusterDir             string
	RayClusterName         string
	LogDir                 string
	RayNodeName            string
	RayClusterNamespace    string
	OwnerKind              string
	OwnerName              string
	RootDir                string
	SessionDir             string
	prevLogsDir            string
	persistCompleteLogsDir string
	PushInterval           time.Duration
	LogBatching            int
	IsHead                 bool
	DashboardAddress       string
	AdditionalEndpoints    []string
	EndpointPollInterval   time.Duration
	LogUploadInterval      time.Duration
	mu                     sync.RWMutex
	// rotatedMu serializes rotated log uploads so the periodic scan, shutdown
	// and prev-logs paths cannot upload one generation twice.
	rotatedMu       sync.Mutex
	rotatedUploaded map[string]struct{}
	// activeLogs is where the last upload stopped reading each chunked log.
	// Guarded by activeMu as the periodic scan, the shutdown walk and the
	// prev-logs walk may update it concurrently.
	activeMu   sync.Mutex
	activeLogs map[activeLogKey]tailState
}

func (r *RayLogHandler) GetRayNodeName() string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.RayNodeName
}

func (r *RayLogHandler) SetRayNodeName(newNodeID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.RayNodeName != newNodeID {
		logrus.Infof("RayLogHandler: updated node ID: %s -> %s", r.RayNodeName, newNodeID)
		r.RayNodeName = newNodeID
	}
}

func (r *RayLogHandler) Run(stop <-chan struct{}) error {
	// watchPath := r.LogDir
	r.prevLogsDir = utils.GetRayPrevLogsPath()
	r.persistCompleteLogsDir = utils.GetRayPersistCompletePath()

	// Initialize log file paths storage
	r.logFilePaths = make(map[string]bool)

	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		logrus.Fatalf("Create fsnotify NewWatcher error %v", err)
	}
	defer watcher.Close()

	// WatchPrevLogsLoops performs an initial scan of the prev-logs directory on startup
	// to process leftover log files in prev-logs/{sessionID}/{nodeID}/logs/ directories.
	// After scanning, it watches for new directories and files. This ensures incomplete
	// uploads from previous runs are resumed.
	go r.WatchPrevLogsLoops()
	go r.PollActiveSessionChanges()

	sessionScanStopped := make(chan struct{})
	go func() {
		defer close(sessionScanStopped)
		r.scanSessionLogs(stop)
	}()
	var periodicPollResults <-chan periodicPollResult
	if r.IsHead {
		go r.WatchSessionLatestLoops() // Watch session_latest symlink changes
		go r.FetchAndStoreClusterMetadata()
		go r.FetchAndStoreTimezone()
		// Driven by stop rather than ShutdownChan, which closes only after the final
		// poll below: a tick in between would overwrite that final snapshot.
		periodicPollResults = r.startPeriodicEndpointPolling(stop)
	}

	<-stop
	logrus.Info("Received stop signal, processing all logs...")

	// Endpoint data dies with the dashboard; log files stay on disk, so poll concurrently.
	var wg sync.WaitGroup
	if r.IsHead {
		// Reuse the periodic poller's state only after it exits and transfers ownership.
		periodicResult, joined := waitForPeriodicPollResult(periodicPollResults, periodicPollJoinTimeout)
		if !joined {
			logrus.Warn("Periodic endpoint poller still busy, starting the final poll anyway")
		}
		wg.Go(func() {
			r.processAdditionalEndpoints(periodicResult)
		})
	}
	// Join the scanner before the final collection so no scan outlives the collector.
	<-sessionScanStopped
	r.processSessionLatestLogs()
	wg.Wait()

	// Signal background goroutines after the synchronous shutdown flushes complete.
	close(r.ShutdownChan)

	return nil
}

// processSessionLatestLogs processes logs in the configured session_latest/logs directory
// on shutdown, using the real session ID and node ID
func (r *RayLogHandler) processSessionLatestLogs() {
	logrus.Info("Processing session_latest logs on shutdown...")

	// Resolve the session_latest symlink to get the real session directory
	sessionLatestDir := utils.GetRaySessionLatestPath()
	sessionRealDir, err := filepath.EvalSymlinks(sessionLatestDir)
	if err != nil {
		logrus.Errorf("Failed to resolve session_latest symlink: %v", err)
		return
	}

	// Extract the real session ID from the resolved path
	sessionID := filepath.Base(sessionRealDir)
	if r.IsHead {
		if err := r.writeSessionMarker(sessionID); err != nil {
			logrus.Errorf("Failed to write session marker for %s: %v", sessionID, err)
			return
		}
	}

	// Use already discovered node ID (RayNodeName) instead of retrying network requests during shutdown
	nodeID := strings.TrimSpace(r.GetRayNodeName())

	// Process logs in session_latest/logs
	logsDir := filepath.Join(sessionLatestDir, utils.RAY_SESSIONDIR_LOGDIR_NAME)
	dirExist := false
	for range 10 {
		if _, err := os.Stat(logsDir); os.IsNotExist(err) {
			logrus.Warnf("Logs directory does not exist: %s", logsDir)
			time.Sleep(time.Millisecond * 10)
		} else {
			dirExist = true
			break
		}
	}
	if !dirExist {
		logrus.Errorf("Logs directory does not exist after 10 attempts: %s", logsDir)
		return
	}

	rotatedObjectPrefix := r.rotatedObjectPrefix(sessionID, nodeID)

	// Walk through the logs directory and process all files
	err = filepath.WalkDir(logsDir, func(path string, info fs.DirEntry, err error) error {
		if err != nil {
			logrus.Errorf("Error walking logs path %s: %v", path, err)
			return nil
		}

		// Skip non-regular files (e.g. symlinks, directories, sockets, devices)
		if !info.Type().IsRegular() {
			return nil
		}

		if r.collectIfRotatedLog(path, logsDir, rotatedObjectPrefix) {
			return nil
		}

		// Only upload the tail of chunked files; upload others in full.
		if err := r.collectActiveLog(path, logsDir, sessionID, nodeID); err != nil {
			logrus.Errorf("Failed to upload session_latest log file %s: %v", path, err)
		}

		return nil
	})
	if err != nil {
		logrus.Errorf("Error walking logs directory %s: %v", logsDir, err)
	}

	logrus.Info("Finished processing session_latest logs")
}

func (r *RayLogHandler) WatchPrevLogsLoops() {
	watchPath := r.prevLogsDir

	// Check if prev-logs directory exists
	if _, err := os.Stat(watchPath); os.IsNotExist(err) {
		logrus.Infof("prev-logs directory does not exist, creating it: %s", watchPath)
		if err := os.MkdirAll(watchPath, 0o777); err != nil {
			logrus.Errorf("Failed to create prev-logs directory %s: %v", watchPath, err)
			return
		}
		if err := os.Chmod(watchPath, 0o777); err != nil {
			logrus.Errorf("Failed to create prev-logs directory %s: %v", watchPath, err)
			return
		}
	}

	// Also check and create persist-complete-logs directory
	completeLogsDir := r.persistCompleteLogsDir
	if _, err := os.Stat(completeLogsDir); os.IsNotExist(err) {
		logrus.Infof("persist-complete-logs directory does not exist, creating it: %s", completeLogsDir)
		if err := os.MkdirAll(completeLogsDir, 0o777); err != nil {
			logrus.Errorf("Failed to create persist-complete-logs directory %s: %v", completeLogsDir, err)
			return
		}
		if err := os.Chmod(completeLogsDir, 0o777); err != nil {
			logrus.Errorf("Failed to create prev-logs directory %s: %v", completeLogsDir, err)
			return
		}
	}

	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		logrus.Errorf("Failed to create fsnotify watcher for prev-logs: %v", err)
		return
	}
	defer watcher.Close()

	sessionWatcher, err := fsnotify.NewWatcher()
	if err != nil {
		logrus.Errorf("Failed to create fsnotify watcher for prev-logs sessions: %v", err)
		return
	}
	defer sessionWatcher.Close()

	nodeWatcher, err := fsnotify.NewWatcher()
	if err != nil {
		logrus.Errorf("Failed to create fsnotify watcher for prev-logs nodeWatcher: %v", err)
		return
	}
	defer nodeWatcher.Close()

	// Add the root prev-logs directory to watcher
	if err := watcher.Add(watchPath); err != nil {
		logrus.Errorf("Failed to add %s to watcher: %v", watchPath, err)
		return
	}

	// Walk through existing directories and add them to watcher
	err = filepath.WalkDir(watchPath, func(path string, info fs.DirEntry, err error) error {
		if err != nil {
			logrus.Errorf("Error walking path %s: %v", path, err)
			return nil
		}

		if info.IsDir() && path != watchPath {
			// Check if this is a session directory (direct subdirectory of prev-logs)
			rel, err := filepath.Rel(watchPath, path)
			if err != nil {
				logrus.Errorf("Failed to get relative path for %s: %v", path, err)
				return nil
			}

			// Split the relative path to check depth
			parts := strings.Split(filepath.ToSlash(rel), "/")

			switch len(parts) {
			case 1: // Session directory
				if err := sessionWatcher.Add(path); err != nil {
					logrus.Errorf("Failed to add session directory %s to sessionWatcher: %v", path, err)
				}
				// Process all existing node directories under this session
				r.processSessionPrevLogs(path)
				// case 2: // Node directory
				// 	if err := nodeWatcher.Add(path); err != nil {
				// 		logrus.Errorf("Failed to add node directory %s to nodeWatcher: %v", path, err)
				// 	}
				// 	// Check if this node directory has a logs subdirectory
				// 	logsDir := filepath.Join(path, utils.RAY_SESSIONDIR_LOGDIR_NAME)
				// 	if _, err := os.Stat(logsDir); err == nil {
				// 		// This is a session/node directory with logs, process its logs
				// 		go r.processPrevLogsDir(path)
				// 	}
			}
		}
		return nil
	})
	if err != nil {
		logrus.Errorf("Error walking prev-logs directory: %v", err)
		return
	}

	logrus.Infof("Started watching prev-logs directory: %s", watchPath)

	for {
		select {
		case <-r.ShutdownChan:
			logrus.Info("Received shutdown signal, stopping prev-logs watcher")
			return
		case event, ok := <-watcher.Events:
			if !ok {
				logrus.Warn("Prev-logs watcher events channel closed")
				return
			}

			logrus.Infof("File system event in prev-logs: %s %s", event.Op, event.Name)

			// Handle new directories being created
			if event.Op&fsnotify.Create == fsnotify.Create {
				info, err := os.Stat(event.Name)
				if err != nil {
					logrus.Errorf("Failed to stat %s: %v", event.Name, err)
					continue
				}

				if info.IsDir() {
					// Add new directory to sessionWatcher
					if err := sessionWatcher.Add(event.Name); err != nil {
						logrus.Errorf("Failed to add %s to sessionWatcher: %v", event.Name, err)
					}
					// Process all existing node directories under this new session
					r.processSessionPrevLogs(event.Name)
				}
			}
		case event, ok := <-sessionWatcher.Events:
			if !ok {
				logrus.Warn("Session watcher events channel closed")
				return
			}

			logrus.Infof("File system event in session directory: %s %s", event.Op, event.Name)

			// Handle new directories being created in session directories
			if event.Op&fsnotify.Create == fsnotify.Create {
				info, err := os.Stat(event.Name)
				if err != nil {
					logrus.Errorf("Failed to stat %s: %v", event.Name, err)
					continue
				}

				if info.IsDir() {
					// Add new directory to nodeWatcher
					if err := nodeWatcher.Add(event.Name); err != nil {
						logrus.Errorf("Failed to add %s to nodeWatcher: %v", event.Name, err)
					}

					// Check if this is a node directory by examining its path
					rel, err := filepath.Rel(watchPath, event.Name)
					if err != nil {
						logrus.Errorf("Failed to get relative path for %s: %v", event.Name, err)
						continue
					}

					parts := strings.Split(filepath.ToSlash(rel), "/")
					if len(parts) == 2 { // This is a node directory
						// Check if logs subdirectory exists
						go r.processPrevLogsDir(event.Name)
					}
				}
			}
		case event, ok := <-nodeWatcher.Events:
			if !ok {
				logrus.Warn("Node watcher events channel closed")
				return
			}

			logrus.Debugf("File system event in node directory: %s %s", event.Op, event.Name)

			// Handle new directories or files being created in node directories
			if event.Op&fsnotify.Create == fsnotify.Create {
				info, err := os.Stat(event.Name)
				if err != nil {
					logrus.Errorf("Failed to stat %s: %v", event.Name, err)
					continue
				}

				// Check if this is a logs directory being created
				base := filepath.Base(event.Name)
				if info.IsDir() && base == utils.RAY_SESSIONDIR_LOGDIR_NAME {
					// This is a logs directory, process its parent (node directory)
					nodeDir := filepath.Dir(event.Name)
					logrus.Infof("New logs directory detected: %s", nodeDir)
					go r.processPrevLogsDir(nodeDir)
				}
			}
		case err, ok := <-watcher.Errors:
			if !ok {
				logrus.Warn("Prev-logs watcher errors channel closed")
				return
			}
			logrus.Errorf("Prev-logs watcher error: %v", err)
		case err, ok := <-sessionWatcher.Errors:
			if !ok {
				logrus.Warn("Session watcher errors channel closed")
				return
			}
			logrus.Errorf("Session watcher error: %v", err)
		case err, ok := <-nodeWatcher.Errors:
			if !ok {
				logrus.Warn("Node watcher errors channel closed")
				return
			}
			logrus.Errorf("Node watcher error: %v", err)
		}
	}
}

// processSessionPrevLogs processes all node logs under a session directory
func (r *RayLogHandler) processSessionPrevLogs(sessionDir string) {
	// Check if this is actually a session directory (one level deep under prev-logs)
	watchPath := r.prevLogsDir
	rel, err := filepath.Rel(watchPath, sessionDir)
	if err != nil {
		logrus.Errorf("Failed to get relative path for session directory %s: %v", sessionDir, err)
		return
	}

	parts := strings.Split(filepath.ToSlash(rel), "/")
	if len(parts) != 1 {
		// Not a session directory, skip
		return
	}

	sessionID := parts[0]
	logrus.Infof("Processing all node logs for session: %s", sessionID)
	if r.IsHead {
		if err := r.writeSessionMarker(sessionID); err != nil {
			logrus.Errorf("Failed to write session marker for %s: %v", sessionID, err)
			return
		}
	}

	// Walk through all node directories under this session
	err = filepath.WalkDir(sessionDir, func(path string, info fs.DirEntry, err error) error {
		if err != nil {
			logrus.Errorf("Error walking path %s: %v", path, err)
			return nil
		}

		if info.IsDir() && path != sessionDir {
			// Check if this is a node directory (two levels deep under prev-logs)
			logrus.Infof("found node session logs in directory: %v", path)
			rel, err := filepath.Rel(watchPath, path)
			if err != nil {
				logrus.Errorf("Failed to get relative path for node directory %s: %v", path, err)
				return nil
			}

			parts := strings.Split(filepath.ToSlash(rel), "/")
			if len(parts) == 2 {
				go r.processPrevLogsDir(path)
			}
		}
		return nil
	})
	if err != nil {
		logrus.Errorf("Error walking session directory %s: %v", sessionDir, err)
	}
}

// isFileAlreadyPersisted checks if a log file has already been uploaded to storage and moved to
// the persist-complete-logs directory. This prevents duplicate uploads during collector restarts.
//
// When a log file is successfully uploaded, it is moved from prev-logs to persist-complete-logs
// to mark it as processed. This function checks if the equivalent file path exists in the
// persist-complete-logs directory.
//
// Example:
//
//	Given absoluteLogPath = "/tmp/ray/prev-logs/session_123/node_456/logs/raylet.out"
//	This function checks if "/tmp/ray/persist-complete-logs/session_123/node_456/logs/raylet.out" exists
//	- If exists: returns true (file was already uploaded, skip it)
//	- If not exists: returns false (file needs to be uploaded)
func (r *RayLogHandler) isFileAlreadyPersisted(absoluteLogPath, sessionID, nodeID string) bool {
	// Calculate the relative path within the logs directory
	logsDir := filepath.Join(r.prevLogsDir, sessionID, nodeID, utils.RAY_SESSIONDIR_LOGDIR_NAME)
	relativeLogPath, err := filepath.Rel(logsDir, absoluteLogPath)
	if err != nil {
		logrus.Errorf("Failed to get relative path for %s: %v", absoluteLogPath, err)
		return false
	}

	// Construct the path in persist-complete-logs
	persistedPath := filepath.Join(r.persistCompleteLogsDir, sessionID, nodeID, utils.RAY_SESSIONDIR_LOGDIR_NAME, relativeLogPath)

	// Check if the file exists
	if _, err := os.Stat(persistedPath); err == nil {
		return true
	}
	return false
}

// processPrevLogsDir processes logs in a /tmp/ray/prev-logs/{sessionid}/{nodeid} directory
func (r *RayLogHandler) processPrevLogsDir(sessionNodeDir string) {
	// Extract session ID and node ID from the path
	// Path format: /tmp/ray/prev-logs/{sessionid}/{nodeid}
	parts := strings.Split(sessionNodeDir, string(filepath.Separator))
	if len(parts) < 2 {
		logrus.Errorf("Invalid path format for sessionNodeDir: %s", sessionNodeDir)
		return
	}

	// Extract nodeID and sessionID from the path
	// Path is like: /tmp/ray/prev-logs/sessionid/nodeid
	nodeID := parts[len(parts)-1]
	sessionID := parts[len(parts)-2]

	// Validate that we're not processing the root prev-logs directory
	if sessionID == "prev-logs" {
		logrus.Debugf("Skipping root prev-logs directory")
		return
	}

	logrus.Infof("Processing prev-logs for session: %s, node: %s", sessionID, nodeID)

	logsDir := filepath.Join(sessionNodeDir, utils.RAY_SESSIONDIR_LOGDIR_NAME)
	dirExist := false
	for range 10 {
		if _, err := os.Stat(logsDir); os.IsNotExist(err) {
			logrus.Warnf("Logs directory does not exist: %s", logsDir)
			time.Sleep(time.Millisecond * 10)
		} else {
			dirExist = true
			break
		}
	}
	if !dirExist {
		logrus.Errorf("Logs directory does not exist after 10 attempts: %s", logsDir)
		return
	}

	rotatedObjectPrefix := r.rotatedObjectPrefix(sessionID, nodeID)

	// Walk through the logs directory and process all files
	err := filepath.WalkDir(logsDir, func(path string, info fs.DirEntry, err error) error {
		if err != nil {
			logrus.Errorf("Error walking logs path %s: %v", path, err)
			return nil
		}

		// Skip non-regular files (e.g. symlinks, directories, sockets, devices)
		if !info.Type().IsRegular() {
			return nil
		}

		if r.collectIfRotatedLog(path, logsDir, rotatedObjectPrefix) {
			return nil
		}

		// Check if this file has already been persisted
		if r.isFileAlreadyPersisted(path, sessionID, nodeID) {
			logrus.Debugf("File %s already persisted, skipping", path)
			return nil
		}

		// Process log file
		if err := r.processPrevLogFile(path, logsDir, sessionID, nodeID); err != nil {
			logrus.Errorf("Failed to process prev-log file %s: %v", path, err)
		}

		return nil
	})
	if err != nil {
		logrus.Errorf("Error walking logs directory %s: %v", logsDir, err)
		return
	}

	// After successfully processing all files, remove the node directory
	logrus.Infof("Finished processing all logs for session: %s, node: %s. Removing node directory.", sessionID, nodeID)
	if err := os.RemoveAll(sessionNodeDir); err != nil {
		logrus.Errorf("Failed to remove node directory %s: %v", sessionNodeDir, err)
		// Keep this session's dedup entries so a retry does not re-upload.
		return
	}
	logrus.Infof("Successfully removed node directory: %s", sessionNodeDir)
	// The directory is gone, so these entries can never be seen again.
	r.pruneRotatedUploaded(rotatedObjectPrefix, nil)
}

// processPrevLogFile uploads one prev-log file (chunked files only their tail,
// others in full) and then moves it to persist-complete-logs so a collector
// restart skips it.
func (r *RayLogHandler) processPrevLogFile(absoluteLogPathName, localLogDir, sessionID, nodeID string) error {
	if err := r.collectActiveLog(absoluteLogPathName, localLogDir, sessionID, nodeID); err != nil {
		return err
	}

	// The localLogDir is /tmp/ray/prev-logs/{sessionid}/{nodeid}/logs
	relativePath, err := filepath.Rel(localLogDir, absoluteLogPathName)
	if err != nil {
		return fmt.Errorf("failed to get relative path for %s: %w", absoluteLogPathName, err)
	}
	subdir, _ := filepath.Split(relativePath)

	// Move the processed file to persist-complete-logs directory to avoid re-uploading
	completeBaseDir := filepath.Join(r.persistCompleteLogsDir, sessionID, nodeID)
	completeDir := filepath.Join(completeBaseDir, utils.RAY_SESSIONDIR_LOGDIR_NAME)

	if _, err := os.Stat(completeDir); os.IsNotExist(err) {
		// Create the target directory if it doesn't exist
		if err := os.MkdirAll(completeDir, 0o777); err != nil {
			logrus.Errorf("Failed to create complete logs directory %s: %v", completeDir, err)
			return nil // Don't fail the whole process if we can't move the file
		}
		if err := os.Chmod(completeDir, 0o777); err != nil {
			logrus.Errorf("Failed to chmod complete logs directory %s: %v", completeDir, err)
			return nil // Don't fail the whole process if we can't move the file
		}
	}

	// Construct the target file path
	targetFilePath := filepath.Join(completeDir, relativePath)
	targetFileDir := filepath.Dir(targetFilePath)

	// Create subdirectory in target location if needed
	if subdir != "" && subdir != "." {
		if _, err := os.Stat(targetFileDir); os.IsNotExist(err) {
			if err := os.MkdirAll(targetFileDir, 0o777); err != nil {
				logrus.Errorf("Failed to create target subdirectory %s: %v", targetFileDir, err)
				return nil
			}
			if err := os.Chmod(targetFileDir, 0o777); err != nil {
				logrus.Errorf("Failed to chmod complete logs directory %s: %v", targetFileDir, err)
				return nil // Don't fail the whole process if we can't move the file
			}
		}
	}

	// Move the file
	if err := os.Rename(absoluteLogPathName, targetFilePath); err != nil {
		logrus.Errorf("Failed to move file from %s to %s: %v", absoluteLogPathName, targetFilePath, err)
	} else {
		logrus.Infof("Moved processed file from %s to %s", absoluteLogPathName, targetFilePath)
	}

	return nil
}

// Any session change triggers sessiondir updates on all head and worker nodes,
// so we only need to update from one node.
// for example:
//
//	my-cluster_abc123/
//		session_2024-12-15_10-30-45_123456    ← Empty file! The path itself is the information
//		session_2024-12-15_14-20-10_789012
func (r *RayLogHandler) WatchSessionLatestLoops() {
	sessionLatestDir := utils.GetTmpRayRoot()
	sessionLatestSymlink := filepath.Join(sessionLatestDir, "session_latest")
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		logrus.Errorf("Failed to create fsnotify watcher for session_latest: %v", err)
		return
	}
	defer watcher.Close()

	// Add the session_latest directory to watcher
	if err := watcher.Add(sessionLatestDir); err != nil {
		logrus.Errorf("Failed to add %s to watcher: %v", sessionLatestDir, err)
		return
	}

	// Write the marker for the session that already exists when the collector
	// starts up; the watcher below only sees sessions created after it.
	if sessionDir, err := filepath.EvalSymlinks(sessionLatestSymlink); err == nil {
		if err := r.writeSessionMarker(filepath.Base(sessionDir)); err != nil {
			logrus.Errorf("Failed to write session marker for %s: %v", sessionDir, err)
		}
	}

	logrus.Infof("Started watching session_latest directory: %s", sessionLatestDir)
	for {
		select {
		case <-r.ShutdownChan:
			logrus.Info("Received shutdown signal, stopping session_latest watcher")
			return
		case event, ok := <-watcher.Events:
			if !ok {
				logrus.Warn("Session latest watcher events channel closed")
				return
			}

			logrus.Infof("File system event in session_latest: %s %s", event.Op, event.Name)
			if event.Name == sessionLatestSymlink {
				continue
			}
			rel, err := filepath.Rel(sessionLatestDir, event.Name)
			if err != nil {
				logrus.Errorf("Failed to get relative path for %s: %s", event.Name, err)
				continue
			}
			if !strings.Contains(rel, "session_") {
				logrus.Infof("Skip to process file: %s", event.Name)
				continue
			}

			// Handle changes to the symlink
			if event.Op&(fsnotify.Create|fsnotify.Write) != 0 {
				if err := r.writeSessionMarker(filepath.Base(event.Name)); err != nil {
					logrus.Errorf("Failed to write session marker for %s: %v", event.Name, err)
					return
				}
			}
		case err, ok := <-watcher.Errors:
			if !ok {
				logrus.Warn("Session latest watcher errors channel closed")
				return
			}
			logrus.Errorf("Session latest watcher error: %v", err)
		}
	}
}

// writeSessionMarker writes the empty object the History Server lists sessions
// by (see clustermetadata.EncodePath).
func (r *RayLogHandler) writeSessionMarker(sessionID string) error {
	metafile := clustermetadata.EncodePath(
		utils.ClusterInfo{
			Name:      r.RayClusterName,
			Namespace: r.RayClusterNamespace,
			OwnerKind: r.OwnerKind,
			OwnerName: r.OwnerName,
		},
		r.RootDir,
		sessionID,
	)
	if err := r.Writer.CreateDirectory(path.Dir(metafile)); err != nil {
		return fmt.Errorf("failed to create directory %s: %w", path.Dir(metafile), err)
	}
	if err := r.Writer.WriteFile(metafile, strings.NewReader("")); err != nil {
		return fmt.Errorf("failed to write session file %s: %w", metafile, err)
	}
	return nil
}

// Polls if the active session changes, when it does, it moves the old session logs to a prev-logs/ folder.
func (r *RayLogHandler) PollActiveSessionChanges() {
	tmpRayRoot := utils.GetTmpRayRoot()
	symlinkPath := filepath.Join(tmpRayRoot, "session_latest")

	var lastResolvedDir string
	currentActiveDir, err := filepath.EvalSymlinks(symlinkPath)
	if err == nil && currentActiveDir != "" {
		if r.SessionDir != "" && currentActiveDir != r.SessionDir {
			logrus.Infof("PollActiveSessionChanges: detected startup session change from %s to %s. Relocating startup session logs.", r.SessionDir, currentActiveDir)
			if err := utils.MoveLeftoverSessionLogs(currentActiveDir, r.GetRayNodeName()); err != nil {
				logrus.Warnf("PollActiveSessionChanges: failed to relocate startup session logs: %v. Retrying on next poll tick.", err)
				lastResolvedDir = r.SessionDir
			} else {
				lastResolvedDir = currentActiveDir
			}
		} else {
			lastResolvedDir = currentActiveDir
		}
	} else {
		logrus.Warnf("PollActiveSessionChanges: failed to resolve initial session_latest target: %v. Falling back to startup SessionDir %s", err, r.SessionDir)
		lastResolvedDir = r.SessionDir
	}

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	logrus.Infof("Started polling active session changes at: %s (initial target: %s)", symlinkPath, lastResolvedDir)
	for {
		select {
		case <-r.ShutdownChan:
			logrus.Info("PollActiveSessionChanges: stopping active session poller")
			return
		case <-ticker.C:
			newResolvedDir, err := filepath.EvalSymlinks(symlinkPath)
			if err != nil || newResolvedDir == "" {
				continue
			}

			if lastResolvedDir != "" && newResolvedDir != lastResolvedDir {
				logrus.Infof("PollActiveSessionChanges: session changed from %s to %s. Relocating old logs.", lastResolvedDir, newResolvedDir)
				if err := utils.MoveLeftoverSessionLogs(newResolvedDir, r.GetRayNodeName()); err != nil {
					logrus.Warnf("PollActiveSessionChanges: failed to relocate leftover session logs from %s to %s: %v. Retrying on next tick.", lastResolvedDir, newResolvedDir, err)
					continue
				}
			}
			lastResolvedDir = newResolvedDir

			if freshNodeID, err := utils.FetchCurrentNodeID(); err == nil && freshNodeID != "" {
				if hexID, err := utils.ConvertBase64ToHex(freshNodeID); err == nil && hexID != "" {
					r.SetRayNodeName(hexID)
				}
			}
		}
	}
}
