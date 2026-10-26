package historyserver

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"path"
	"regexp"
	"sort"
	"strings"

	"github.com/bmatcuk/doublestar/v4"
	"github.com/sirupsen/logrus"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ray-project/kuberay/historyserver/pkg/compression"
	"github.com/ray-project/kuberay/historyserver/pkg/eventserver"
	eventtypes "github.com/ray-project/kuberay/historyserver/pkg/eventserver/types"
	"github.com/ray-project/kuberay/historyserver/pkg/storage/clusterlogs"
	"github.com/ray-project/kuberay/historyserver/pkg/utils"
	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	rayutils "github.com/ray-project/kuberay/ray-operator/controllers/ray/utils"
)

const (
	// DEFAULT_LOG_LIMIT is the default number of lines to return when lines parameter is not specified or is 0.
	// This matches Ray Dashboard API default behavior.
	DEFAULT_LOG_LIMIT = 1000

	// DEFAULT_DOWNLOAD_FILENAME is the default filename when download_filename is not specified
	DEFAULT_DOWNLOAD_FILENAME = "file.txt"

	// MAX_LOG_LIMIT is the maximum number of lines that can be requested.
	// Requests exceeding this limit will be capped to this value.
	MAX_LOG_LIMIT = 10000
)

// ANSI escape code pattern for filtering colored output from logs
// Matches patterns like: \x1b[31m (red color), \x1b[0m (reset), etc.
var ansiEscapePattern = regexp.MustCompile(`\x1b\[[0-9;]+m`)

type logByteRange struct {
	start int64
	end   int64
}

// exactRangeReader reads exactly `remaining` bytes from the underlying reader.
// Unlike io.LimitReader, it returns io.ErrUnexpectedEOF if the reader ends
// early, so a truncated log file surfaces as an error instead of silently
// returning a partial task log.
type exactRangeReader struct {
	reader    io.Reader
	remaining int64
}

func (r *exactRangeReader) Read(p []byte) (int, error) {
	if r.remaining == 0 {
		return 0, io.EOF
	}
	if int64(len(p)) > r.remaining {
		p = p[:r.remaining]
	}
	n, err := r.reader.Read(p)
	r.remaining -= int64(n)
	if errors.Is(err, io.EOF) && r.remaining > 0 {
		return n, io.ErrUnexpectedEOF
	}
	return n, err
}

// filterAnsiEscapeCodes removes ANSI escape sequences from log content
func filterAnsiEscapeCodes(content []byte) []byte {
	return ansiEscapePattern.ReplaceAll(content, []byte(""))
}

func buildLiveClusterInfo(liveCluster *rayv1.RayCluster) utils.ClusterInfo {
	var ownerKind, ownerName string
	if ownerRef := metav1.GetControllerOf(liveCluster); ownerRef != nil {
		k := strings.ToLower(ownerRef.Kind)
		// Only set owner info for the Kuberay CRD resources for now.
		if k == utils.RayJobKind || k == utils.RayServiceKind || k == utils.RayClusterKind {
			ownerKind = k
			ownerName = ownerRef.Name
		} else {
			logrus.Warnf("RayCluster %s/%s has an unknown owner controller kind %q, which is currently not supported", liveCluster.Namespace, liveCluster.Name, ownerRef.Kind)
		}
	}
	return utils.ClusterInfo{
		Name:            liveCluster.Name,
		Namespace:       liveCluster.Namespace,
		CreateTime:      liveCluster.CreationTimestamp.String(),
		CreateTimeStamp: liveCluster.CreationTimestamp.Unix(),
		SessionName:     "live",
		OwnerKind:       ownerKind,
		OwnerName:       ownerName,
	}
}

func crdLabelValueFor(kindLower string) string {
	switch kindLower {
	case utils.RayJobKind:
		return "RayJob"
	case utils.RayServiceKind:
		return "RayService"
	case utils.RayClusterKind:
		return "RayCluster"
	default:
		return kindLower
	}
}

func (s *ServerHandler) listClusters(limit int) []utils.ClusterInfo {
	// Initial continuation marker
	logrus.Debugf("Prepare to get list clusters info ...")
	liveClusterInfos := []utils.ClusterInfo{}
	if s.enableLiveClusters {
		ctx := context.Background()
		liveClusterNames := []string{}
		liveClusters, err := s.clientManager.ListRayClusters(ctx)
		if err != nil {
			logrus.Errorf("Failed to list live RayClusters: %v", err)
		}
		for _, liveCluster := range liveClusters {
			liveClusterInfo := buildLiveClusterInfo(liveCluster)
			liveClusterInfos = append(liveClusterInfos, liveClusterInfo)
			liveClusterNames = append(liveClusterNames, liveCluster.Name)
		}
		logrus.Infof("live clusters: %v", liveClusterNames)
	}
	clusters := s.reader.List()
	sort.Sort(utils.ClusterInfoList(clusters))
	if limit > 0 && limit < len(clusters) {
		clusters = clusters[:limit]
	}
	clusters = append(liveClusterInfos, clusters...)

	return clusters
}

// resolveSession maps (namespace, resourceType, resourceName, session) to a concrete ClusterInfo.
func (s *ServerHandler) resolveSession(ctx context.Context, namespace, resourceType, resourceName, session string) (utils.ClusterInfo, bool, error) {
	isLatestOrEmpty := session == "latest" || session == ""
	resTypeLower := strings.ToLower(resourceType)
	if resTypeLower != utils.RayClusterKind && resTypeLower != utils.RayJobKind && resTypeLower != utils.RayServiceKind {
		return utils.ClusterInfo{}, false, fmt.Errorf("unsupported resource kind: %q (must be raycluster, rayjob, or rayservice)", resourceType)
	}

	// "live" is not a valid session while --enable-live-clusters is disabled.
	if session == "live" && !s.enableLiveClusters {
		return utils.ClusterInfo{}, false, nil
	}

	// Check live clusters first if applicable
	if s.enableLiveClusters && (isLatestOrEmpty || session == "live") {
		if resTypeLower == utils.RayClusterKind {
			liveCluster, err := s.clientManager.GetRayCluster(ctx, namespace, resourceName)
			if err == nil {
				return buildLiveClusterInfo(liveCluster), true, nil
			} else if !apierrors.IsNotFound(err) {
				return utils.ClusterInfo{}, false, fmt.Errorf("failed to check live RayCluster %s/%s: %w", namespace, resourceName, err)
			}
		} else {
			// Both labels are needed: name alone is ambiguous since a RayJob and a
			// RayService in the same namespace can share a name.
			liveClusters, err := s.clientManager.ListRayClusters(ctx,
				client.InNamespace(namespace),
				client.MatchingLabels{
					rayutils.RayOriginatedFromCRNameLabelKey: resourceName,
					rayutils.RayOriginatedFromCRDLabelKey:    crdLabelValueFor(resTypeLower),
				},
			)
			if err != nil {
				return utils.ClusterInfo{}, false, fmt.Errorf("failed to list live RayClusters: %w", err)
			}
			// TODO: A RayService owns both an active and a pending cluster during an upgrade. Needs to decide which one to take.
			if len(liveClusters) > 0 {
				info := buildLiveClusterInfo(liveClusters[0])
				if info.OwnerKind == "" {
					info.OwnerKind = resTypeLower
					info.OwnerName = resourceName
				}
				return info, true, nil
			}
		}
		if session == "live" {
			return utils.ClusterInfo{}, false, nil
		}
	}

	// Check stored clusters
	var sessions []utils.ClusterInfo
	for _, c := range s.reader.List() {
		if c.Namespace != namespace {
			continue
		}
		if resTypeLower == utils.RayClusterKind {
			if c.Name != resourceName {
				continue
			}
		} else {
			if strings.ToLower(c.OwnerKind) != resTypeLower || c.OwnerName != resourceName {
				continue
			}
		}

		if c.SessionName == session {
			return c, true, nil
		}
		// If session is latest or "", we fall back to picking the latest dead session.
		if isLatestOrEmpty {
			sessions = append(sessions, c)
		}
	}

	if len(sessions) > 0 {
		sort.Sort(utils.ClusterInfoList(sessions))
		return sessions[0], true, nil
	}

	return utils.ClusterInfo{}, false, nil
}

func (s *ServerHandler) _getNodeLogs(clusterLogPathPrefix, sessionId, nodeId, folder, glob string) ([]byte, error) {
	logPath := clusterlogs.RelLogsDir(sessionId, nodeId)
	if folder != "" {
		logPath = path.Join(logPath, folder)
	}

	// Filter files by glob pattern if provided; otherwise use all files.
	// Use recursive listing when glob contains ** to support cross-directory matching.
	var matchedFiles []string
	if glob == "" {
		matchedFiles = s.reader.ListFiles(clusterLogPathPrefix, logPath)
	} else {
		var files []string
		if strings.Contains(glob, "**") {
			files = s.listFilesRecursive(clusterLogPathPrefix, logPath)
		} else {
			files = s.reader.ListFiles(clusterLogPathPrefix, logPath)
		}
		for _, file := range files {
			matched, err := doublestar.Match(glob, file)
			if err != nil {
				return []byte{}, fmt.Errorf("invalid glob pattern %q matching against %q: %w", glob, file, err)
			}
			if matched {
				matchedFiles = append(matchedFiles, file)
			}
		}
	}

	// Categorize log files to match Ray Dashboard API format.
	// Ref: Ray Dashboard's LogsManager._categorize_log_files in log_manager.py
	categorized := categorizeLogFiles(matchedFiles)

	ret := map[string]any{
		"result": true,
		"msg":    "",
		"data": map[string]any{
			"result": categorized,
		},
	}
	return json.Marshal(ret)
}

// listFilesRecursive recursively lists all files under dir,
// returning paths relative to dir (e.g. "subdir/foo.log", "bar.out").
// It recurses into subdirectories returned by ListFiles (identified by a trailing "/").
func (s *ServerHandler) listFilesRecursive(prefix, dir string) []string {
	entries := s.reader.ListFiles(prefix, dir)
	var result []string
	for _, entry := range entries {
		if strings.HasSuffix(entry, "/") {
			subDir := path.Join(dir, entry)
			subFiles := s.listFilesRecursive(prefix, subDir)
			for _, f := range subFiles {
				result = append(result, path.Join(strings.TrimSuffix(entry, "/"), f))
			}
		} else {
			result = append(result, entry)
		}
	}
	return result
}

// categorizeLogFiles categorizes log files by component type.
// This mirrors Ray Dashboard's LogsManager._categorize_log_files logic.
// Ref: https://github.com/ray-project/ray/blob/master/python/ray/dashboard/modules/log/log_manager.py
//
// NOTE: The order of checks matters. "log_monitor" must be checked before "monitor"
// because "log_monitor" contains "monitor" as a substring.
func categorizeLogFiles(files []string) map[string][]string {
	result := make(map[string][]string)
	for _, f := range files {
		switch {
		case strings.Contains(f, "worker") && strings.HasSuffix(f, ".out"):
			result["worker_out"] = append(result["worker_out"], f)
		case strings.Contains(f, "worker") && strings.HasSuffix(f, ".err"):
			result["worker_err"] = append(result["worker_err"], f)
		case strings.Contains(f, "core-worker") && strings.HasSuffix(f, ".log"):
			result["core_worker"] = append(result["core_worker"], f)
		case strings.Contains(f, "core-driver") && strings.HasSuffix(f, ".log"):
			result["driver"] = append(result["driver"], f)
		case strings.Contains(f, "raylet."):
			result["raylet"] = append(result["raylet"], f)
		case strings.Contains(f, "gcs_server."):
			result["gcs_server"] = append(result["gcs_server"], f)
		case strings.Contains(f, "log_monitor"):
			result["internal"] = append(result["internal"], f)
		case strings.Contains(f, "monitor"):
			result["autoscaler"] = append(result["autoscaler"], f)
		case strings.Contains(f, "agent."):
			result["agent"] = append(result["agent"], f)
		case strings.Contains(f, "dashboard."):
			result["dashboard"] = append(result["dashboard"], f)
		default:
			result["internal"] = append(result["internal"], f)
		}
	}
	return result
}

func (s *ServerHandler) _getNodeLogFile(clusterSessionKey, clusterLogPathPrefix, sessionID string, options GetLogFileOptions) ([]byte, error) {
	// Resolve node_id and filename based on options
	nodeID, filename, byteRange, err := s.resolveLogFilename(clusterSessionKey, clusterLogPathPrefix, sessionID, options)
	if err != nil {
		// Preserve HTTPError status code if already set, otherwise use BadRequest
		var httpErr *utils.HTTPError
		if errors.As(err, &httpErr) {
			return nil, err
		}
		return nil, utils.NewHTTPError(err, http.StatusBadRequest)
	}

	// Build log path using clusterlogs helper (<nodeID>/<sessionID>/logs/<filename>)
	logPath := path.Join(clusterlogs.RelLogsDir(sessionID, nodeID), filename)
	reader := s.reader.GetContent(clusterLogPathPrefix, logPath)

	if reader == nil {
		return nil, utils.NewHTTPError(fmt.Errorf("log file not found: %s", logPath), http.StatusNotFound)
	}
	reader, err = applyLogByteRange(reader, byteRange)
	if err != nil {
		return nil, err
	}

	maxLines := options.Lines
	if maxLines < 0 {
		// -1 means read all lines
		content, err := io.ReadAll(reader)
		if err != nil {
			return nil, err
		}
		if options.FilterAnsiCode {
			content = filterAnsiEscapeCodes(content)
		}
		return content, nil
	}

	if maxLines == 0 {
		maxLines = DEFAULT_LOG_LIMIT
	}

	if maxLines > MAX_LOG_LIMIT {
		logrus.Warnf("Requested lines (%d) exceeds max limit (%d), capping to max", maxLines, MAX_LOG_LIMIT)
		maxLines = MAX_LOG_LIMIT
	}

	scanner := bufio.NewScanner(reader)
	buffer := make([]string, maxLines)
	index := 0
	totalLines := 0

	// TODO(nary): Optimize this to prevent scanning through the whole log file to get last n lines
	// Get the last N lines following Ray Dashboard API behavior with circular buffer
	// Example with maxLines=3, file has 5 lines:
	// 	Line 1: buffer[0], Line 2: buffer[1], Line 3: buffer[2]
	// 	Line 4: buffer[0] (overwrites Line 1), Line 5: buffer[1] (overwrites Line 2)
	// 	Final buffer: ["Line 4", "Line 5", "Line 3"]
	for scanner.Scan() {
		buffer[index%maxLines] = scanner.Text()
		index++
		totalLines++
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	// Reconstruct lines in correct order
	var lines []string
	if totalLines <= maxLines {
		// File has fewer lines than requested, return all
		lines = buffer[:totalLines]
	} else {
		// Construct response with correct log line order from the circular buffer
		// Example: buffer=["Line 4", "Line 5", "Line 3"], start=2
		//   buffer[2:] = ["Line 3"] (oldest)
		//   buffer[:2] = ["Line 4", "Line 5"] (newest)
		//   Result: ["Line 3", "Line 4", "Line 5"]
		start := index % maxLines
		lines = append(buffer[start:], buffer[:start]...)
	}

	result := []byte(strings.Join(lines, "\n"))
	if options.FilterAnsiCode {
		result = filterAnsiEscapeCodes(result)
	}

	return result, nil
}

func applyLogByteRange(reader io.Reader, byteRange *logByteRange) (io.Reader, error) {
	if byteRange == nil {
		return reader, nil
	}
	if byteRange.start < 0 || byteRange.end < 0 || byteRange.end < byteRange.start {
		return nil, fmt.Errorf("invalid task log byte range [%d,%d)", byteRange.start, byteRange.end)
	}

	if byteRange.start > 0 {
		if _, err := io.CopyN(io.Discard, reader, byteRange.start); err != nil {
			return nil, fmt.Errorf("task log ended before byte range start %d: %w", byteRange.start, err)
		}
	}

	return &exactRangeReader{reader: reader, remaining: byteRange.end - byteRange.start}, nil
}

// resolveLogFilename resolves the log file node_id and filename based on the provided options.
// This mirrors Ray Dashboard's resolve_filename logic.
// The sessionID parameter is required for task_id resolution to search worker log files.
func (s *ServerHandler) resolveLogFilename(clusterSessionKey, clusterLogPathPrefix, sessionID string, options GetLogFileOptions) (nodeID, filename string, byteRange *logByteRange, err error) {
	// If filename is explicitly provided, use it and ignore suffix
	if options.Filename != "" {
		if options.NodeID == "" {
			return "", "", nil, fmt.Errorf("node_id is required when filename is provided")
		}
		filename := options.Filename
		// Append attempt_number if specified for explicit filenames
		if options.AttemptNumber > 0 {
			filename = fmt.Sprintf("%s.%d", filename, options.AttemptNumber)
		}
		return options.NodeID, filename, nil, nil
	}

	// If task_id is provided, resolve from task events
	if options.TaskID != "" {
		return s.resolveTaskLogFilename(clusterSessionKey, clusterLogPathPrefix, sessionID, options.TaskID, options.AttemptNumber, options.Suffix)
	}

	// If actor_id is provided, resolve from actor events
	if options.ActorID != "" {
		nodeID, filename, err := s.resolveActorLogFilename(clusterSessionKey, clusterLogPathPrefix, sessionID, options.ActorID, options.Suffix)
		return nodeID, filename, nil, err
	}

	// If pid is provided, resolve worker log file
	if options.PID > 0 {
		nodeID, filename, err := s.resolvePidLogFilename(clusterLogPathPrefix, sessionID, options.NodeID, options.PID, options.Suffix)
		return nodeID, filename, nil, err
	}

	return "", "", nil, fmt.Errorf("must provide one of: filename, task_id, actor_id, or pid")
}

// resolvePidLogFilename resolves a log file by PID.
// It requires a nodeID and searches for a log file with a name ending in "-{pid}.{suffix}".
func (s *ServerHandler) resolvePidLogFilename(clusterLogPathPrefix, sessionID, nodeID string, pid int, suffix string) (string, string, error) {
	if nodeID == "" {
		return "", "", fmt.Errorf("node_id is required for pid resolution")
	}

	// Convert to hex if not already is
	nodeIDHex, err := utils.ConvertBase64ToHex(nodeID)
	if err != nil {
		return "", "", fmt.Errorf("failed to decode node_id: %w", err)
	}

	logPath := clusterlogs.RelLogsDir(sessionID, nodeIDHex)
	files := s.reader.ListFiles(clusterLogPathPrefix, logPath)

	pidSuffix := fmt.Sprintf("-%d.%s", pid, suffix)

	for _, file := range files {
		// A rotated generation is never the canonical stream for a pid.
		if utils.IsRotatedLogName(file) {
			continue
		}
		if strings.HasSuffix(file, pidSuffix) {
			return nodeIDHex, file, nil
		}
	}

	return "", "", utils.NewHTTPError(fmt.Errorf("log file not found for pid %d in path %s", pid, logPath), http.StatusNotFound)
}

func getTasksByID(tasks []eventtypes.Task, taskID string) ([]eventtypes.Task, bool) {
	var attempts []eventtypes.Task
	for _, t := range tasks {
		if t.TaskID == taskID {
			attempts = append(attempts, t)
		}
	}
	if len(attempts) == 0 {
		return nil, false
	}
	return attempts, true
}

// resolveTaskLogFilename resolves log file for a task by querying task events.
// This mirrors Ray Dashboard's _resolve_task_filename logic.
// The sessionID parameter is required for searching worker log files when task_log_info is not available.
func (s *ServerHandler) resolveTaskLogFilename(clusterSessionKey, clusterLogPathPrefix, sessionID, taskID string, attemptNumber int, suffix string) (nodeID, filename string, byteRange *logByteRange, err error) {
	snap, ok := s.sessionLoader.GetSnapshot(clusterSessionKey)
	if !ok {
		return "", "", nil, fmt.Errorf("snapshot not found for %s", clusterSessionKey)
	}

	taskAttempts, found := getTasksByID(snap.Tasks, taskID)
	if !found {
		return "", "", nil, fmt.Errorf("task not found: task_id=%s", taskID)
	}

	// Find the specific attempt
	var foundTask *eventtypes.Task
	for i, task := range taskAttempts {
		if task.TaskAttempt == attemptNumber {
			foundTask = &taskAttempts[i]
			break
		}
	}

	if foundTask == nil {
		return "", "", nil, fmt.Errorf("task attempt not found: task_id=%s, attempt_number=%d", taskID, attemptNumber)
	}

	// Check if task has node_id
	if foundTask.NodeID == "" {
		return "", "", nil, fmt.Errorf("task %s (attempt %d) has no node_id (task not scheduled yet)", taskID, attemptNumber)
	}

	// Check if this is an actor task
	if foundTask.ActorID != "" {
		return "", "", nil, fmt.Errorf(
			"for actor task, please query actor log for actor(%s) by providing actor_id query parameter",
			foundTask.ActorID,
		)
	}

	// A task log range is only valid together with the file name from the same
	// TaskLogInfo stream. Falling back to a worker file when that name is missing
	// can return output from other tasks that reused the worker.
	if foundTask.TaskLogInfo != nil {
		var logFilename string
		var start, end int64
		if suffix == "err" {
			logFilename = foundTask.TaskLogInfo.StderrFile
			start = foundTask.TaskLogInfo.StderrStart
			end = foundTask.TaskLogInfo.StderrEnd
		} else {
			logFilename = foundTask.TaskLogInfo.StdoutFile
			start = foundTask.TaskLogInfo.StdoutStart
			end = foundTask.TaskLogInfo.StdoutEnd
		}
		if logFilename = taskLogBasename(logFilename); logFilename != "" {
			// Public Ray events use plain scalar offsets, so end == 0 is
			// indistinguishable from a missing end update. Treat it as incomplete
			// instead of reporting a potentially false empty log.
			if end == 0 {
				return "", "", nil, utils.NewHTTPError(
					fmt.Errorf("task %s (attempt %d) has incomplete task_log_info for suffix %s: end offset is unavailable", taskID, attemptNumber, suffix),
					http.StatusNotFound,
				)
			}
			if start < 0 || end < 0 || end < start {
				return "", "", nil, utils.NewHTTPError(
					fmt.Errorf("task %s (attempt %d) has invalid %s log byte range [%d,%d)", taskID, attemptNumber, suffix, start, end),
					http.StatusInternalServerError,
				)
			}
			return foundTask.NodeID, logFilename, &logByteRange{start: start, end: end}, nil
		}
		return "", "", nil, utils.NewHTTPError(
			fmt.Errorf("task %s (attempt %d) has incomplete task_log_info for suffix %s: log filename is empty", taskID, attemptNumber, suffix),
			http.StatusNotFound,
		)
	}

	// Fallback: Find worker log file by worker_id
	if foundTask.WorkerID == "" {
		return "", "", nil, fmt.Errorf(
			"task %s (attempt %d) has no worker_id",
			taskID, attemptNumber,
		)
	}
	if sessionID == "" {
		return "", "", nil, fmt.Errorf(
			"task %s (attempt %d) has no task_log_info and sessionID is required to search for worker log files",
			taskID, attemptNumber,
		)
	}
	nodeIDHex, logFilename, err := s.findWorkerLogFile(clusterLogPathPrefix, sessionID, foundTask.NodeID, foundTask.WorkerID, suffix)
	if err != nil {
		return "", "", nil, fmt.Errorf(
			"failed to find worker log file for task %s (attempt %d, worker_id=%s, node_id=%s): %w",
			taskID, attemptNumber, foundTask.WorkerID, foundTask.NodeID, err,
		)
	}

	return nodeIDHex, logFilename, nil, nil
}

// taskLogBasename maps Ray's absolute worker-container log path to the file
// name stored below the History Server's per-node logs prefix.
func taskLogBasename(filename string) string {
	if filename == "" {
		return ""
	}
	filename = path.Base(filename)
	if filename == "." || filename == ".." || filename == "/" {
		return ""
	}
	return filename
}

// resolveActorLogFilename resolves log file for an actor by querying actor events.
// This mirrors Ray Dashboard's _resolve_actor_filename logic.
func (s *ServerHandler) resolveActorLogFilename(clusterSessionKey, clusterLogPathPrefix, sessionID, actorID, suffix string) (nodeID, filename string, err error) {
	snap, ok := s.sessionLoader.GetSnapshot(clusterSessionKey)
	if !ok {
		return "", "", fmt.Errorf("snapshot not found for %s", clusterSessionKey)
	}

	actor, found := snap.Actors[actorID]
	if !found {
		return "", "", fmt.Errorf("actor not found: actor_id=%s", actorID)
	}

	// Check if actor has node_id (means it was scheduled)
	if actor.Address.NodeID == "" {
		return "", "", fmt.Errorf(
			"actor %s has no node_id (actor not scheduled yet)",
			actorID,
		)
	}

	// Check if actor has worker_id
	if actor.Address.WorkerID == "" {
		return "", "", fmt.Errorf(
			"actor %s has no worker_id (actor not scheduled yet)",
			actorID,
		)
	}

	if sessionID == "" {
		return "", "", fmt.Errorf(
			"sessionID is required to search for worker log files for actor %s",
			actorID,
		)
	}

	// Find worker log file by worker_id
	nodeIDHex, logFilename, err := s.findWorkerLogFile(
		clusterLogPathPrefix,
		sessionID,
		actor.Address.NodeID,
		actor.Address.WorkerID,
		suffix,
	)
	if err != nil {
		return "", "", fmt.Errorf(
			"failed to find worker log file for actor %s (worker_id=%s, node_id=%s): %w",
			actorID, actor.Address.WorkerID, actor.Address.NodeID, err,
		)
	}

	return nodeIDHex, logFilename, nil
}

// findWorkerLogFile searches for a worker log file by worker_id.
// Worker log files follow the pattern: worker-{worker_id_hex}-{job_id_hex}-{pid}.{suffix}
// Ref: https://github.com/ray-project/ray/blob/219ee7037bbdc02f66b58a814c9ad2618309c19e/src/ray/core_worker/core_worker_process.cc#L80-L80
// Returns (nodeIDHex, filename, error).
func (s *ServerHandler) findWorkerLogFile(clusterLogPathPrefix, sessionID, nodeID, workerID, suffix string) (string, string, error) {
	// Convert to hex if not already is
	nodeIDHex, err := utils.ConvertBase64ToHex(nodeID)
	if err != nil {
		return "", "", fmt.Errorf("failed to decode node_id: %w", err)
	}

	// Convert Base64 worker_id to hex
	workerIDHex, err := utils.ConvertBase64ToHex(workerID)
	if err != nil {
		return "", "", fmt.Errorf("failed to decode worker_id: %w", err)
	}

	// List all files in the node's log directory
	logPath := clusterlogs.RelLogsDir(sessionID, nodeIDHex)
	files := s.reader.ListFiles(clusterLogPathPrefix, logPath)

	// Search for files matching pattern: worker-{worker_id_hex}-*.{suffix}
	workerPrefix := fmt.Sprintf("worker-%s-", workerIDHex)
	workerSuffix := fmt.Sprintf(".%s", suffix)

	for _, file := range files {
		// A worker stream can span several rotated generations, so none of them is
		// the canonical file a task or actor lookup should resolve to.
		if utils.IsRotatedLogName(file) {
			continue
		}
		if strings.HasPrefix(file, workerPrefix) && strings.HasSuffix(file, workerSuffix) {
			return nodeIDHex, file, nil
		}
	}

	return "", "", utils.NewHTTPError(
		fmt.Errorf("worker log file not found: worker_id=%s (hex=%s), suffix=%s, searched in %s", workerID, workerIDHex, suffix, logPath),
		http.StatusNotFound,
	)
}

// ipToNodeId resolves node_id from node_ip by querying node_events from storage.
// This mirrors Ray Dashboard's ip_to_node_id logic.
// Returns node_id in hex format if found, error otherwise.
func (s *ServerHandler) ipToNodeId(clusterLogPathPrefix, sessionID, nodeIP string) (string, error) {
	if nodeIP == "" {
		return "", fmt.Errorf("node_ip is empty")
	}

	// Use targeted listing to find node_events directories under each node
	var candidatePrefixes []string
	for _, nodeName := range clusterlogs.ListSessionNodeDirs(s.reader, clusterLogPathPrefix, sessionID) {
		candidatePrefixes = append(candidatePrefixes, clusterlogs.RelNodeEventsDir(sessionID, nodeName)+"/")
	}

	for _, nodeEventDirPrefix := range candidatePrefixes {
		nodeEventFileNames := s.reader.ListFiles(clusterLogPathPrefix, nodeEventDirPrefix)
		for _, fileName := range nodeEventFileNames {
			if strings.HasSuffix(fileName, "/") {
				continue
			}
			fullPath := nodeEventDirPrefix + fileName
			nodeIDHex, found := s.searchNodeIDHexInEventFile(clusterLogPathPrefix, fullPath, nodeIP)
			if found {
				logrus.Infof("Resolved node_ip %s to node_id %s in session %s", nodeIP, nodeIDHex, sessionID)
				return nodeIDHex, nil
			}
		}
	}

	return "", fmt.Errorf("node_id not found for node_ip=%s in session=%s", nodeIP, sessionID)
}

// searchNodeIDHexInEventFile searches for a node with the given IP in a single event file.
// Returns (nodeIDHex, true) if found, ("", false) otherwise.
func (s *ServerHandler) searchNodeIDHexInEventFile(clusterLogPathPrefix, filePath, nodeIP string) (string, bool) {
	var reader io.Reader
	var err error
	if strings.HasSuffix(filePath, ".gz") {
		var rc io.ReadCloser
		rc, err = compression.ReadCompressedContent(s.reader, clusterLogPathPrefix, filePath)
		if err != nil {
			logrus.Warnf("Failed to decompress node event file %s: %v", filePath, err)
			return "", false
		}
		reader = rc
	} else {
		reader = s.reader.GetContent(clusterLogPathPrefix, filePath)
	}

	if reader == nil {
		return "", false
	}
	if closer, ok := reader.(io.Closer); ok {
		defer closer.Close()
	}

	data, err := io.ReadAll(reader)
	if err != nil {
		logrus.Warnf("Failed to read node event file %s: %v", filePath, err)
		return "", false
	}

	// Auto-detect JSON array (legacy) vs JSONL (new collector) so node_ip
	// lookups work against both formats.
	events, err := eventserver.DecodeEventFileBytes(filePath, data)
	if err != nil {
		logrus.Warnf("Failed to decode node events from %s: %v", filePath, err)
		return "", false
	}

	// Search for NODE_DEFINITION_EVENT with matching node_ip
	for _, event := range events {
		eventType, ok := event["eventType"].(string)
		if !ok || eventType != string(eventtypes.NODE_DEFINITION_EVENT) {
			continue
		}

		nodeDefEvent, ok := event["nodeDefinitionEvent"].(map[string]any)
		if !ok {
			continue
		}

		ipAddr, ok := nodeDefEvent["nodeIpAddress"].(string)
		if !ok || ipAddr != nodeIP {
			continue
		}

		// Found matching node, extract node_id
		nodeIDBytes, ok := nodeDefEvent["nodeId"].(string)
		if !ok {
			continue
		}

		// Convert to hex if not already is
		nodeIDHex, err := utils.ConvertBase64ToHex(nodeIDBytes)
		if err != nil {
			logrus.Warnf("Failed to decode node_id %s: %v", nodeIDBytes, err)
			continue
		}
		return nodeIDHex, true
	}

	return "", false
}
