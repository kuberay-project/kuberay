package eventcollector

import (
	"bufio"
	"compress/gzip"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/emicklei/go-restful/v3"
	"github.com/sirupsen/logrus"

	"github.com/ray-project/kuberay/historyserver/pkg/storage"
	"github.com/ray-project/kuberay/historyserver/pkg/storage/clusterlogs"
	"github.com/ray-project/kuberay/historyserver/pkg/utils"
)

// Defaults for disk-first event storage. Kept aligned with the
// collector-memory-control-proposal.md design document.
const (
	defaultRotationInterval = 5 * time.Minute
	defaultMaxFileSizeBytes = int64(100) * 1024 * 1024
	defaultMaxDiskBytes     = int64(200) * 1024 * 1024

	rotationCheckInterval = 30 * time.Second
	diskReconcileInterval = 1 * time.Minute
	// Raylet restarts hand out a new node ID. Poll often enough that events are
	// filed under the right one; matches the log collector's poll cadence.
	nodeIDPollInterval    = 5 * time.Second
	bufWriterSize         = 64 * 1024
	diskPressureWatermark = 0.8
	rotationQueueSize     = 256
	// Backoff before retrying a failed or deferred upload.
	retryBackoff = 5 * time.Second

	categoryNodeEvents = "node_events"
	categoryJobPrefix  = "job_events"
)

// Options groups tunables for disk-first event storage.
type Options struct {
	DataDir          string
	RotationInterval time.Duration
	MaxFileSizeBytes int64
	MaxDiskBytes     int64
	// CompressionEnabled controls whether rotated JSONL files are gzipped
	// before being uploaded to remote storage. When false, plain JSONL files
	// are uploaded as-is. Events are always written to local disk first
	// regardless of this setting.
	CompressionEnabled bool
}

// eventTypesWithJobID enumerates event types whose payload carries a jobId.
// Mirrors the previous in-memory implementation.
var eventTypesWithJobID = []string{
	// Job Events (Driver Job)
	"driverJobDefinitionEvent",
	"driverJobLifecycleEvent",

	// Task Events (Normal Task)
	"taskDefinitionEvent",
	"taskLifecycleEvent",
	"taskProfileEvents",

	// Actor Events (Actor Task + Actor Definition)
	"actorTaskDefinitionEvent",
	"actorDefinitionEvent",
}

var nodeEventType = []string{"NODE_LIFECYCLE_EVENT", "NODE_DEFINITION_EVENT"}

// safePathComponentRe matches values safe to use as a single path segment.
// Ray session names look like "session_2026-07-31_10-30-45_123456" and job IDs
// are hex, so this is permissive enough for real traffic.
var safePathComponentRe = regexp.MustCompile(`^[A-Za-z0-9._-]+$`)

// isSafePathComponent reports whether s can be used as a single path segment
// in local paths and remote storage keys.
// Reject "." and ".." explicitly: they pass the regexp but ".." escapes the
// parent dir and "." gets cleaned away by path.Join, collapsing this session
// into its parent and mixing it with others.
func isSafePathComponent(s string) bool {
	if s == "." || s == ".." {
		return false
	}
	return safePathComponentRe.MatchString(s)
}

// activeFileState wraps the per-category append target.
type activeFileState struct {
	file        *os.File
	writer      *bufio.Writer
	path        string
	category    string
	sessionName string
	nodeID      string
	size        int64
	// accountedSize is the byte count already added to ec.totalDiskUsed for this
	// file. A file accumulates across many PersistEvents calls, so reconciling
	// against the real on-disk size needs this baseline to avoid double-counting.
	accountedSize int64
	createdAt     time.Time
}

// rotationTask describes a local event file ready for upload.
type rotationTask struct {
	// path is the local file awaiting upload, can be a freshly rotated .jsonl or a
	// .jsonl.gz leftover re-enqueued by resumePendingFiles after a restart.
	path        string
	category    string
	sessionName string
	nodeID      string
	createdAt   time.Time
	size        int64
}

// EventCollector persists events to local disk as JSONL and asynchronously
// compresses + uploads rotated files to remote storage.
type EventCollector struct {
	storageWriter storage.StorageWriter

	// stopProducers tells the queue's sender-side goroutines (rotationLoop,
	// diskReconcileLoop, retry goroutines) to finish; see shutdown() for the
	// ordering that makes closing rotationQueue safe.
	stopProducers chan struct{}

	clusterNamespace   string
	sessionDir         string
	nodeID             string
	clusterName        string
	sessionName        string
	root               string
	ownerKind          string
	ownerName          string
	currentSessionName string
	currentNodeID      string

	// Disk-first storage configuration.
	dataDir            string
	rotationInterval   time.Duration
	maxFileSizeBytes   int64
	maxDiskBytes       int64
	compressionEnabled bool

	draining   atomic.Bool    // set during shutdown to reject new events
	inflightWG sync.WaitGroup // tracks in-flight PersistEvents handlers

	writeMu       sync.Mutex
	activeFiles   map[string]*activeFileState
	rotationQueue chan rotationTask
	totalDiskUsed atomic.Int64

	producersWG sync.WaitGroup // goroutines that may send to rotationQueue
	consumerWG  sync.WaitGroup // compressionUploadWorker + its retry goroutines
}

// NewEventCollector constructs an EventCollector with disk-first storage.
func NewEventCollector(
	writer storage.StorageWriter,
	rootDir, sessionDir, nodeID, clusterName, clusterNamespace, sessionName, ownerKind, ownerName string,
	opts Options,
) *EventCollector {
	if opts.DataDir == "" {
		opts.DataDir = "/tmp/ray/event-data"
	}
	if opts.RotationInterval <= 0 {
		opts.RotationInterval = defaultRotationInterval
	}
	if opts.MaxFileSizeBytes <= 0 {
		opts.MaxFileSizeBytes = defaultMaxFileSizeBytes
	}
	if opts.MaxDiskBytes <= 0 {
		opts.MaxDiskBytes = defaultMaxDiskBytes
	}

	collector := &EventCollector{
		storageWriter:      writer,
		root:               rootDir,
		sessionDir:         sessionDir,
		nodeID:             nodeID,
		clusterName:        clusterName,
		clusterNamespace:   clusterNamespace,
		sessionName:        sessionName,
		ownerKind:          ownerKind,
		ownerName:          ownerName,
		currentSessionName: sessionName,
		currentNodeID:      nodeID,
		stopProducers:      make(chan struct{}),

		dataDir:            opts.DataDir,
		rotationInterval:   opts.RotationInterval,
		maxFileSizeBytes:   opts.MaxFileSizeBytes,
		maxDiskBytes:       opts.MaxDiskBytes,
		compressionEnabled: opts.CompressionEnabled,
		activeFiles:        make(map[string]*activeFileState),
		rotationQueue:      make(chan rotationTask, rotationQueueSize),
	}

	return collector
}

// UpdateNodeID switches the node ID used for subsequent writes. Active files are
// rotated first so already-written events keep the old node ID in their remote
// key, while later events land in files named after the new one.
func (ec *EventCollector) UpdateNodeID(newNodeID string) {
	ec.writeMu.Lock()
	defer ec.writeMu.Unlock()

	if ec.currentNodeID == newNodeID {
		return
	}
	logrus.Infof("Node ID changed from %s to %s, rotating active files", ec.currentNodeID, newNodeID)
	ec.currentNodeID = newNodeID
	if err := ec.rotateAllFilesLocked(); err != nil {
		logrus.Errorf("Failed to rotate active files after node ID change: %v", err)
	}
}

func (ec *EventCollector) Run(stop <-chan struct{}, port int) {
	if err := os.MkdirAll(ec.dataDir, 0o755); err != nil {
		logrus.Fatalf("Failed to create event data dir %s: %v", ec.dataDir, err)
	}

	// Initialize disk usage counter + resume any files left from a prior run.
	ec.initDiskUsage()
	ec.resumePendingFiles()

	ws := new(restful.WebService)
	ws.Path("/v1")
	ws.Consumes(restful.MIME_JSON)
	ws.Produces(restful.MIME_JSON)
	ws.Route(ws.POST("/events").To(ec.PersistEvents))
	restful.Add(ws)

	go func() {
		logrus.Infof("Starting event collector on port %d", port)
		logrus.Fatal(http.ListenAndServe(fmt.Sprintf(":%d", port), nil))
	}()

	ec.producersWG.Add(1)
	go ec.rotationLoop()

	ec.consumerWG.Add(1)
	go ec.compressionUploadWorker()

	ec.producersWG.Add(1)
	go ec.diskReconcileLoop()

	<-stop
	logrus.Info("Received stop signal, draining events to storage")
	ec.shutdown()
}

// shutdown drains the whole pipeline following the pattern from
// go.dev/blog/pipelines: stop every sender first, then close the queue, and
// let the consumer drain it.
func (ec *EventCollector) shutdown() {
	// 1. Reject new events, then wait out in-flight handlers. Registering in
	// inflightWG before re-checking draining guarantees no handler can create
	// a fresh active file after the final rotation below.
	ec.draining.Store(true)
	ec.inflightWG.Wait()

	// 2. Rotate remaining active files. Blocking enqueue: the upload worker is
	// still consuming, so the sends make progress even when the queue is full.
	ec.rotateAllFilesForShutdown()

	// 3. Stop the remaining producers and wait until every potential sender is
	// gone; only then is closing the queue safe (a send on a closed channel
	// panics).
	close(ec.stopProducers)
	ec.producersWG.Wait()
	close(ec.rotationQueue)

	// 4. The worker drains the closed queue, giving each remaining task one
	// final attempt, and exits. Files whose upload still fails stay on local
	// disk for resumePendingFiles after the next start.
	ec.consumerWG.Wait()
}

// PersistEvents is the HTTP handler for POST /v1/events. Events are always
// appended to local JSONL files which are rotated and uploaded to remote
// storage asynchronously.
func (ec *EventCollector) PersistEvents(req *restful.Request, resp *restful.Response) {
	if ec.draining.Load() {
		resp.AddHeader("Retry-After", "5")
		resp.WriteErrorString(http.StatusServiceUnavailable, "event collector is shutting down")
		return
	}

	// Register as in-flight, then re-check draining. Shutdown sets draining and
	// then waits on inflightWG before rotating: registering before the second
	// check guarantees this handler either bails here (shutdown already began)
	// or is awaited by shutdown, so it can never create a fresh active file
	// after the final rotation.
	ec.inflightWG.Add(1)
	defer ec.inflightWG.Done()
	if ec.draining.Load() {
		resp.AddHeader("Retry-After", "5")
		resp.WriteErrorString(http.StatusServiceUnavailable, "event collector is shutting down")
		return
	}

	// Disk pressure only applies when the local disk pipeline is in use.
	if ec.underDiskPressure() {
		resp.AddHeader("Retry-After", "10")
		resp.WriteErrorString(http.StatusServiceUnavailable, "event collector under disk pressure")
		return
	}

	body, err := io.ReadAll(req.Request.Body)
	if err != nil {
		logrus.Errorf("Failed to read request body: %v", err)
		resp.WriteError(http.StatusBadRequest, err)
		return
	}

	var eventDatas []map[string]any
	if err := json.Unmarshal(body, &eventDatas); err != nil {
		logrus.Errorf("Failed to unmarshal event: %v", err)
		resp.WriteError(http.StatusBadRequest, err)
		return
	}

	// Phase 1: validate all events before acquiring the lock, so a
	// mid-batch validation error cannot leave partial data on disk.
	for _, eventData := range eventDatas {
		if _, ok := eventData["timestamp"].(string); !ok {
			logrus.Errorf("Event timestamp not found or not a string")
			resp.WriteError(http.StatusBadRequest, fmt.Errorf("timestamp not found"))
			return
		}
		sessionNameStr, ok := eventData["sessionName"].(string)
		if !ok {
			logrus.Errorf("Event sessionName not found or not a string")
			resp.WriteError(http.StatusBadRequest, fmt.Errorf("sessionName not found"))
			return
		}
		// sessionName becomes a directory name and part of the remote storage
		// key, so reject anything that could escape the collector data dir.
		if !isSafePathComponent(sessionNameStr) {
			logrus.Errorf("Event sessionName %q is not a valid path component", sessionNameStr)
			resp.WriteError(http.StatusBadRequest, fmt.Errorf("invalid sessionName"))
			return
		}
		if _, err := time.Parse(time.RFC3339Nano, eventData["timestamp"].(string)); err != nil {
			logrus.Errorf("Failed to parse timestamp: %v", err)
			resp.WriteError(http.StatusBadRequest, err)
			return
		}
	}

	// Phase 2: marshal and write all validated events under the lock.
	ec.writeMu.Lock()
	defer ec.writeMu.Unlock()

	// Track files written in this batch so the deferred flush only touches
	// them, not every open writer in ec.activeFiles.
	touchedWriters := make(map[*activeFileState]struct{})
	var writeErr error

	// Best-effort flush on any exit path so on-disk state stays consistent
	// with state.size / totalDiskUsed accounting.
	defer func() {
		for state := range touchedWriters {
			if err := state.writer.Flush(); err != nil {
				logrus.Errorf("Failed to flush %s: %v", state.path, err)
				if writeErr == nil {
					writeErr = fmt.Errorf("flush %s: %w", state.path, err)
				}
			}
			// Flush may be partial on error; either way the real on-disk size is
			// authoritative, so settle accounting against it.
			ec.reconcileDiskAccountingLocked(state)
		}
		if writeErr != nil {
			resp.WriteError(http.StatusInternalServerError, writeErr)
		} else {
			resp.WriteHeader(http.StatusOK)
		}
	}()

	for _, eventData := range eventDatas {
		sessionNameStr := eventData["sessionName"].(string)

		// Active files are keyed by category only, so on a session change the old
		// session's files must be rotated (closed + enqueued for upload) before
		// writing events for the new session.
		if ec.currentSessionName != sessionNameStr {
			logrus.Infof("Session name changed from %s to %s, rotating active files", ec.currentSessionName, sessionNameStr)
			// Rotation already flushed and settled the old session's files, so
			// drop them from touchedWriters — the deferred flush must not touch
			// their closed writers.
			if err := ec.rotateAllFilesLocked(); err != nil && writeErr == nil {
				writeErr = fmt.Errorf("rotate on session change: %w", err)
			}
			ec.currentSessionName = sessionNameStr
			touchedWriters = make(map[*activeFileState]struct{})
		}

		category := ec.categorize(eventData)

		line, err := json.Marshal(eventData)
		if err != nil {
			logrus.Errorf("Failed to marshal event: %v", err)
			writeErr = fmt.Errorf("marshal event: %w", err)
			return
		}

		state, err := ec.getOrCreateActiveFileLocked(category, sessionNameStr)
		if err != nil {
			logrus.Errorf("Failed to open active file for %s: %v", category, err)
			writeErr = err
			return
		}

		n, err := state.writer.Write(line)
		if err == nil {
			var m int
			m, err = state.writer.WriteString("\n")
			n += m
		}
		if err != nil {
			logrus.Errorf("Failed to write event to %s: %v", state.path, err)
			writeErr = err
			// bufio errors are sticky: evict the wedged writer so the next
			// request starts a fresh file. Rotate file here to salvage the bytes
			// already on disk.
			delete(touchedWriters, state)
			if rerr := ec.rotateFileLocked(category, false); rerr != nil {
				logrus.Errorf("Failed to rotate wedged writer for %s: %v", category, rerr)
			}
			return
		}
		state.size += int64(n)
		touchedWriters[state] = struct{}{}
	}
}

// categorize determines the storage category path for an event.
// - NODE_* events → "node_events"
// - others with a jobID → "job_events/{jobID}"
// - fallback → "node_events" (matches previous behavior)
func (ec *EventCollector) categorize(eventData map[string]any) string {
	if isNodeEvent(eventData) {
		return categoryNodeEvents
	}
	if jobID := getJobID(eventData); jobID != "" {
		return path.Join(categoryJobPrefix, jobID)
	}
	return categoryNodeEvents
}

// reconcileDiskAccountingLocked settles a file's accounting (totalDiskUsed,
// accountedSize, size) against its real on-disk size, so cleanup subtracts
// what was really written instead of what we thought we wrote.
// Must be called with writeMu held.
func (ec *EventCollector) reconcileDiskAccountingLocked(state *activeFileState) {
	info, err := os.Stat(state.path)
	if err != nil {
		logrus.Warnf("Failed to stat %s for disk accounting: %v", state.path, err)
		return
	}
	actual := info.Size()
	ec.totalDiskUsed.Add(actual - state.accountedSize)
	state.accountedSize = actual
	// Keep size truthful too, so size-based rotation is not triggered early by
	// bytes that were never written.
	state.size = actual
}

// getOrCreateActiveFileLocked returns the active file for a category,
// opening one lazily if necessary. Must be called with writeMu held.
func (ec *EventCollector) getOrCreateActiveFileLocked(category, sessionName string) (*activeFileState, error) {
	if state, ok := ec.activeFiles[category]; ok {
		return state, nil
	}
	return ec.openNewActiveFileLocked(category, sessionName)
}

// openNewActiveFileLocked opens a fresh JSONL file for the given category.
// The resulting path looks like:
//
//	{dataDir}/{clusterName}_{namespace}/{sessionName}/{category}/{nodeID}_{unixNano}.jsonl
//
// e.g. /data/raycluster-sample_default/session_2026-08-03_10-00-00_123_1/job_events/01000000/abc123_1754200000000000000.jsonl
// Must be called with writeMu held.
func (ec *EventCollector) openNewActiveFileLocked(category, sessionName string) (*activeFileState, error) {
	dir := filepath.Join(ec.dataDir, ec.clusterKey(), sessionName, category)
	// confirm the join path stayed under dataDir
	if err := ec.assertInsideDataDir(dir); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("mkdir %s: %w", dir, err)
	}

	createdAt := time.Now()
	nodeID := ec.currentNodeID
	fileName := fmt.Sprintf("%s_%d.jsonl", sanitizeFileComponent(nodeID), createdAt.UnixNano())
	fullPath := filepath.Join(dir, fileName)

	f, err := os.OpenFile(fullPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", fullPath, err)
	}

	state := &activeFileState{
		file:        f,
		writer:      bufio.NewWriterSize(f, bufWriterSize),
		path:        fullPath,
		category:    category,
		sessionName: sessionName,
		nodeID:      nodeID,
		createdAt:   createdAt,
	}
	ec.activeFiles[category] = state
	return state, nil
}

// assertInsideDataDir fails if p resolved to somewhere outside ec.dataDir.
func (ec *EventCollector) assertInsideDataDir(p string) error {
	rel, err := filepath.Rel(ec.dataDir, p)
	if err != nil {
		return fmt.Errorf("resolve %s against data dir %s: %w", p, ec.dataDir, err)
	}
	if rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return fmt.Errorf("path %s escapes data dir %s", p, ec.dataDir)
	}
	return nil
}

// clusterKey returns "{clusterName}_{namespace}" used as dataDir subdirectory.
func (ec *EventCollector) clusterKey() string {
	return fmt.Sprintf("%s_%s", ec.clusterName, ec.clusterNamespace)
}

// rotationLoop periodically checks rotation conditions for each active file and
// refreshes the node ID. The node ID is polled from the Ray dashboard, so a raylet
// restart is picked up within nodeIDPollInterval.
func (ec *EventCollector) rotationLoop() {
	defer ec.producersWG.Done()
	ticker := time.NewTicker(rotationCheckInterval)
	defer ticker.Stop()
	nodeIDTicker := time.NewTicker(nodeIDPollInterval)
	defer nodeIDTicker.Stop()

	for {
		select {
		case <-ticker.C:
			ec.checkRotation()
		case <-nodeIDTicker.C:
			if freshNodeID, err := utils.FetchCurrentNodeID(); err == nil && freshNodeID != "" {
				if hexID, err := utils.ConvertBase64ToHex(freshNodeID); err == nil && hexID != "" {
					ec.UpdateNodeID(hexID)
				}
			}
		case <-ec.stopProducers:
			return
		}
	}
}

// checkRotation rotates any active file older than rotationInterval or
// larger than maxFileSizeBytes.
func (ec *EventCollector) checkRotation() {
	ec.writeMu.Lock()
	defer ec.writeMu.Unlock()

	now := time.Now()
	for category, state := range ec.activeFiles {
		if now.Sub(state.createdAt) >= ec.rotationInterval || state.size >= ec.maxFileSizeBytes {
			if err := ec.rotateFileLocked(category, false); err != nil {
				logrus.Errorf("Failed to rotate %s: %v", category, err)
			}
		}
	}
}

// rotateAllFilesLocked rotates every active file and returns the combined
// errors so PersistEvents can report rotation failures to the client.
// Must be called with writeMu held.
func (ec *EventCollector) rotateAllFilesLocked() error {
	var errs []error
	for category := range ec.activeFiles {
		if err := ec.rotateFileLocked(category, false); err != nil {
			logrus.Errorf("Failed to rotate %s: %v", category, err)
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// rotateAllFilesForShutdown rotates every active file, blocking until each
// rotated file is enqueued for upload. Unlike the periodic path it never drops
// tasks when the queue is momentarily full — the upload worker is still running
// during shutdown, so blocking sends drain and make room.
func (ec *EventCollector) rotateAllFilesForShutdown() {
	ec.writeMu.Lock()
	defer ec.writeMu.Unlock()
	for category := range ec.activeFiles {
		if err := ec.rotateFileLocked(category, true); err != nil {
			logrus.Errorf("Failed to rotate %s during shutdown: %v", category, err)
		}
	}
}

// enqueueRotationTask hands a rotated file to the upload worker. When blocking
// is true it waits for queue space; otherwise it drops to a delayed-retry
// goroutine when the queue is full.
func (ec *EventCollector) enqueueRotationTask(task rotationTask, blocking bool) {
	if blocking {
		ec.rotationQueue <- task
		return
	}
	select {
	case ec.rotationQueue <- task:
	default:
		logrus.Warnf("rotation queue full, scheduling retry for %s", task.path)
		ec.retryProcess(task)
	}
}

// retryProcess re-processes a failed or deferred rotation task after a
// backoff. Once shutdown begins, new retries are declined while sleeping ones
// are woken for one final attempt. Tracked by consumerWG so shutdown waits.
func (ec *EventCollector) retryProcess(task rotationTask) {
	select {
	case <-ec.stopProducers:
		logrus.Errorf("Giving up on retrying %s during shutdown; file left on disk", task.path)
		return
	default:
	}
	ec.consumerWG.Go(func() {
		select {
		case <-time.After(retryBackoff):
		case <-ec.stopProducers:
			// Shutdown cuts the backoff short; the attempt below is the final
			// one, since a failure re-enters retryProcess and is declined.
		}
		ec.processRotatedFile(task)
	})
}

// rotateFileLocked closes the active file for a category and queues it for
// compression + upload. Must be called with writeMu held. When blocking is
// true the enqueue waits for queue space instead of dropping to a retry
// goroutine (used during shutdown).
func (ec *EventCollector) rotateFileLocked(category string, blocking bool) error {
	state, ok := ec.activeFiles[category]
	if !ok {
		return nil
	}
	delete(ec.activeFiles, category)

	flushFailed := false
	if err := state.writer.Flush(); err != nil {
		logrus.Errorf("Failed to flush %s before rotation: %v", state.path, err)
		flushFailed = true
	}
	if err := state.file.Sync(); err != nil {
		logrus.Warnf("Failed to sync %s before rotation: %v", state.path, err)
	}
	if err := state.file.Close(); err != nil {
		logrus.Errorf("Failed to close %s before rotation: %v", state.path, err)
	}

	// Settle accounting against the actual on-disk size: state.size may include
	// bytes lost to a failed flush, and the caller may have written bytes not
	// yet accounted (mid-batch session rotation).
	diskSize := state.size
	if info, err := os.Stat(state.path); err == nil {
		diskSize = info.Size()
	}
	ec.totalDiskUsed.Add(diskSize - state.accountedSize)
	state.accountedSize = diskSize

	if diskSize == 0 {
		if err := os.Remove(state.path); err != nil && !os.IsNotExist(err) {
			logrus.Warnf("Failed to remove empty rotated file %s: %v", state.path, err)
		}
		if flushFailed {
			return fmt.Errorf("flush failed for %s, buffered events lost", state.path)
		}
		return nil
	}

	task := rotationTask{
		path:        state.path,
		category:    state.category,
		sessionName: state.sessionName,
		nodeID:      state.nodeID,
		createdAt:   state.createdAt,
		size:        diskSize,
	}

	if flushFailed {
		logrus.Warnf("Flush failed for %s; scheduling delayed upload of on-disk content (%d bytes)", state.path, diskSize)
		if blocking {
			ec.enqueueRotationTask(task, true)
		} else {
			ec.retryProcess(task)
		}
		return fmt.Errorf("flush failed for %s, queued for delayed upload", state.path)
	}

	ec.enqueueRotationTask(task, blocking)
	return nil
}

// compressionUploadWorker consumes rotationQueue, optionally compressing
// rotated files before uploading them to remote storage. It exits once the
// queue is closed and drained, which doubles as the shutdown drain: every
// remaining task gets one final attempt.
func (ec *EventCollector) compressionUploadWorker() {
	defer ec.consumerWG.Done()

	for task := range ec.rotationQueue {
		ec.processRotatedFile(task)
	}
}

// processRotatedFile optionally gzips the file, uploads it, then cleans up the
// local copies. A failed upload is handed to retryProcess, which retries after
// a backoff or, during shutdown, abandons the task.
func (ec *EventCollector) processRotatedFile(task rotationTask) {
	uploadPath := task.path
	var compressedPath string

	// Resumed .jsonl.gz leftovers are already compressed; gzipping them again
	// would double-compress the payload and make it undecodable downstream.
	alreadyCompressed := strings.HasSuffix(task.path, ".gz")

	if ec.compressionEnabled && !alreadyCompressed {
		gzPath := task.path + ".gz"
		if err := gzipFile(task.path, gzPath); err != nil {
			logrus.Errorf("Failed to compress %s: %v", task.path, err)
			// Leave the .jsonl file for retry on next restart.
			return
		}
		uploadPath = gzPath
		compressedPath = gzPath
		if info, err := os.Stat(gzPath); err == nil {
			ec.totalDiskUsed.Add(info.Size())
		}
	}

	key := ec.buildEventStorageKey(task)

	f, err := os.Open(uploadPath)
	if err != nil {
		logrus.Errorf("Failed to open %s for upload: %v", uploadPath, err)
		return
	}

	// Ensure parent directory exists in remote storage (best-effort).
	if err := ec.storageWriter.CreateDirectory(path.Dir(key)); err != nil {
		logrus.Warnf("Failed to create remote directory %s: %v", path.Dir(key), err)
	}

	if err := ec.storageWriter.WriteFile(key, f); err != nil {
		f.Close()
		logrus.Errorf("Failed to upload %s to %s: %v", uploadPath, key, err)
		// Drop any compressed artifact so a retry regenerates it cleanly and
		// its size is not double-counted in totalDiskUsed.
		if compressedPath != "" {
			if info, err := os.Stat(compressedPath); err == nil {
				ec.totalDiskUsed.Add(-info.Size())
			}
			if err := os.Remove(compressedPath); err != nil && !os.IsNotExist(err) {
				logrus.Warnf("Failed to remove %s after failed upload: %v", compressedPath, err)
			}
		}
		ec.retryProcess(task)
		return
	}
	f.Close()

	// Clean up local files (decrement accounting best-effort).
	if info, err := os.Stat(task.path); err == nil {
		ec.totalDiskUsed.Add(-info.Size())
	}
	if err := os.Remove(task.path); err != nil && !os.IsNotExist(err) {
		logrus.Warnf("Failed to remove %s after upload: %v", task.path, err)
	}

	if compressedPath != "" {
		if info, err := os.Stat(compressedPath); err == nil {
			ec.totalDiskUsed.Add(-info.Size())
		}
		if err := os.Remove(compressedPath); err != nil && !os.IsNotExist(err) {
			logrus.Warnf("Failed to remove %s after upload: %v", compressedPath, err)
		}
	}

	logrus.Infof("Uploaded %d bytes to %s", task.size, key)
}

// buildEventStorageKey constructs the remote storage key for a rotated file,
// using the clusterlogs layout the history server reads (#4918):
//
//	Node events: <prefix>/{sessionName}/{nodeID}/node_events/{nodeID}-{hour}-{nanoTs}.jsonl.gz
//	Job events:  <prefix>/{sessionName}/{nodeID}/job_events/{jobID}/{nodeID}-{hour}-{nanoTs}.jsonl.gz
//
// The UnixNano suffix avoids collisions when multiple files are rotated within
// the same hour for the same nodeID+category.
func (ec *EventCollector) buildEventStorageKey(task rotationTask) string {
	hour := task.createdAt.Truncate(time.Hour).Format("2006-01-02-15")
	sessionName := task.sessionName
	if sessionName == "" {
		sessionName = ec.sessionName
	}
	// nodeID is a directory level in the clusterlogs layout, so the fallback
	// also keeps resumed files (whose name may not parse) out of an empty dir.
	nodeID := task.nodeID
	if nodeID == "" {
		nodeID = ec.nodeID
	}

	// The extension must describe the bytes actually uploaded: an already
	// compressed leftover (resumed .jsonl.gz) stays gzip regardless of the
	// current compression flag, which may have changed since the prior run.
	ext := ".jsonl"
	if ec.compressionEnabled || strings.HasSuffix(task.path, ".gz") {
		ext = ".jsonl.gz"
	}

	fileName := fmt.Sprintf("%s-%s-%d%s", nodeID, hour, task.createdAt.UnixNano(), ext)

	var dir string
	if jobID, ok := strings.CutPrefix(task.category, categoryJobPrefix+"/"); ok {
		dir = clusterlogs.JobEventsDir(ec.root, ec.ownerKind, ec.ownerName, ec.clusterNamespace, ec.clusterName, sessionName, nodeID, jobID)
	} else {
		dir = clusterlogs.NodeEventsDir(ec.root, ec.ownerKind, ec.ownerName, ec.clusterNamespace, ec.clusterName, sessionName, nodeID)
	}
	return path.Join(dir, fileName)
}

// diskReconcileLoop periodically reconciles totalDiskUsed with a real
// directory walk to correct any drift.
func (ec *EventCollector) diskReconcileLoop() {
	defer ec.producersWG.Done()
	ticker := time.NewTicker(diskReconcileInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if actual, err := dirSize(ec.dataDir); err == nil {
				ec.totalDiskUsed.Store(actual)
			} else {
				logrus.Warnf("Failed to reconcile disk usage: %v", err)
			}
		case <-ec.stopProducers:
			return
		}
	}
}

// initDiskUsage initializes the disk usage counter by walking dataDir once.
func (ec *EventCollector) initDiskUsage() {
	if size, err := dirSize(ec.dataDir); err == nil {
		ec.totalDiskUsed.Store(size)
	} else {
		logrus.Warnf("Failed to compute initial disk usage for %s: %v", ec.dataDir, err)
	}
}

// resumePendingFiles enqueues any files left behind by a prior run for upload.
// Pre-existing .jsonl files are treated as rotated (already-closed) and
// compressed+uploaded. Pre-existing .jsonl.gz files are re-uploaded directly.
// When both a .jsonl and its .jsonl.gz exist (failed cleanup), only the .gz is
// uploaded to avoid duplicates.
//
// Directory layout: {clusterKey}/{sessionName}/{category}/{file}
func (ec *EventCollector) resumePendingFiles() {
	// {dataDir}/{cluster_name}_{cluster_namespace}
	clusterRoot := filepath.Join(ec.dataDir, ec.clusterKey())
	entries, err := os.Stat(clusterRoot)
	if err != nil || !entries.IsDir() {
		return
	}

	type pendingFile struct {
		path     string
		info     os.FileInfo
		category string
		session  string
	}

	// First pass: collect all event files.
	gzFiles := make(map[string]pendingFile)    // keyed by base path without .gz
	jsonlFiles := make(map[string]pendingFile) // keyed by full path

	_ = filepath.Walk(clusterRoot, func(p string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return nil
		}

		rel, err := filepath.Rel(clusterRoot, p)
		if err != nil {
			return nil
		}

		// Recover session/category from the directory levels, reversing the
		// layout openNewActiveFileLocked writes (nodeID/createdAt are parsed
		// from the file name later):
		//
		//	node events: {sessionName}/node_events/{file}
		//	job events:  {sessionName}/job_events/{jobID}/{file}
		//
		// Anything not matching either shape exactly is not ours; skip it.
		parts := strings.Split(filepath.ToSlash(rel), "/")
		var sessionName, category string
		switch {
		case len(parts) == 3 && parts[1] == categoryNodeEvents:
			sessionName, category = parts[0], categoryNodeEvents
		case len(parts) == 4 && parts[1] == categoryJobPrefix:
			sessionName, category = parts[0], path.Join(categoryJobPrefix, parts[2])
		default:
			return nil
		}

		pf := pendingFile{path: p, info: info, category: category, session: sessionName}

		name := info.Name()
		switch {
		case strings.HasSuffix(name, ".tmp"):
			// A crash mid-compression leaves a partial gzip under a .tmp name. Drop it here.
			if err := os.Remove(p); err != nil && !os.IsNotExist(err) {
				logrus.Warnf("Failed to remove partial gzip %s: %v", p, err)
			} else {
				ec.totalDiskUsed.Add(-info.Size())
				logrus.Infof("Removed partial gzip %s left by a prior crash", p)
			}
		case strings.HasSuffix(name, ".jsonl.gz"):
			basePath := strings.TrimSuffix(p, ".gz")
			gzFiles[basePath] = pf
		case strings.HasSuffix(name, ".jsonl"):
			jsonlFiles[p] = pf
		}
		return nil
	})

	// Second pass: process files, skipping .jsonl when a .jsonl.gz already exists.
	for basePath, pf := range gzFiles {
		// Remove .jsonl when its .gz counterpart already exists.
		if orphan, ok := jsonlFiles[basePath]; ok {
			delete(jsonlFiles, basePath)
			if err := os.Remove(orphan.path); err != nil && !os.IsNotExist(err) {
				logrus.Warnf("Failed to remove superseded %s: %v", orphan.path, err)
			} else {
				ec.totalDiskUsed.Add(-orphan.info.Size())
				logrus.Infof("Removed %s superseded by its .gz counterpart", orphan.path)
			}
		}

		nodeID := nodeIDFromFileName(pf.info.Name())
		task := rotationTask{
			path:        pf.path,
			category:    pf.category,
			sessionName: pf.session,
			nodeID:      nodeID,
			createdAt:   createdAtFromFileName(pf.info.Name(), pf.info.ModTime()),
			size:        pf.info.Size(),
		}
		select {
		case ec.rotationQueue <- task:
		default:
			logrus.Warnf("rotation queue full during resume, scheduling retry for %s", pf.path)
			ec.retryProcess(task)
		}
	}

	for _, pf := range jsonlFiles {
		nodeID := nodeIDFromFileName(pf.info.Name())
		task := rotationTask{
			path:        pf.path,
			category:    pf.category,
			sessionName: pf.session,
			nodeID:      nodeID,
			createdAt:   createdAtFromFileName(pf.info.Name(), pf.info.ModTime()),
			size:        pf.info.Size(),
		}
		select {
		case ec.rotationQueue <- task:
		default:
			logrus.Warnf("rotation queue full during resume, scheduling retry for %s", pf.path)
			ec.retryProcess(task)
		}
	}
}

// underDiskPressure reports whether new events should be rejected.
func (ec *EventCollector) underDiskPressure() bool {
	if ec.maxDiskBytes <= 0 {
		return false
	}
	threshold := int64(float64(ec.maxDiskBytes) * diskPressureWatermark)
	return ec.totalDiskUsed.Load() >= threshold
}

// isNodeEvent checks if event is node-related.
func isNodeEvent(eventData map[string]any) bool {
	eventType, ok := eventData["eventType"].(string)
	if !ok {
		return false
	}
	return slices.Contains(nodeEventType, eventType)
}

// getJobID extracts a jobId from known nested event payloads.
func getJobID(eventData map[string]any) string {
	for _, eventType := range eventTypesWithJobID {
		if nestedEvent, ok := eventData[eventType].(map[string]any); ok {
			if jobID, hasJob := nestedEvent["jobId"]; hasJob && jobID != "" {
				id := fmt.Sprintf("%v", jobID)
				// Payload job IDs arrive base64-encoded (e.g. "AQAAAA=="). Normalize to hex for safe path validation.
				if hexID, err := utils.ConvertBase64ToHex(id); err == nil {
					id = hexID
				}
				if !isSafePathComponent(id) {
					logrus.Warnf("Ignoring unsafe jobId %q; filing event under %s", id, categoryNodeEvents)
					return ""
				}
				return id
			}
		}
	}
	return ""
}

// gzipFile compresses src into dst using gzip.
func gzipFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()

	// Compress into a temp file and atomically rename it into place once it is
	// fully written and synced. A crash mid-compression can therefore never
	// leave a truncated file at dst.
	tmp := dst + ".tmp"
	out, err := os.Create(tmp)
	if err != nil {
		return err
	}

	// On any failure after creation, remove the partial temp output.
	cleanup := func(cause error) error {
		out.Close()
		if rmErr := os.Remove(tmp); rmErr != nil && !os.IsNotExist(rmErr) {
			logrus.Warnf("Failed to remove partial gzip %s: %v", tmp, rmErr)
		}
		return cause
	}

	gz := gzip.NewWriter(out)
	if _, err := io.Copy(gz, in); err != nil {
		gz.Close()
		return cleanup(err)
	}
	if err := gz.Close(); err != nil {
		return cleanup(err)
	}
	if err := out.Sync(); err != nil {
		return cleanup(err)
	}
	if err := out.Close(); err != nil {
		return cleanup(err)
	}
	if err := os.Rename(tmp, dst); err != nil {
		return cleanup(err)
	}
	return nil
}

// dirSize returns the total byte size of files under root.
func dirSize(root string) (int64, error) {
	var total int64
	err := filepath.Walk(root, func(_ string, info os.FileInfo, err error) error {
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}
		if !info.IsDir() {
			total += info.Size()
		}
		return nil
	})
	return total, err
}

// sanitizeFileComponent replaces path separators in filename components.
func sanitizeFileComponent(s string) string {
	if s == "" {
		return "unknown"
	}
	replacer := strings.NewReplacer("/", "_", string(os.PathSeparator), "_", " ", "_")
	return replacer.Replace(s)
}

// nodeIDFromFileName extracts the nodeID from a file named
// "{sanitizedNodeID}_{unixNano}.jsonl" (or .jsonl.gz). Returns empty string
// if the format is unrecognized.
func nodeIDFromFileName(name string) string {
	// Strip known suffixes.
	base := strings.TrimSuffix(name, ".gz")
	base = strings.TrimSuffix(base, ".jsonl")

	// The last "_" separates nodeID from the Unix-nano timestamp.
	idx := strings.LastIndex(base, "_")
	if idx <= 0 {
		return ""
	}
	return base[:idx]
}

// createdAtFromFileName parses the UnixNano timestamp embedded in filenames
// like "{nodeId}_{unixNano}.jsonl[.gz]". Returns fallback if the filename
// does not match this format or the timestamp cannot be parsed.
func createdAtFromFileName(name string, fallback time.Time) time.Time {
	base := strings.TrimSuffix(name, ".gz")
	base = strings.TrimSuffix(base, ".jsonl")

	idx := strings.LastIndex(base, "_")
	if idx < 0 || idx >= len(base)-1 {
		return fallback
	}
	nanos, err := strconv.ParseInt(base[idx+1:], 10, 64)
	if err != nil {
		return fallback
	}
	return time.Unix(0, nanos)
}
