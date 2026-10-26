// Package eventserver provides the log event reader for the History Server.
//
// This file implements reading Log Events from logs/{nodeId}/events/event_*.log files
// stored in object storage, matching Ray Dashboard's event monitoring behavior.
//
// Reference:
//   - Ray Dashboard event_utils.py (monitor_events, _read_file, parse_event_strings)
//     python/ray/dashboard/modules/event/event_utils.py
//   - Ray Dashboard event_head.py
//     python/ray/dashboard/modules/event/event_head.py
package eventserver

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path"
	"strings"

	"github.com/sirupsen/logrus"

	"github.com/ray-project/kuberay/historyserver/pkg/eventserver/types"
	"github.com/ray-project/kuberay/historyserver/pkg/storage"
	"github.com/ray-project/kuberay/historyserver/pkg/storage/clusterlogs"
	"github.com/ray-project/kuberay/historyserver/pkg/utils"
)

// maxLineLengthLimit is the maximum line length for event files.
// Ray Dashboard uses EVENT_READ_LINE_LENGTH_LIMIT = 2MB (configurable via env var).
// Reference: python/ray/dashboard/modules/event/event_consts.py
// TODO: Make this configurable via environment variable (e.g., EVENT_READ_LINE_LENGTH_LIMIT)
// to match Ray Dashboard's behavior.
const maxLineLengthLimit = 2 * 1024 * 1024 // 2MB, matching Ray Dashboard default

// LogEventReader reads Log Events from object storage.
// It scans logs/{nodeId}/events/event_*.log files matching Ray Dashboard's behavior.
type LogEventReader struct {
	reader storage.StorageReader
}

// NewLogEventReader creates a new LogEventReader.
func NewLogEventReader(reader storage.StorageReader) *LogEventReader {
	return &LogEventReader{
		reader: reader,
	}
}

// ReadLogEvents reads all Log Events from logs/{nodeId}/events/event_*.log files
// and stores them in the provided ClusterLogEventMap.
//
// Path structure in storage: cluster-history/{ownerKind}/{namespace}/{ownerName}/{clusterName}/{sessionName}/{nodeId}/logs/events/event_*.log
//
// Return an error if any listed file fails to read (total or partial).
func (r *LogEventReader) ReadLogEvents(clusterInfo utils.ClusterInfo, clusterSessionKey string, eventStore *types.ClusterLogEventMap) error {
	// Build storage prefix used by StorageReader
	clusterLogPathPrefix := clusterlogs.Prefix("", clusterInfo.OwnerKind, clusterInfo.OwnerName, clusterInfo.Namespace, clusterInfo.Name)

	// Get or create the JobEventMap for this cluster session
	jobEventMap := eventStore.GetOrCreateJobEventMap(clusterSessionKey)

	// Find candidate nodes under {sessionName}/
	nodeIDs := clusterlogs.ListSessionNodeDirs(r.reader, clusterLogPathPrefix, clusterInfo.SessionName)
	logrus.Debugf("Found candidate node directories for cluster %s: %v", clusterSessionKey, nodeIDs)

	total := 0
	read := 0
	for _, nodeID := range nodeIDs {
		// Events directory: hierarchical (<sessionName>/<nodeId>/logs/events)
		eventsDir := path.Join(clusterlogs.RelLogsDir(clusterInfo.SessionName, nodeID), "events")
		eventFileNames := r.reader.ListFiles(clusterLogPathPrefix, eventsDir)
		for _, fileName := range eventFileNames {
			if !strings.HasPrefix(fileName, "event_") || !strings.HasSuffix(fileName, ".log") {
				continue
			}
			eventFilePath := path.Join(eventsDir, fileName)
			total++
			if err := r.readEventFile(clusterLogPathPrefix, eventFilePath, jobEventMap); err != nil {
				logrus.Warnf("Failed to read event file %s: %v", eventFilePath, err)
				continue
			}
			read++
		}
	}

	if total != read {
		return fmt.Errorf("read %d of %d log event files for %s", read, total, clusterSessionKey)
	}
	return nil
}

// readEventFile reads and parses a single event_*.log file (JSON Lines format).
// Lines exceeding maxLineLengthLimit are drained and skipped without accumulating
// in memory, matching Ray Dashboard's _read_file() behavior in event_utils.py.
func (r *LogEventReader) readEventFile(prefix, filePath string, jobEventMap *types.JobEventMap) error {
	ioReader := r.reader.GetContent(prefix, filePath)
	if ioReader == nil {
		return fmt.Errorf("failed to get content for %s", filePath)
	}

	// Use a moderate initial buffer (64KB); readLineWithLimit handles long-line
	// draining so we never accumulate more than maxLineLengthLimit in memory.
	br := bufio.NewReaderSize(ioReader, 64*1024)

	lineNum := 0
	eventCount := 0
	for {
		line, n, tooLong, err := readLineWithLimit(br, maxLineLengthLimit)

		// No remaining data — clean EOF with nothing left to process
		if errors.Is(err, io.EOF) && n == 0 {
			break
		}
		if err != nil && !errors.Is(err, io.EOF) {
			return fmt.Errorf("error reading %s at line %d: %w", filePath, lineNum+1, err)
		}

		lineNum++

		if tooLong {
			// Matching Ray Dashboard behavior: skip long lines and continue.
			// Reference: event_utils.py _read_file() — "Ignored long string: %s...(%s chars)"
			logrus.Warnf("Ignored long line at %s line %d: %d bytes (limit: %d)",
				filePath, lineNum, n, maxLineLengthLimit)
		} else if event := r.parseLine(line, filePath, lineNum); event != nil {
			jobEventMap.AddEvent(event)
			eventCount++
		}

		// EOF after processing the last partial line (file didn't end with '\n')
		if errors.Is(err, io.EOF) {
			break
		}
	}

	logrus.Debugf("Read %d events from %s (%d lines)", eventCount, filePath, lineNum)
	return nil
}

// readLineWithLimit reads one logical line (up to the next '\n') from br.
// It bounds memory usage: if the line exceeds limit bytes, the remainder is
// drained without being accumulated, and tooLong is set to true.
//
// Returns:
//   - line:    the full line bytes (nil when tooLong)
//   - n:       total bytes consumed for this logical line (including '\n')
//   - tooLong: true if the line exceeded limit
//   - err:     io.EOF when the stream ends (line/n may still hold the last partial data)
func readLineWithLimit(br *bufio.Reader, limit int) (line []byte, n int, tooLong bool, err error) {
	var buf []byte

	for {
		frag, e := br.ReadSlice('\n')
		n += len(frag)

		if !tooLong {
			if n > limit {
				// Line exceeded the limit — drop accumulated data and mark as too long.
				tooLong = true
				buf = nil
			} else {
				buf = append(buf, frag...)
			}
		}
		// If tooLong, we keep looping to drain remaining bytes until '\n' or EOF,
		// but do not accumulate them.

		switch {
		case e == nil:
			return buf, n, tooLong, nil
		case errors.Is(e, bufio.ErrBufferFull):
			continue
		case errors.Is(e, io.EOF):
			return buf, n, tooLong, io.EOF
		default:
			return nil, n, false, e
		}
	}
}

// parseLine parses a single JSON line into a LogEvent.
// Returns nil if the line is empty, invalid, or missing event_id.
func (r *LogEventReader) parseLine(line []byte, filePath string, lineNum int) *types.LogEvent {
	// Trim whitespace and newline
	line = bytes.TrimSpace(line)
	if len(line) == 0 {
		return nil
	}

	var event types.LogEvent
	if err := json.Unmarshal(line, &event); err != nil {
		logrus.Warnf("Failed to parse event at %s line %d: %v", filePath, lineNum, err)
		return nil
	}

	// Skip events without event_id
	if event.EventID == "" {
		return nil
	}

	// Restore escaped newlines in message (matching Ray Dashboard behavior)
	event.RestoreNewline()

	return &event
}
