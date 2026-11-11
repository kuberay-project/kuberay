package logcollector

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"syscall"

	"github.com/sirupsen/logrus"
)

// activeLogKind is how a session log is uploaded.
type activeLogKind int

const (
	// activeLogChunk uploads only the bytes appended since the last pass, as
	// one object per pass, because the file only ever grows.
	activeLogChunk activeLogKind = iota
	// activeLogOverwrite re-uploads the whole file because Ray rewrites it in
	// place, so there is no stable prefix to build on.
	activeLogOverwrite
)

// chunkDirSuffix names the object directory that holds a file's chunks: "<file>.chunks/<offset>".
const chunkDirSuffix = ".chunks"

// tailState is where the previous pass stopped reading one active log. The
// inode detects Ray swapping the file underneath the path on rotation.
//
// Progress is in memory only and a collector restart will re-upload the whole file from chunk 0.
type tailState struct {
	inode  uint64
	offset int64
}

// activeLogKey identifies one log stream independently of where the file
// currently lives, so progress survives the move into prev-logs on a session
// change. Add node ID as part of the key because a worker's Ray container can
// restart into the same session with a new node ID.
type activeLogKey struct {
	sessionID string
	nodeID    string
	relPath   string
}

// classifyActiveLog picks the upload strategy for a path relative to the
// session logs directory. Ray appends to every log it keeps there, except
// debug_state.txt, which the raylet truncates and rewrites on every dump:
// https://github.com/ray-project/ray/blob/c8466ab8fd2b14691633b163c56b5ef036d7d146/src/ray/raylet/node_manager.cc#L2613-L2619
//
// NOTE: some files are written once and never appended to, such as the
// profiler dumps under profiles/. We still classify them as chunk kind, as
// uploadNewBytes only uploads when the file has grown, so they go up as a
// single chunk and are not re-uploaded afterwards.
//
// Directory layout: https://docs.ray.io/en/latest/ray-observability/user-guides/configure-logging.html#logging-directory-structure
func classifyActiveLog(relPath string) activeLogKind {
	if relPath == "debug_state.txt" {
		return activeLogOverwrite
	}
	return activeLogChunk
}

// chunkObjectName is the object one chunk uploads to. Offsets are zero-padded
// so a lexicographic listing is also byte order.
//
// For example, the chunk holding bytes [4096, 8192) will return:
//
//	job-driver-raysubmit_abc.log.chunks/00000000000000004096
func chunkObjectName(objectName string, offset int64) string {
	return fmt.Sprintf("%s%s/%020d", objectName, chunkDirSuffix, offset)
}

// uploadNewBytes uploads whatever absPath gained since the last pass as one
// chunk object.
//
// TODO: log rotation is not supported yet. A rotated file restarts from offset
// zero, overwriting the old generation's chunks.
func (r *RayLogHandler) uploadNewBytes(absPath, objectName string, key activeLogKey) error {
	r.activeMu.Lock()
	defer r.activeMu.Unlock()

	file, err := os.Open(absPath)
	if err != nil {
		return fmt.Errorf("failed to open active log %s: %w", absPath, err)
	}
	defer file.Close()

	info, err := file.Stat()
	if err != nil {
		return fmt.Errorf("failed to stat active log %s: %w", absPath, err)
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return fmt.Errorf("inode of active log %s is unavailable on this platform", absPath)
	}

	if r.activeLogs == nil {
		r.activeLogs = make(map[activeLogKey]tailState)
	}
	state := r.activeLogs[key]
	// A key we have not seen, an inode change (rotation) or a shrink
	// (truncation) all start over from offset 0. A fresh key always uploads
	// once, even when the file is empty, so the log is listed in storage.
	fresh := state.inode != stat.Ino || info.Size() < state.offset
	if fresh {
		state = tailState{inode: stat.Ino}
	}
	if !fresh && info.Size() == state.offset {
		return nil
	}

	chunkName := chunkObjectName(objectName, state.offset)
	if err := r.Writer.CreateDirectory(path.Dir(chunkName)); err != nil {
		return fmt.Errorf("failed to create directory for %s: %w", chunkName, err)
	}
	// Fix the range at the stat size so bytes appended mid-upload go to the next chunk.
	chunk := io.NewSectionReader(file, state.offset, info.Size()-state.offset)
	if err := r.Writer.WriteFile(chunkName, chunk); err != nil {
		return fmt.Errorf("failed to write object %s: %w", chunkName, err)
	}

	state.offset = info.Size()
	r.activeLogs[key] = state
	logrus.Debugf("Uploaded active log chunk %s (object: %s, size: %d bytes)", absPath, chunkName, chunk.Size())
	return nil
}

// collectActiveLog uploads one log below logsDir according to classifyActiveLog.
func (r *RayLogHandler) collectActiveLog(absPath, logsDir, sessionID, nodeID string) error {
	relPath, err := filepath.Rel(logsDir, absPath)
	if err != nil {
		return fmt.Errorf("failed to get relative path for %s: %w", absPath, err)
	}
	relPath = filepath.ToSlash(relPath)
	objectPrefix := r.rotatedObjectPrefix(sessionID, nodeID)
	if objectPrefix == "" {
		return fmt.Errorf("session or node ID is unknown for %s", absPath)
	}
	objectName := path.Join(objectPrefix, relPath)

	switch classifyActiveLog(relPath) {
	case activeLogOverwrite:
		return r.uploadWholeFile(absPath, objectName)
	default:
		return r.uploadNewBytes(absPath, objectName, activeLogKey{sessionID: sessionID, nodeID: nodeID, relPath: relPath})
	}
}

// uploadWholeFile uploads absPath in full to objectName.
func (r *RayLogHandler) uploadWholeFile(absPath, objectName string) error {
	content, err := os.ReadFile(absPath)
	if err != nil {
		return fmt.Errorf("failed to read file %s: %w", absPath, err)
	}
	if err := r.Writer.WriteFile(objectName, bytes.NewReader(content)); err != nil {
		return fmt.Errorf("failed to write object %s: %w", objectName, err)
	}
	logrus.Infof("Successfully wrote object %s, size: %d bytes", objectName, len(content))
	return nil
}
