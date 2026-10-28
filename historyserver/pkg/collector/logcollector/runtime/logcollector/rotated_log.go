package logcollector

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"

	"github.com/sirupsen/logrus"

	"github.com/ray-project/kuberay/historyserver/pkg/storage/clusterlogs"
	"github.com/ray-project/kuberay/historyserver/pkg/utils"
)

// rotatedIdentity distinguishes rotation generations of one log stream. The
// inode alone is not enough because Linux reuses an evicted generation's inode
// for the next one; the modification time separates them and survives Ray's
// .1 -> .2 renames.
type rotatedIdentity struct {
	modTimeNs int64
	inode     uint64
}

func (id rotatedIdentity) String() string {
	return fmt.Sprintf("%d-%d", id.modTimeNs, id.inode)
}

// rotatedCandidate is the object a rotation backup uploads to. It holds no
// descriptor: the function that opens the backup keeps ownership of it.
type rotatedCandidate struct {
	path       string
	objectName string
	size       int64
}

// rotationBaseName reports whether name is a Ray rotation backup ("raylet.out.2")
// and, if so, returns the active log it was rotated out of ("raylet.out").
func rotationBaseName(name string) (string, bool) {
	dot := strings.LastIndexByte(name, '.')
	if dot <= 0 || dot == len(name)-1 {
		return "", false
	}
	index := name[dot+1:]
	if index[0] == '0' {
		return "", false
	}
	for _, c := range index {
		if c < '0' || c > '9' {
			return "", false
		}
	}
	return name[:dot], true
}

// rotationIndex returns the backup index of a Ray rotation backup
// ("raylet.out.2" -> 2), or 0 when name is not one.
func rotationIndex(name string) int {
	base, ok := rotationBaseName(name)
	if !ok {
		return 0
	}
	index, err := strconv.Atoi(name[len(base)+1:])
	if err != nil {
		return 0
	}
	return index
}

// rotatedLogName builds the deterministic object name for one rotation
// generation: "raylet.out.1" becomes "raylet.rotated.<mtime-ns>-<inode>.out".
// The time leads so a plain listing of one stream stays in rotation order.
func rotatedLogName(backupName string, id rotatedIdentity) (string, bool) {
	base, ok := rotationBaseName(backupName)
	if !ok {
		return "", false
	}
	identity := utils.RotatedLogMarker + id.String()
	ext := filepath.Ext(base)
	stem := strings.TrimSuffix(base, ext)
	if stem == "" {
		// Dotfile such as ".out": keep it whole rather than invent an extension.
		return base + identity, true
	}
	return stem + identity + ext, true
}

// collectIfRotatedLog uploads absPath when it is a Ray rotation backup and
// reports whether it was one, so walkers can skip their ordinary log handling.
func (r *RayLogHandler) collectIfRotatedLog(absPath, logsDir, objectPrefix string) bool {
	if _, ok := rotationBaseName(filepath.Base(absPath)); !ok {
		return false
	}
	if objectPrefix == "" {
		logrus.Warnf("Skipping rotated log %s: session or node ID is unknown", absPath)
		return true
	}
	r.collectRotatedLog(absPath, logsDir, objectPrefix)
	return true
}

// collectRotatedLog uploads one rotation backup unless this run already did, and
// returns the object name of the generation it found. The descriptor stays open
// across the upload so Ray cannot evict the generation mid-write.
func (r *RayLogHandler) collectRotatedLog(absPath, logsDir, objectPrefix string) (string, bool) {
	file, err := os.Open(absPath)
	if err != nil {
		// A missing path is Ray advancing the ring between the walk and the open.
		if !errors.Is(err, fs.ErrNotExist) {
			logrus.Errorf("Failed to open rotated log %s: %v", absPath, err)
		}
		return "", false
	}
	defer file.Close()

	candidate, ok := buildRotatedCandidate(file, absPath, logsDir, objectPrefix)
	if !ok {
		return "", false
	}
	if err := r.uploadRotatedCandidate(candidate, file); err != nil {
		logrus.Errorf("Failed to collect rotated log %s: %v", candidate.path, err)
	}
	return candidate.objectName, true
}

// rotatedObjectPrefix is the object directory a node's rotation backups are
// uploaded under, or "" when the session or node is not known yet.
func (r *RayLogHandler) rotatedObjectPrefix(sessionID, nodeID string) string {
	if sessionID == "" || nodeID == "" {
		return ""
	}
	return clusterlogs.LogsDir(r.RootDir, r.OwnerKind, r.OwnerName, r.RayClusterNamespace, r.RayClusterName, sessionID, nodeID)
}

// buildRotatedCandidate derives the object key of an already opened rotation
// backup. It borrows file to stat it and never closes it.
func buildRotatedCandidate(file *os.File, absPath, logsDir, objectPrefix string) (rotatedCandidate, bool) {
	relPath, err := filepath.Rel(logsDir, absPath)
	if err != nil {
		logrus.Errorf("Failed to get relative path for rotated log %s: %v", absPath, err)
		return rotatedCandidate{}, false
	}

	// Stat the descriptor rather than the path so the identity and the uploaded
	// bytes describe the same inode even if Ray renames the file mid-upload.
	info, err := file.Stat()
	if err != nil {
		logrus.Errorf("Failed to stat rotated log %s: %v", absPath, err)
		return rotatedCandidate{}, false
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		logrus.Errorf("Inode of rotated log %s is unavailable on this platform", absPath)
		return rotatedCandidate{}, false
	}
	objectBaseName, ok := rotatedLogName(filepath.Base(relPath), rotatedIdentity{
		modTimeNs: info.ModTime().UnixNano(),
		inode:     stat.Ino,
	})
	if !ok {
		return rotatedCandidate{}, false
	}

	return rotatedCandidate{
		path:       absPath,
		objectName: path.Join(objectPrefix, filepath.ToSlash(filepath.Dir(relPath)), objectBaseName),
		size:       info.Size(),
	}, true
}

// uploadRotatedCandidate uploads a generation this run has not written yet. A
// failed upload is not recorded, so the next periodic scan retries it while the
// generation is still in Ray's rotation ring.
func (r *RayLogHandler) uploadRotatedCandidate(candidate rotatedCandidate, content io.ReadSeeker) error {
	r.rotatedMu.Lock()
	defer r.rotatedMu.Unlock()

	if _, uploaded := r.rotatedUploaded[candidate.objectName]; uploaded {
		return nil
	}
	if err := r.Writer.CreateDirectory(path.Dir(candidate.objectName)); err != nil {
		return fmt.Errorf("failed to create directory for %s: %w", candidate.objectName, err)
	}
	if err := r.Writer.WriteFile(candidate.objectName, content); err != nil {
		return fmt.Errorf("failed to write object %s: %w", candidate.objectName, err)
	}
	if r.rotatedUploaded == nil {
		r.rotatedUploaded = make(map[string]struct{})
	}
	r.rotatedUploaded[candidate.objectName] = struct{}{}

	logrus.Infof("Uploaded rotated log %s (object: %s, size: %d bytes)", candidate.path, candidate.objectName, candidate.size)
	return nil
}

// pruneRotatedUploaded drops generations Ray has evicted, keeping the object
// names a complete scan of objectPrefix saw on disk. The scope matters: a scan of
// one session must not discard the entries prev-logs still dedups against.
func (r *RayLogHandler) pruneRotatedUploaded(objectPrefix string, seen map[string]struct{}) {
	// Match on a whole path segment so ".../node1/logs" never covers
	// ".../node10/logs".
	scope := strings.TrimSuffix(objectPrefix, "/") + "/"

	r.rotatedMu.Lock()
	defer r.rotatedMu.Unlock()

	for objectName := range r.rotatedUploaded {
		if !strings.HasPrefix(objectName, scope) {
			continue
		}
		if _, ok := seen[objectName]; !ok {
			delete(r.rotatedUploaded, objectName)
		}
	}
}

func stopRequested(stop <-chan struct{}) bool {
	select {
	case <-stop:
		return true
	default:
		return false
	}
}
