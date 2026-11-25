// Package storagetest holds the pieces the per-backend storage tests would
// otherwise each copy. The backends implement the same interface against
// different SDKs, so their tests ask the same questions and only the transport
// differs. Keeping the shared half here means a new backend's test file starts
// from the same fixture values as the existing ones instead of inventing its own.
package storagetest

import "sync"

// Log path pieces shared by every backend test. GetContent builds its object key
// from a root dir, a cluster prefix and a file name, and a deployment that sets a
// root dir only works if all three end up in the key, so the backends are pinned
// against the same three values.
const (
	// RootDir is the configured root under which a deployment stores Ray logs.
	RootDir = "ray-logs"
	// ClusterPrefix is a root-dir-relative path prefix, not a bare cluster id.
	// See clusterlogs.Prefix("", ...) in pkg/historyserver/router.go.
	ClusterPrefix = "ray_cluster_history/raycluster/default/my-cluster"
	// FileName is a session-relative log path of the shape the readers see.
	FileName = "session_2026-05-08_18-35-06_774618_1/logs/node123/events/event_CORE_WORKER_256.log"
)

// Recorder collects what a test server was asked for. Requests are served on the
// server's own goroutines, so access is mutex-guarded to stay clean under -race.
type Recorder struct {
	mu     sync.Mutex
	values []string
}

// Add records one value seen by the test server.
func (rec *Recorder) Add(value string) {
	rec.mu.Lock()
	defer rec.mu.Unlock()
	rec.values = append(rec.values, value)
}

// Snapshot returns a copy of what has been recorded so far, so a caller can
// assert on it without holding the lock or racing further requests.
func (rec *Recorder) Snapshot() []string {
	rec.mu.Lock()
	defer rec.mu.Unlock()
	return append([]string(nil), rec.values...)
}
