package historyserver

import (
	"context"
	"errors"
	"fmt"

	"github.com/sirupsen/logrus"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	k8stypes "k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ray-project/kuberay/historyserver/pkg/eventserver"
	"github.com/ray-project/kuberay/historyserver/pkg/storage"
	"github.com/ray-project/kuberay/historyserver/pkg/utils"
	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
)

// SessionStatus is ProcessSession's outcome classification. Success statuses
// (Live, Processed) return a nil error; all others pair with a non-nil error.
type SessionStatus int

const (
	// SessionStatusUnknown is the zero value, reserved as a defensive guard.
	SessionStatusUnknown SessionStatus = iota
	// SessionStatusLive means the RayCluster CR is still present and the
	// session is intentionally skipped. Never returned when the processor
	// serves running clusters from storage.
	SessionStatusLive
	// SessionStatusProcessed means events were ingested into EventHandler's
	// in-memory state.
	SessionStatusProcessed
	// SessionStatusProcessedRunning is SessionStatusProcessed for a session whose
	// RayCluster CR still exists, so its data may still grow. Only returned when
	// the processor serves running clusters from storage.
	SessionStatusProcessedRunning
	// SessionStatusClusterStateUnknown means the cluster state could not be
	// determined (e.g., transient API failure).
	SessionStatusClusterStateUnknown
	// SessionStatusEventsErr means event parsing failed.
	SessionStatusEventsErr
	// SessionStatusCanceled means ctx was canceled mid-processing.
	SessionStatusCanceled
)

// SessionProcessor processes a single session end-to-end: dead detection and
// event parsing. It is stateless across sessions and safe for concurrent use.
type SessionProcessor struct {
	reader    storage.StorageReader
	k8sClient client.Client
	// serveRunningClusters builds snapshots for sessions whose RayCluster CR
	// still exists, instead of reporting them as live. Set when the live proxy
	// is disabled, so running clusters are served from storage.
	serveRunningClusters bool
}

// NewSessionProcessor constructs a SessionProcessor.
func NewSessionProcessor(reader storage.StorageReader, k8sClient client.Client, serveRunningClusters bool) *SessionProcessor {
	return &SessionProcessor{
		reader:               reader,
		k8sClient:            k8sClient,
		serveRunningClusters: serveRunningClusters,
	}
}

// ProcessSession processes one session end-to-end.
func (p *SessionProcessor) ProcessSession(ctx context.Context, session utils.ClusterInfo) (SessionStatus, *eventserver.SessionSnapshot, error) {
	dead, err := p.IsDead(ctx, session)
	if err != nil {
		if isCtxCanceled(err) {
			return SessionStatusCanceled, nil, err
		}
		if !p.serveRunningClusters {
			return SessionStatusClusterStateUnknown, nil, fmt.Errorf("check cluster state for %s/%s: %w", session.Namespace, session.Name, err)
		}
		// Running clusters are also served from storage, so a failed Kubernetes API
		// check should not fail the load. Treat the cluster as running so Revalidate
		// checks it again later.
		logrus.Warnf("Failed to check cluster state for %s/%s, serving it as running: %v", session.Namespace, session.Name, err)
		dead = false
	}
	if !dead && !p.serveRunningClusters {
		return SessionStatusLive, nil, nil
	}

	// Use per-call EventHandler so ingestion maps become GC-eligible once the snapshot is built.
	h := eventserver.NewEventHandler(p.reader)
	if err := h.ProcessSingleSession(ctx, session); err != nil {
		if isCtxCanceled(err) {
			return SessionStatusCanceled, nil, err
		}
		return SessionStatusEventsErr, nil, fmt.Errorf("process events for %s/%s: %w", session.Namespace, session.Name, err)
	}

	if !dead {
		return SessionStatusProcessedRunning, h.BuildSnapshot(session), nil
	}
	return SessionStatusProcessed, h.BuildSnapshot(session), nil
}

// IsDead determines if the RayCluster CR is absent.
//
// Known limit: An old session of a still-running RayCluster will be misclassified as live.
func (p *SessionProcessor) IsDead(ctx context.Context, session utils.ClusterInfo) (bool, error) {
	rc := &rayv1.RayCluster{}
	err := p.k8sClient.Get(ctx, k8stypes.NamespacedName{
		Namespace: session.Namespace,
		Name:      session.Name,
	}, rc)
	if apierrors.IsNotFound(err) {
		return true, nil
	}
	if err != nil {
		return false, err
	}
	return false, nil
}

// isCtxCanceled checks if the error is caused by context cancellation
// or deadline exceeded.
func isCtxCanceled(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}
