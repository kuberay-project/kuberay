package historyserver

import (
	"context"
	"errors"
	"fmt"

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
	// session is intentionally skipped.
	SessionStatusLive
	// SessionStatusProcessed means events were ingested into EventHandler's
	// in-memory state.
	SessionStatusProcessed
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
}

// NewSessionProcessor constructs a SessionProcessor.
func NewSessionProcessor(reader storage.StorageReader, k8sClient client.Client) *SessionProcessor {
	return &SessionProcessor{
		reader:    reader,
		k8sClient: k8sClient,
	}
}

// ProcessSession processes one session end-to-end.
func (p *SessionProcessor) ProcessSession(ctx context.Context, session utils.ClusterInfo) (SessionStatus, *eventserver.SessionSnapshot, error) {
	dead, err := p.isDead(ctx, session)
	if err != nil {
		if isCtxCanceled(err) {
			return SessionStatusCanceled, nil, err
		}
		return SessionStatusClusterStateUnknown, nil, fmt.Errorf("check cluster state for %s/%s: %w", session.Namespace, session.Name, err)
	}
	if !dead {
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

	return SessionStatusProcessed, h.BuildSnapshot(session), nil
}

// isDead determines if the RayCluster CR is absent.
//
// Known limit: An old session of a still-running RayCluster will be misclassified as live.
func (p *SessionProcessor) isDead(ctx context.Context, session utils.ClusterInfo) (bool, error) {
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
