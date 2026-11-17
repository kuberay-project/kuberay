package historyserver

import (
	"context"
	"errors"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"github.com/ray-project/kuberay/historyserver/pkg/utils"
	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
)

func newFakeK8sClient(t *testing.T, rc *rayv1.RayCluster) client.Client {
	t.Helper()
	scheme := runtime.NewScheme()
	utilruntime.Must(rayv1.AddToScheme(scheme))
	b := fake.NewClientBuilder().WithScheme(scheme)
	if rc != nil {
		b = b.WithObjects(rc)
	}
	return b.Build()
}

func rayCluster(namespace, name string) *rayv1.RayCluster {
	return &rayv1.RayCluster{
		ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name},
	}
}

func TestIsDead(t *testing.T) {
	const (
		ns             = "default"
		name           = "raycluster-test"
		queriedSession = "session_2026-04-22_10-00-00_000000_1"
	)

	tests := []struct {
		name     string
		cr       *rayv1.RayCluster // nil = RayCluster CR absent
		wantDead bool
	}{
		{
			name:     "RayCluster CR absent -> dead",
			cr:       nil,
			wantDead: true,
		},
		{
			name:     "RayCluster CR present -> alive",
			cr:       rayCluster(ns, name),
			wantDead: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			k := newFakeK8sClient(t, tc.cr)
			p := NewSessionProcessor(nil, k, false)

			info := utils.ClusterInfo{Namespace: ns, Name: name, SessionName: queriedSession}
			gotDead, err := p.IsDead(context.Background(), info)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if gotDead != tc.wantDead {
				t.Fatalf("IsDead = %v, want %v", gotDead, tc.wantDead)
			}
		})
	}
}

func TestProcessSession_ServeRunningClusters(t *testing.T) {
	const (
		ns      = "default"
		name    = "raycluster-test"
		session = "session_2026-04-22_10-00-00_000000_1"
	)
	info := utils.ClusterInfo{Namespace: ns, Name: name, SessionName: session}

	tests := []struct {
		name         string
		serveRunning bool
		cr           *rayv1.RayCluster
		wantStatus   SessionStatus
		wantSnapshot bool
	}{
		{"flag off, CR present -> live, no snapshot", false, rayCluster(ns, name), SessionStatusLive, false},
		{"flag off, CR absent -> processed", false, nil, SessionStatusProcessed, true},
		{"flag on, CR present -> processed running from storage", true, rayCluster(ns, name), SessionStatusProcessedRunning, true},
		{"flag on, CR absent -> processed", true, nil, SessionStatusProcessed, true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			p := NewSessionProcessor(&mockStorageReader{}, newFakeK8sClient(t, tc.cr), tc.serveRunning)

			status, snap, err := p.ProcessSession(context.Background(), info)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if status != tc.wantStatus {
				t.Fatalf("status = %v, want %v", status, tc.wantStatus)
			}
			if (snap != nil) != tc.wantSnapshot {
				t.Fatalf("snapshot != nil is %v, want %v", snap != nil, tc.wantSnapshot)
			}
		})
	}
}

// TestProcessSession_ClusterStateCheckError verifies that a failed RayCluster
// GET fails the load only in live cluster mode. When running clusters are served
// from storage, the session is still loaded and marked running for revalidation.
func TestProcessSession_ClusterStateCheckError(t *testing.T) {
	info := utils.ClusterInfo{Namespace: "default", Name: "raycluster-test", SessionName: "session_2026-04-22_10-00-00_000000_1"}

	tests := []struct {
		name         string
		serveRunning bool
		wantStatus   SessionStatus
		wantErr      bool
	}{
		{"serveRunning flag off -> cluster state unknown", false, SessionStatusClusterStateUnknown, true},
		{"serveRunning flag on -> processed running from storage", true, SessionStatusProcessedRunning, false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			scheme := runtime.NewScheme()
			utilruntime.Must(rayv1.AddToScheme(scheme))
			// Make every Kubernetes Get return an error.
			k := fake.NewClientBuilder().WithScheme(scheme).WithInterceptorFuncs(interceptor.Funcs{
				Get: func(context.Context, client.WithWatch, client.ObjectKey, client.Object, ...client.GetOption) error {
					return errors.New("apiserver unavailable")
				},
			}).Build()
			p := NewSessionProcessor(&mockStorageReader{}, k, tc.serveRunning)

			status, _, err := p.ProcessSession(context.Background(), info)
			if (err != nil) != tc.wantErr {
				t.Fatalf("err = %v, wantErr %v", err, tc.wantErr)
			}
			if status != tc.wantStatus {
				t.Fatalf("status = %v, want %v", status, tc.wantStatus)
			}
		})
	}
}
