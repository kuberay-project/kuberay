package historyserver

import (
	"context"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

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
			p := NewSessionProcessor(nil, k)

			info := utils.ClusterInfo{Namespace: ns, Name: name, SessionName: queriedSession}
			gotDead, err := p.isDead(context.Background(), info)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if gotDead != tc.wantDead {
				t.Fatalf("isDead = %v, want %v", gotDead, tc.wantDead)
			}
		})
	}
}
