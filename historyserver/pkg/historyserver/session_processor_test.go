package historyserver

import (
	"context"
	"errors"
	"io"
	"path"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"github.com/ray-project/kuberay/historyserver/pkg/storage/clusterlogs"
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

func rayCluster() *rayv1.RayCluster {
	return &rayv1.RayCluster{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "raycluster-test"},
	}
}

type optionalSnapshotReader struct {
	submissionStorageReader
	dirs          map[string][]string
	optional      io.Reader
	optionalPath  string
	optionalReads int
}

func (r *optionalSnapshotReader) ListFiles(prefix, dir string) []string {
	return r.dirs[path.Join(prefix, dir)]
}

func (r *optionalSnapshotReader) GetContent(prefix, filename string) io.Reader {
	if path.Join(prefix, filename) == r.optionalPath {
		r.optionalReads++
		return r.optional
	}
	return r.submissionStorageReader.GetContent(prefix, filename)
}

type optionalReadFailure struct {
	err    error
	cancel context.CancelFunc
	closed bool
}

func (r *optionalReadFailure) Read([]byte) (int, error) {
	if r.cancel != nil {
		r.cancel()
	}
	return 0, r.err
}

func (r *optionalReadFailure) Close() error { r.closed = true; return nil }

func TestProcessSessionOptionalSubmissions(t *testing.T) {
	for _, owner := range []struct{ kind, name, prefix string }{
		{"", "", "cluster-history/raycluster/default/raycluster-test"},
		{"rayjob", "synthetic-job", "cluster-history/rayjob/default/synthetic-job/raycluster-test"},
		{"rayservice", "synthetic-service", "cluster-history/rayservice/default/synthetic-service/raycluster-test"},
	} {
		for _, tc := range []struct {
			name, body  string
			want        int
			readFailure bool
		}{
			{"valid", syntheticFailedSubmission, 1, false},
			{"missing", "", 0, false},
			{"wrong shape", `{}`, 0, false},
			{"malformed", `[{`, 0, false},
			{"wrong row", `[{"submission_id":"bad","status":"INVALID"}]`, 0, false},
			{"partial", `[{"submission_id":"sub-only","status":"FAILED","runtime_env":{"synthetic":"discard"}},{"status":"INVALID"},{"type":"DRIVER","job_id":"02000000","status":"RUNNING"}]`, 1, false},
			{"drivers only", `[{"type":"DRIVER","job_id":"02000000","status":"RUNNING"}]`, 0, false},
			{"empty", `[]`, 0, false},
			{"read failed", "", 0, true},
		} {
			for _, state := range []struct {
				name       string
				cr         *rayv1.RayCluster
				wantStatus SessionStatus
			}{
				{"dead", nil, SessionStatusProcessed},
				{"running", rayCluster(), SessionStatusProcessedRunning},
			} {
				t.Run(owner.kind+"/"+tc.name+"/"+state.name, func(t *testing.T) {
					info := testClusterInfo()
					info.OwnerKind, info.OwnerName = owner.kind, owner.name
					prefix := clusterlogs.Prefix("", info)
					require.Equal(t, owner.prefix, prefix)
					eventPath := path.Join(prefix, info.SessionName, "node/node_events/node-2026-01-01-00")
					reader := &optionalSnapshotReader{
						optionalPath: path.Join(prefix, info.SessionName, "fetched_endpoints/restful__api__jobs"),
						dirs: map[string][]string{
							path.Join(prefix, info.SessionName):                     {"node/", "fetched_endpoints/"},
							path.Join(prefix, info.SessionName, "node/node_events"): {"node-2026-01-01-00"},
						},
						submissionStorageReader: submissionStorageReader{objects: map[string]string{eventPath: `[{"eventType":"NODE_DEFINITION_EVENT","nodeDefinitionEvent":{"nodeId":"YWJjZA=="}}]`}},
					}
					if tc.body != "" {
						reader.optional = strings.NewReader(tc.body)
					}
					failure := &optionalReadFailure{err: errors.New("synthetic read failure")}
					if tc.readFailure {
						reader.optional = failure
					}
					status, snap, err := NewSessionProcessor(reader, newFakeK8sClient(t, state.cr), true).ProcessSession(t.Context(), info)
					require.NoError(t, err)
					require.Equal(t, state.wantStatus, status)
					require.Len(t, snap.Submissions, tc.want)
					require.Contains(t, snap.Nodes, "61626364", "optional snapshot must preserve events")
					require.Empty(t, snap.Jobs, "never store submission IDs in core Jobs")
					require.Equal(t, 1, reader.optionalReads)
					for _, record := range snap.Submissions {
						require.Nil(t, record.RuntimeEnv)
					}
					if tc.readFailure {
						require.True(t, failure.closed)
					}
				})
			}
		}
	}
}

func TestProcessSessionOptionalSnapshotCancellationAndEventOutage(t *testing.T) {
	info := testClusterInfo()
	prefix := clusterlogs.Prefix("", info)
	optionalPath := path.Join(prefix, info.SessionName, "fetched_endpoints/restful__api__jobs")
	for _, tc := range []string{"before empty events", "during optional read", "read canceled", "read deadline", "all events fail"} {
		t.Run(tc, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			reader := &optionalSnapshotReader{optionalPath: optionalPath, optional: strings.NewReader(syntheticFailedSubmission)}
			want := SessionStatusCanceled
			var failure *optionalReadFailure
			switch tc {
			case "before empty events":
				cancel()
			case "during optional read":
				failure = &optionalReadFailure{err: io.EOF, cancel: cancel}
			case "read canceled":
				failure = &optionalReadFailure{err: context.Canceled}
			case "read deadline":
				failure = &optionalReadFailure{err: context.DeadlineExceeded}
			case "all events fail":
				want = SessionStatusEventsErr
				reader.dirs = map[string][]string{
					path.Join(prefix, info.SessionName):                     {"node/"},
					path.Join(prefix, info.SessionName, "node/node_events"): {"node-2026-01-01-00"},
				}
			}
			if failure != nil {
				reader.optional = failure
			}
			status, snap, err := NewSessionProcessor(reader, newFakeK8sClient(t, nil), true).ProcessSession(ctx, info)
			require.Error(t, err)
			require.Nil(t, snap)
			require.Equal(t, want, status)
			if want == SessionStatusCanceled {
				require.True(t, isCtxCanceled(err))
			} else {
				require.Contains(t, err.Error(), "read 0 of 1 event files")
			}
			if failure != nil {
				require.True(t, failure.closed)
			} else {
				require.Zero(t, reader.optionalReads)
			}
		})
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
			cr:       rayCluster(),
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
		{"flag off, CR present -> live, no snapshot", false, rayCluster(), SessionStatusLive, false},
		{"flag off, CR absent -> processed", false, nil, SessionStatusProcessed, true},
		{"flag on, CR present -> processed running from storage", true, rayCluster(), SessionStatusProcessedRunning, true},
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
