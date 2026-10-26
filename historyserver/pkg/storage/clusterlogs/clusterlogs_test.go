package clusterlogs

import (
	"io"
	"slices"
	"testing"

	"github.com/ray-project/kuberay/historyserver/pkg/utils"
)

func TestClusterLogsPaths(t *testing.T) {
	rootDir := ""
	ownerKind := "rayjob"
	ownerName := "job-1"
	ns := "default"
	cluster := "cluster-1"
	session := "session-1"
	node := "node-1"
	jobID := "01000000"

	wantPrefix := "cluster-history/rayjob/default/job-1/cluster-1"
	if got := Prefix(rootDir, ownerKind, ownerName, ns, cluster); got != wantPrefix {
		t.Errorf("Prefix() = %q, want %q", got, wantPrefix)
	}

	wantSession := wantPrefix + "/session-1"
	if got := SessionDir(rootDir, ownerKind, ownerName, ns, cluster, session); got != wantSession {
		t.Errorf("SessionDir() = %q, want %q", got, wantSession)
	}

	wantFetchedEndpoints := wantSession + "/fetched_endpoints"
	if got := FetchedEndpointsDir(wantPrefix, session); got != wantFetchedEndpoints {
		t.Errorf("FetchedEndpointsDir() = %q, want %q", got, wantFetchedEndpoints)
	}

	wantNode := wantSession + "/node-1"
	if got := NodeDir(rootDir, ownerKind, ownerName, ns, cluster, session, node); got != wantNode {
		t.Errorf("NodeDir() = %q, want %q", got, wantNode)
	}

	wantLogs := wantNode + "/logs"
	if got := LogsDir(rootDir, ownerKind, ownerName, ns, cluster, session, node); got != wantLogs {
		t.Errorf("LogsDir() = %q, want %q", got, wantLogs)
	}

	wantNodeEvents := wantNode + "/node_events"
	if got := NodeEventsDir(rootDir, ownerKind, ownerName, ns, cluster, session, node); got != wantNodeEvents {
		t.Errorf("NodeEventsDir() = %q, want %q", got, wantNodeEvents)
	}

	wantJobEvents := wantNode + "/job_events/01000000"
	if got := JobEventsDir(rootDir, ownerKind, ownerName, ns, cluster, session, node, jobID); got != wantJobEvents {
		t.Errorf("JobEventsDir() = %q, want %q", got, wantJobEvents)
	}

	wantJobEventsNoID := wantNode + "/job_events"
	if got := JobEventsDir(rootDir, ownerKind, ownerName, ns, cluster, session, node, ""); got != wantJobEventsNoID {
		t.Errorf("JobEventsDir(no jobID) = %q, want %q", got, wantJobEventsNoID)
	}

	if got := RelLogsDir(session, node); got != "session-1/node-1/logs" {
		t.Errorf("RelLogsDir() = %q", got)
	}
	if got := RelNodeEventsDir(session, node); got != "session-1/node-1/node_events" {
		t.Errorf("RelNodeEventsDir() = %q", got)
	}
	if got := RelJobEventsDir(session, node, jobID); got != "session-1/node-1/job_events/01000000" {
		t.Errorf("RelJobEventsDir() = %q", got)
	}
}

type mockStorageReader struct {
	files map[string][]string
}

func (m *mockStorageReader) List() []utils.ClusterInfo {
	return nil
}

func (m *mockStorageReader) GetContent(_, _ string) io.Reader {
	return nil
}

func (m *mockStorageReader) ListFiles(_ string, dir string) []string {
	if entries, ok := m.files[dir]; ok {
		return entries
	}
	return nil
}

func TestListSessionNodeDirs(t *testing.T) {
	tests := []struct {
		name        string
		sessionName string
		dirEntries  []string
		expected    []string
	}{
		{
			name:        "returns node directories",
			sessionName: "session-1",
			dirEntries:  []string{"node-a/", "node-b/"},
			expected:    []string{"node-a", "node-b"},
		},
		{
			name:        "skips files and fetched_endpoints",
			sessionName: "session-1",
			dirEntries: []string{
				"node-a/",
				"file.txt",
				utils.RAY_SESSIONDIR_FETCHED_ENDPOINTS_NAME + "/",
				"/",
			},
			expected: []string{"node-a"},
		},
		{
			name:        "empty listing",
			sessionName: "session-1",
			dirEntries:  nil,
			expected:    nil,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			reader := &mockStorageReader{
				files: map[string][]string{
					tc.sessionName: tc.dirEntries,
				},
			}
			got := ListSessionNodeDirs(reader, "prefix", tc.sessionName)
			if !slices.Equal(got, tc.expected) {
				t.Errorf("ListSessionNodeDirs() = %v, want %v", got, tc.expected)
			}
		})
	}
}
