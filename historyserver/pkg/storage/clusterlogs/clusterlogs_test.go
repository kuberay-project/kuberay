package clusterlogs

import (
	"io"
	"slices"
	"strings"
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
	files   map[string][]string // dir -> entries
	content map[string]string   // object path -> content
	gets    []string            // GetContent calls, in order
}

func (m *mockStorageReader) List() []utils.ClusterInfo {
	return nil
}

func (m *mockStorageReader) GetContent(_ string, fileName string) io.Reader {
	m.gets = append(m.gets, fileName)
	if content, ok := m.content[fileName]; ok {
		return strings.NewReader(content)
	}
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

func TestListLogFiles(t *testing.T) {
	reader := &mockStorageReader{files: map[string][]string{
		"logs": {"raylet.out", "job-driver-x.log.chunks/", "worker-a.out", "worker-a.out.chunks/", "events/"},
	}}

	got := ListLogFiles(reader, "prefix", "logs")

	// Chunk directories are hidden; a chunk-only file appears once, as the file.
	want := []string{"raylet.out", "job-driver-x.log", "worker-a.out", "events/"}
	if !slices.Equal(got, want) {
		t.Fatalf("ListLogFiles() = %v, want %v", got, want)
	}
}

func TestReadLogFile(t *testing.T) {
	const logPath = "logs/job-driver-x.log"
	chunk := func(name string) string { return logPath + ChunkDirSuffix + "/" + name }

	tests := map[string]struct {
		files   []string          // listing of the chunk directory
		content map[string]string // objects in storage
		want    string
		wantNil bool
	}{
		"chunks win and the whole object is never fetched": {
			files:   []string{"00000000000000000000"},
			content: map[string]string{logPath: "whole", chunk("00000000000000000000"): "chunk"},
			want:    "chunk",
		},
		"whole object is read when there are no chunks": {
			content: map[string]string{logPath: "whole"},
			want:    "whole",
		},
		"chunks are joined in offset order regardless of listing order": {
			files: []string{"00000000000000000005", "00000000000000000000", "00000000000000000008"},
			content: map[string]string{
				chunk("00000000000000000000"): "line1",
				chunk("00000000000000000005"): "lin",
				chunk("00000000000000000008"): "e2",
			},
			want: "line1line2",
		},
		"stale chunks left by a collector restart are skipped, later chunks still join": {
			// Chunk 0 was re-uploaded after a restart and covers [0,18); the
			// chunks at 6 and 12 are from before the restart. The chunk at 18
			// was written after the restart and must still be reached.
			files: []string{"00000000000000000000", "00000000000000000006", "00000000000000000012", "00000000000000000018"},
			content: map[string]string{
				chunk("00000000000000000000"): "line1 line2 line3 ",
				chunk("00000000000000000006"): "line2 ",
				chunk("00000000000000000012"): "line3 ",
				chunk("00000000000000000018"): "line4 ",
			},
			want: "line1 line2 line3 line4 ",
		},
		"a real gap stops the join": {
			files: []string{"00000000000000000000", "00000000000000000010"},
			content: map[string]string{
				chunk("00000000000000000000"): "line1",
				chunk("00000000000000000010"): "late",
			},
			want: "line1",
		},
		"neither whole file nor chunks": {
			wantNil: true,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			reader := &mockStorageReader{
				files:   map[string][]string{logPath + ChunkDirSuffix: tc.files},
				content: tc.content,
			}

			got := ReadLogFile(reader, "prefix", logPath)

			if tc.wantNil {
				if got != nil {
					t.Fatalf("ReadLogFile() = non-nil, want nil")
				}
				return
			}
			if got == nil {
				t.Fatalf("ReadLogFile() = nil, want %q", tc.want)
			}
			data, err := io.ReadAll(got)
			if err != nil {
				t.Fatalf("ReadAll: %v", err)
			}
			if string(data) != tc.want {
				t.Fatalf("ReadLogFile() = %q, want %q", data, tc.want)
			}
			if len(tc.files) > 0 && slices.Contains(reader.gets, logPath) {
				t.Fatalf("ReadLogFile() fetched the whole object although chunks exist: %v", reader.gets)
			}
		})
	}
}
