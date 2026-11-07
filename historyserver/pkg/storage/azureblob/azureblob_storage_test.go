package azureblob

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"

	"github.com/ray-project/kuberay/historyserver/pkg/storage/internal/storagetest"
	"github.com/ray-project/kuberay/historyserver/pkg/utils"
)

// The four methods below round out what gcs_handler_test.go already covers for
// the gcs backend: List, ListFiles, CreateDirectory and WriteFile. As in the
// GetContent tests, the assertions are on the requests that reach the server.

const metadataPrefix = storagetest.RootDir + "/cluster-metadata/"

// writeHierarchicalListResult answers a delimiter listing: blobs go in Blobs,
// "subdirectories" in BlobPrefix entries.
func writeHierarchicalListResult(w http.ResponseWriter, prefix string, names []string, blobPrefixes []string) {
	var body strings.Builder
	for _, name := range names {
		body.WriteString(fmt.Sprintf("<Blob><Name>%s</Name><Properties></Properties></Blob>", name))
	}
	for _, bp := range blobPrefixes {
		body.WriteString(fmt.Sprintf("<BlobPrefix><Name>%s</Name></BlobPrefix>", bp))
	}
	w.Header().Set("Content-Type", "application/xml")
	_, _ = io.WriteString(w, fmt.Sprintf(`<?xml version="1.0" encoding="utf-8"?>
<EnumerationResults ContainerName="%s"><Prefix>%s</Prefix><Delimiter>/</Delimiter><Blobs>%s</Blobs><NextMarker /></EnumerationResults>`,
		testContainer, prefix, body.String()))
}

// ListFiles has to hand back files without a trailing slash and subdirectories
// with one: callers such as ServerHandler.listFilesRecursive tell the two apart
// that way. Directory marker blobs are neither. This backend does not write
// them itself, but they are a common Azure convention and other writers leave
// them behind, so a listing may still return one.
func TestListFilesSeparatesFilesFromDirectories(t *testing.T) {
	const dir = "session_2026-05-08_18-35-06_774618_1/logs/node123/events"
	wantPrefix := path.Join(storagetest.RootDir, storagetest.ClusterPrefix, dir) + "/"

	var listed storagetest.Recorder
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		prefix := r.URL.Query().Get("prefix")
		listed.Add(prefix)
		if prefix != wantPrefix {
			writeListResult(w, prefix)
			return
		}
		if r.URL.Query().Get("delimiter") != "/" {
			// A flat listing reports no BlobPrefix entries and returns
			// everything below the prefix instead, so a request that forgets
			// the delimiter gets the nested blob rather than the subdirectory
			// it stands for.
			writeHierarchicalListResult(w, prefix,
				[]string{prefix, prefix + "event_RAYLET.log", prefix + "event_GCS.log", prefix + "old/rotated.log"},
				nil)
			return
		}
		writeHierarchicalListResult(w, prefix,
			[]string{prefix, prefix + "event_RAYLET.log", prefix + "event_GCS.log"},
			[]string{prefix + "old/"})
	}))
	defer srv.Close()

	got := newTestHandler(t, srv).ListFiles(storagetest.ClusterPrefix, dir)

	// Order is up to the backend; the contract is which entries come back and
	// whether each carries the trailing slash.
	want := []string{"event_GCS.log", "event_RAYLET.log", "old/"}
	if diff := cmp.Diff(want, got, cmpopts.SortSlices(func(a, b string) bool { return a < b })); diff != "" {
		t.Errorf("ListFiles() diff (-want +got):\n%s\nprefixes listed: %v", diff, listed.Snapshot())
	}
}

// The blob names are written out in full here rather than built with
// clustermetadata.EncodePath, so that a change in the layout has to be made in
// two places before this test agrees with it.
func TestListReadsClusterMetadataUnderRootDir(t *testing.T) {
	newerSession := "session_2026-05-08_18-35-06_774618"
	olderSession := "session_2026-05-07_09-00-00_000001"
	newer := time.Date(2026, 5, 8, 18, 35, 6, 774618000, time.UTC)
	older := time.Date(2026, 5, 7, 9, 0, 0, 1000, time.UTC)

	var listed storagetest.Recorder
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		prefix := r.URL.Query().Get("prefix")
		listed.Add(prefix)
		if prefix != metadataPrefix {
			writeListResult(w, prefix)
			return
		}
		writeListResult(w, prefix,
			metadataPrefix+"raycluster/defaultns_mycluster1/"+olderSession,
			metadataPrefix+"rayjob/defaultns_myrayjob_mycluster2/"+newerSession,
			// A directory marker and a malformed entry: neither can be decoded,
			// and neither may take the whole listing down with it.
			metadataPrefix+"raycluster/",
			metadataPrefix+"raycluster/not-a-cluster-dir",
		)
	}))
	defer srv.Close()

	got := newTestHandler(t, srv).List()

	// ClusterInfoList sorts newest first, so the rayjob entry leads.
	want := []utils.ClusterInfo{
		{
			Name: "mycluster2", Namespace: "defaultns",
			OwnerKind: "rayjob", OwnerName: "myrayjob",
			SessionName:     newerSession,
			CreateTimeStamp: newer.Unix(),
			CreateTime:      "2026-05-08T18:35:06Z",
		},
		{
			Name: "mycluster1", Namespace: "defaultns",
			SessionName:     olderSession,
			CreateTimeStamp: older.Unix(),
			CreateTime:      "2026-05-07T09:00:00Z",
		},
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("List() diff (-want +got):\n%s\nprefixes listed: %v", diff, listed.Snapshot())
	}
}

// Unlike s3 and aliyunoss, this backend deliberately writes no marker blob:
// virtual directories are inferred from blob paths, and a marker shows up as
// "<no name>" in Azure Storage Explorer. Pin that down so the empty body is
// read as a decision rather than as something left unfinished.
func TestCreateDirectoryWritesNothing(t *testing.T) {
	var requests storagetest.Recorder
	srv := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		name, _ := blobPath(r)
		requests.Add(r.Method + " " + name)
	}))
	defer srv.Close()

	handler := newTestHandler(t, srv)
	if err := handler.CreateDirectory(path.Join(storagetest.RootDir, storagetest.ClusterPrefix, "session_1/logs/node123/events")); err != nil {
		t.Fatalf("CreateDirectory: %v", err)
	}

	if sent := requests.Snapshot(); len(sent) != 0 {
		t.Errorf("CreateDirectory talked to the container: %v", sent)
	}
}

func TestWriteFileUploadsBodyToGivenBlob(t *testing.T) {
	const name = "ray-logs/ray_cluster_history/raycluster/default/my-cluster/session_1/logs/node123/raylet.out"
	const content = "raylet line one\nraylet line two\n"

	var uploads storagetest.Recorder
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotName, _ := blobPath(r)
		if r.Method != http.MethodPut {
			t.Errorf("unexpected %s request for %q", r.Method, gotName)
			return
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("reading uploaded body: %v", err)
			return
		}
		uploads.Add(gotName + "|" + string(body))
		w.WriteHeader(http.StatusCreated)
	}))
	defer srv.Close()

	if err := newTestHandler(t, srv).WriteFile(name, bytes.NewReader([]byte(content))); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	want := []string{name + "|" + content}
	if diff := cmp.Diff(want, uploads.Snapshot()); diff != "" {
		t.Errorf("uploads diff (-want +got):\n%s", diff)
	}
}
