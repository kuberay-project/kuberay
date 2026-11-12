package gcs

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	gstorage "cloud.google.com/go/storage"
	"google.golang.org/api/option"
)

func newContentTestHandler(t *testing.T, serve http.HandlerFunc, rootDir string) *RayLogsHandler {
	t.Helper()
	server := httptest.NewServer(serve)
	t.Cleanup(server.Close)
	client, err := gstorage.NewClient(context.Background(),
		option.WithEndpoint(server.URL), option.WithHTTPClient(server.Client()),
		option.WithoutAuthentication(), gstorage.WithJSONReads())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := client.Close(); err != nil {
			t.Error(err)
		}
	})
	return &RayLogsHandler{StorageClient: client, GCSBucket: "test-bucket", RootDir: rootDir}
}

func TestGetContentReadsExactObjectWithoutListing(t *testing.T) {
	for _, tt := range []struct {
		name, rootDir, prefix, fileName, objectPath, content string
	}{
		{"session file", "", "cluster-history/rayjob/ns/job/cluster", "session/node/logs/events/event_CORE_WORKER.log", "cluster-history/rayjob/ns/job/cluster/session/node/logs/events/event_CORE_WORKER.log", "event"},
		{"configured root", "archive", "cluster-history/raycluster/ns/cluster", "session/logs/raylet.out", "archive/cluster-history/raycluster/ns/cluster/session/logs/raylet.out", "log"},
		{"leading slash", "/archive/", "cluster", "session/logs/worker log.out", "archive/cluster/session/logs/worker log.out", "log with spaces"},
		{"empty file", "", "cluster", "session/logs/empty.log", "cluster/session/logs/empty.log", ""},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var gets, lists atomic.Int64
			h := newContentTestHandler(t, func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Query().Get("alt") != "media" {
					lists.Add(1)
					t.Error("an exact object read must not list the bucket")
					http.Error(w, "unexpected list", http.StatusForbidden)
					return
				}
				gets.Add(1)
				if !strings.HasSuffix(r.URL.Path, "/o/"+tt.objectPath) {
					t.Errorf("unexpected object path: %s", r.URL.Path)
					http.NotFound(w, r)
					return
				}
				_, _ = io.WriteString(w, tt.content)
			}, tt.rootDir)
			reader := h.GetContent(tt.prefix, tt.fileName)
			if reader == nil {
				t.Fatal("GetContent returned nil for an existing object")
			}
			data, err := io.ReadAll(reader)
			if err != nil || string(data) != tt.content {
				t.Fatalf("read = %q, %v; want %q", data, err, tt.content)
			}
			if gets.Load() != 1 || lists.Load() != 0 {
				t.Fatalf("requests: %d reads, %d lists; want 1 read, 0 lists", gets.Load(), lists.Load())
			}
		})
	}
}

func TestGetContentKeepsLegacySearchAfterNotFound(t *testing.T) {
	const fallbackPath = "archive/clusters/clusterA_ns/sessions/session123/logs/important.log"
	var gets, lists atomic.Int64
	h := newContentTestHandler(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("alt") == "media" {
			gets.Add(1)
			switch {
			case strings.HasSuffix(r.URL.Path, "/o/archive/clusterA/important.log"):
				http.NotFound(w, r)
			case strings.HasSuffix(r.URL.Path, "/o/"+fallbackPath):
				_, _ = io.WriteString(w, "legacy log")
			default:
				t.Errorf("unexpected object path: %s", r.URL.Path)
				http.Error(w, "unexpected object", http.StatusForbidden)
			}
			return
		}
		lists.Add(1)
		if got := r.URL.Query().Get("matchGlob"); got != "archive/**/clusterA*/**/important.log" {
			t.Errorf("unexpected fallback glob: %s", got)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"items": []map[string]string{{"name": fallbackPath, "bucket": "test-bucket"}}})
	}, "archive")
	reader := h.GetContent("clusterA", "important.log")
	if reader == nil {
		t.Fatal("GetContent returned nil for a legacy object")
	}
	data, err := io.ReadAll(reader)
	if err != nil || string(data) != "legacy log" {
		t.Fatalf("read = %q, %v; want legacy log", data, err)
	}
	if gets.Load() != 2 || lists.Load() != 1 {
		t.Fatalf("requests: %d reads, %d lists; want 2 reads, 1 list", gets.Load(), lists.Load())
	}
}

func TestGetContentDoesNotSearchAfterPermissionDenied(t *testing.T) {
	var gets, lists atomic.Int64
	h := newContentTestHandler(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("alt") == "media" {
			gets.Add(1)
		} else {
			lists.Add(1)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusForbidden)
		_, _ = io.WriteString(w, `{"error":{"code":403,"message":"forbidden"}}`)
	}, "archive")
	if reader := h.GetContent("cluster", "session/logs/raylet.out"); reader != nil {
		t.Fatal("GetContent returned a reader after permission was denied")
	}
	if gets.Load() != 1 || lists.Load() != 0 {
		t.Fatalf("requests: %d reads, %d lists; want 1 read, 0 lists", gets.Load(), lists.Load())
	}
}
