package gcs

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	gstorage "cloud.google.com/go/storage"
	"google.golang.org/api/option"
)

func forEachContentAPI(t *testing.T, run func(*testing.T, bool)) {
	t.Helper()
	for _, api := range []string{"xml", "json"} {
		t.Run(api, func(t *testing.T) { run(t, api == "json") })
	}
}

func newContentTestHandler(t *testing.T, serve http.HandlerFunc, rootDir string, jsonReads bool) *RayLogsHandler {
	t.Helper()
	server := httptest.NewServer(serve)
	t.Cleanup(server.Close)
	opts := []option.ClientOption{
		option.WithEndpoint(server.URL), option.WithHTTPClient(server.Client()), option.WithoutAuthentication(),
	}
	if jsonReads {
		opts = append(opts, gstorage.WithJSONReads())
	}
	client, err := gstorage.NewClient(context.Background(), opts...)
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

func contentReadObject(r *http.Request) (string, bool) {
	if r.URL.Query().Get("alt") == "media" {
		_, object, found := strings.Cut(r.URL.Path, "/o/")
		return object, found
	}
	const xmlPrefix = "/test-bucket/"
	return strings.TrimPrefix(r.URL.Path, xmlPrefix), strings.HasPrefix(r.URL.Path, xmlPrefix)
}

func writeContentError(w http.ResponseWriter, code int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_, _ = fmt.Fprintf(w, `{"error":{"code":%d,"message":"test error"}}`, code)
}

func assertContent(t *testing.T, reader io.Reader, want string) {
	t.Helper()
	if reader == nil {
		t.Fatal("GetContent returned nil for an existing object")
	}
	data, err := io.ReadAll(reader)
	if err != nil || string(data) != want {
		t.Fatalf("read = %q, %v; want %q", data, err, want)
	}
}

func TestCanonicalContentPath(t *testing.T) {
	for _, tt := range []struct {
		prefix, fileName string
		want             bool
	}{
		{"cluster-history/raycluster/ns/cluster", "session_123/node/logs/file.log", true},
		{"cluster-history/rayjob/ns/job/cluster", "session_123/fetched_endpoints/timezone", true},
		{"cluster-history/rayservice/ns/service/cluster", "session_123/node/logs/file.log.chunks/000", true},
		{"clusterA", "file.log", false},
		{"clusters/clusterA_ns", "session123/logs/file.log", false},
		{"cluster-history/rayjob/ns/job", "session_123/node/logs/file.log", false},
		{"cluster-history/unknown/ns/cluster", "session_123/node/logs/file.log", false},
		{"cluster-history/raycluster/ns/cluster*", "session_123/node/logs/file.log", false},
		{"cluster-history/raycluster//cluster", "session_123/node/logs/file.log", false},
		{"cluster-history/raycluster/ns/cluster", "file.log", false},
		{"cluster-history/raycluster/ns/cluster", "session_123", false},
		{"cluster-history/raycluster/ns/cluster", "session_123/", false},
		{"cluster-history/raycluster/ns/cluster", "session_123/../file.log", false},
		{"cluster-history/raycluster/ns/cluster", "/session_123/node/logs/file.log", false},
	} {
		t.Run(tt.prefix+"/"+tt.fileName, func(t *testing.T) {
			if got := isCanonicalContentPath(tt.prefix, tt.fileName); got != tt.want {
				t.Fatalf("isCanonicalContentPath(%q, %q) = %v; want %v", tt.prefix, tt.fileName, got, tt.want)
			}
		})
	}
}

func TestGetContentReadsExactObjectWithoutListing(t *testing.T) {
	forEachContentAPI(t, func(t *testing.T, jsonReads bool) {
		for _, tt := range []struct {
			name, rootDir, prefix, fileName, content string
		}{
			{"session file", "", "cluster-history/rayjob/ns/job/cluster", "session_123/node/logs/events/event_CORE_WORKER.log", "event"},
			{"configured root", "archive", "cluster-history/raycluster/ns/cluster", "session_123/node/logs/raylet.out", "log"},
			{"leading root slash", "/archive/", "cluster-history/rayservice/ns/service/cluster", "session_123/node/logs/worker log.out", "log with spaces"},
			{"empty file", "", "cluster-history/raycluster/ns/cluster", "session_123/node/logs/empty.log", ""},
			{"chunk", "", "cluster-history/raycluster/ns/cluster", "session_123/node/logs/worker.log.chunks/000", "chunk"},
			{"query in stored filename", "", "cluster-history/raycluster/ns/cluster", "session_123/fetched_endpoints/endpoint?detail=1", "snapshot"},
		} {
			t.Run(tt.name, func(t *testing.T) {
				var gets, lists atomic.Int64
				root := strings.Trim(tt.rootDir, "/")
				objectPath := tt.prefix + "/" + tt.fileName
				if root != "" {
					objectPath = root + "/" + objectPath
				}
				h := newContentTestHandler(t, func(w http.ResponseWriter, r *http.Request) {
					object, isRead := contentReadObject(r)
					if !isRead {
						lists.Add(1)
						t.Error("an exact object read must not list the bucket")
						writeContentError(w, http.StatusForbidden)
						return
					}
					gets.Add(1)
					if object != objectPath {
						t.Errorf("object = %q; want %q", object, objectPath)
						writeContentError(w, http.StatusNotFound)
						return
					}
					_, _ = io.WriteString(w, tt.content)
				}, tt.rootDir, jsonReads)
				assertContent(t, h.GetContent(tt.prefix, tt.fileName), tt.content)
				if gets.Load() != 1 || lists.Load() != 0 {
					t.Fatalf("requests: %d reads, %d lists; want 1 read, 0 lists", gets.Load(), lists.Load())
				}
			})
		}
	})
}

func TestGetContentLegacySearchDoesNotProbeExactKey(t *testing.T) {
	forEachContentAPI(t, func(t *testing.T, jsonReads bool) {
		for _, tt := range []struct {
			name, prefix, fileName, fallbackPath, glob string
		}{
			{
				"partial cluster name", "clusterA", "important.log",
				"archive/clusters/clusterA_ns/sessions/session123/logs/important.log",
				"archive/**/clusterA*/**/important.log",
			},
			{
				"legacy hierarchy", "clusters/clusterA_ns", "session123/logs/important.log",
				"archive/older/clusters/clusterA_ns/session123/logs/important.log",
				"archive/**/clusters/clusterA_ns*/**/session123/logs/important.log",
			},
			{
				"basename below canonical prefix", "cluster-history/rayjob/ns/job/cluster", "important.log",
				"archive/cluster-history/rayjob/ns/job/cluster/session_123/node/logs/important.log",
				"archive/**/cluster-history/rayjob/ns/job/cluster*/**/important.log",
			},
		} {
			t.Run(tt.name, func(t *testing.T) {
				var gets, lists, probes atomic.Int64
				h := newContentTestHandler(t, func(w http.ResponseWriter, r *http.Request) {
					if object, isRead := contentReadObject(r); isRead {
						gets.Add(1)
						if object != tt.fallbackPath {
							// A restricted IAM binding can deny the guessed path
							// while allowing the object returned by listing.
							probes.Add(1)
							writeContentError(w, http.StatusForbidden)
							return
						}
						_, _ = io.WriteString(w, "legacy log")
						return
					}
					lists.Add(1)
					if got := r.URL.Query().Get("matchGlob"); got != tt.glob {
						t.Errorf("glob = %q; want %q", got, tt.glob)
					}
					w.Header().Set("Content-Type", "application/json")
					_ = json.NewEncoder(w).Encode(map[string]any{"items": []map[string]string{{"name": tt.fallbackPath, "bucket": "test-bucket"}}})
				}, "archive", jsonReads)
				assertContent(t, h.GetContent(tt.prefix, tt.fileName), "legacy log")
				if gets.Load() != 1 || lists.Load() != 1 || probes.Load() != 0 {
					t.Fatalf("requests: %d reads, %d lists, %d probes; want 1 read, 1 list, 0 probes", gets.Load(), lists.Load(), probes.Load())
				}
			})
		}
	})
}

func TestGetContentCanonicalFallback(t *testing.T) {
	const prefix = "cluster-history/rayjob/ns/job/cluster"
	const fileName = "session_123/node/logs/important.log"
	const exact = "archive/" + prefix + "/" + fileName
	const fallback = "archive/legacy/" + prefix + "/" + fileName
	forEachContentAPI(t, func(t *testing.T, jsonReads bool) {
		for _, tt := range []struct {
			name                   string
			found                  bool
			listCode, fallbackCode int
			want                   string
			gets                   int64
		}{
			{"found", true, http.StatusOK, http.StatusOK, "legacy log", 2},
			{"missing", false, http.StatusOK, 0, "", 1},
			{"listing denied", false, http.StatusForbidden, 0, "", 1},
			{"deleted after listing", true, http.StatusOK, http.StatusNotFound, "", 2},
		} {
			t.Run(tt.name, func(t *testing.T) {
				var gets, lists atomic.Int64
				h := newContentTestHandler(t, func(w http.ResponseWriter, r *http.Request) {
					if object, isRead := contentReadObject(r); isRead {
						gets.Add(1)
						switch object {
						case exact:
							writeContentError(w, http.StatusNotFound)
						case fallback:
							if tt.fallbackCode != http.StatusOK {
								writeContentError(w, tt.fallbackCode)
							} else {
								_, _ = io.WriteString(w, "legacy log")
							}
						default:
							t.Errorf("unexpected object: %s", object)
							writeContentError(w, http.StatusForbidden)
						}
						return
					}
					lists.Add(1)
					if tt.listCode != http.StatusOK {
						writeContentError(w, tt.listCode)
						return
					}
					items := []map[string]string{}
					if tt.found {
						items = append(items, map[string]string{"name": fallback, "bucket": "test-bucket"})
					}
					w.Header().Set("Content-Type", "application/json")
					_ = json.NewEncoder(w).Encode(map[string]any{"items": items})
				}, "archive", jsonReads)
				reader := h.GetContent(prefix, fileName)
				if tt.want == "" {
					if reader != nil {
						t.Fatal("GetContent returned a reader for an unavailable object")
					}
				} else {
					assertContent(t, reader, tt.want)
				}
				if gets.Load() != tt.gets || lists.Load() != 1 {
					t.Fatalf("requests: %d reads, %d lists; want %d reads, 1 list", gets.Load(), lists.Load(), tt.gets)
				}
			})
		}
	})
}

func TestGetContentCanonicalErrors(t *testing.T) {
	forEachContentAPI(t, func(t *testing.T, jsonReads bool) {
		for _, code := range []int{http.StatusForbidden, http.StatusServiceUnavailable} {
			t.Run(http.StatusText(code), func(t *testing.T) {
				var gets, lists atomic.Int64
				h := newContentTestHandler(t, func(w http.ResponseWriter, r *http.Request) {
					if _, isRead := contentReadObject(r); !isRead {
						lists.Add(1)
						t.Error("a non-404 read failure must not trigger listing")
						writeContentError(w, http.StatusForbidden)
						return
					}
					n := gets.Add(1)
					if code == http.StatusServiceUnavailable && n > 1 {
						_, _ = io.WriteString(w, "retried log")
					} else {
						writeContentError(w, code)
					}
				}, "", jsonReads)
				h.StorageClient.SetRetry(gstorage.WithMaxAttempts(2))
				reader := h.GetContent("cluster-history/raycluster/ns/cluster", "session_123/node/logs/raylet.out")
				wantGets := int64(1)
				if code == http.StatusServiceUnavailable {
					assertContent(t, reader, "retried log")
					wantGets = 2
				} else if reader != nil {
					t.Fatal("GetContent returned a reader after permission was denied")
				}
				if gets.Load() != wantGets || lists.Load() != 0 {
					t.Fatalf("requests: %d reads, %d lists; want %d reads, 0 lists", gets.Load(), lists.Load(), wantGets)
				}
			})
		}
	})
}
