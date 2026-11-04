// Package localfs implements the History Server storage backend that reads
// and writes directly against a POSIX filesystem (e.g. a JuiceFS FUSE mount).
/*
Copyright 2024 by the kuberay authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package localfs

import (
	"io"
	"os"
	"path"
	"strings"
	"testing"

	"github.com/ray-project/kuberay/historyserver/pkg/collector/types"
)

func TestWriteFileOverwritesAtomicallyAndLeavesNoTmp(t *testing.T) {
	root := t.TempDir()
	h := &FSHandler{RootDir: root}
	file := path.Join(root, "cluster_id", "session", "node_events", "node-2024010100")

	if err := h.WriteFile(file, strings.NewReader("first")); err != nil {
		t.Fatalf("first write: %v", err)
	}
	if err := h.WriteFile(file, strings.NewReader("second, longer content")); err != nil {
		t.Fatalf("second write: %v", err)
	}

	got, err := os.ReadFile(file)
	if err != nil {
		t.Fatalf("read back: %v", err)
	}
	if string(got) != "second, longer content" {
		t.Fatalf("got %q, want overwritten content", got)
	}

	entries, err := os.ReadDir(path.Dir(file))
	if err != nil {
		t.Fatalf("read dir: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected only the final file, found %d entries (leftover tmp?)", len(entries))
	}
}

func TestListFilesSkipsLeftoverTmp(t *testing.T) {
	root := t.TempDir()
	h := &FSHandler{RootDir: root}
	dir := path.Join(root, "cluster_id", "session", "node_events")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path.Join(dir, "node-2024010100"), []byte("ok"), 0o644); err != nil {
		t.Fatal(err)
	}
	// Simulate a crash between os.Create and os.Rename in WriteFile.
	if err := os.WriteFile(path.Join(dir, "node-2024010101"+tmpInfix+"12345.6789"), []byte("half"), 0o644); err != nil {
		t.Fatal(err)
	}

	got := h.ListFiles("cluster_id/session", "node_events")
	if len(got) != 1 || got[0] != "node-2024010100" {
		t.Fatalf("got %v, want only the completed file", got)
	}
}

func TestListSkipsLeftoverTmpMarker(t *testing.T) {
	root := t.TempDir()
	h := &FSHandler{RootDir: root}
	sessionDir := path.Join(root, "metadir", "mycluster_myns")
	if err := os.MkdirAll(sessionDir, 0o755); err != nil {
		t.Fatal(err)
	}
	// Real, completed marker.
	if err := os.WriteFile(path.Join(sessionDir, "session_2024-01-02_03-04-05_123_456"), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	// A marker write that crashed mid-flight.
	if err := os.WriteFile(path.Join(sessionDir, "session_2024-01-02_03-04-06_999_999"+tmpInfix+"1.2"), nil, 0o644); err != nil {
		t.Fatal(err)
	}

	clusters := h.List()
	if len(clusters) != 1 {
		t.Fatalf("got %d clusters, want 1 (tmp marker must be skipped): %+v", len(clusters), clusters)
	}
	if clusters[0].Name != "mycluster" || clusters[0].Namespace != "myns" {
		t.Fatalf("got %+v, want mycluster/myns", clusters[0])
	}
}

func TestGetContentExactPathAndBasenameFallback(t *testing.T) {
	root := t.TempDir()
	h := &FSHandler{RootDir: root}
	dir := path.Join(root, "cluster_id", "session", "node_events")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path.Join(dir, "node-2024010100"), []byte("exact"), 0o644); err != nil {
		t.Fatal(err)
	}

	// Exact path hit.
	r := h.GetContent("cluster_id/session", "node_events/node-2024010100")
	if r == nil {
		t.Fatal("expected content, got nil")
	}
	got, _ := io.ReadAll(r)
	if string(got) != "exact" {
		t.Fatalf("got %q, want %q", got, "exact")
	}

	// The caller passes a bare filename that doesn't exist at that exact
	// path; GetContent falls back to a recursive search by base name.
	r = h.GetContent("cluster_id/session", "node-2024010100")
	if r == nil {
		t.Fatal("expected fallback to find the file by basename, got nil")
	}
	got, _ = io.ReadAll(r)
	if string(got) != "exact" {
		t.Fatalf("fallback got %q, want %q", got, "exact")
	}

	// Genuinely missing file: no exact match, no basename match.
	if r := h.GetContent("cluster_id/session", "node_events/does-not-exist"); r != nil {
		t.Fatal("expected nil for a file that doesn't exist anywhere under the dir")
	}
}

func TestPathTraversalRejected(t *testing.T) {
	root := t.TempDir()
	h := &FSHandler{RootDir: root}

	if r := h.GetContent("../../../../etc", "passwd"); r != nil {
		t.Fatal("GetContent must reject a path escaping RootDir")
	}
	if got := h.ListFiles("..", ".."); len(got) != 0 {
		t.Fatalf("ListFiles must reject a path escaping RootDir, got %v", got)
	}
	if err := h.WriteFile(path.Join(root, "..", "escaped"), strings.NewReader("x")); err == nil {
		t.Fatal("WriteFile must reject a path escaping RootDir")
	}
	if err := h.CreateDirectory(path.Join(root, "..", "escaped-dir")); err == nil {
		t.Fatal("CreateDirectory must reject a path escaping RootDir")
	}
	if _, err := os.Stat(path.Join(root, "..", "escaped")); err == nil {
		t.Fatal("escaped file must not have been created outside RootDir")
	}
}

func TestWithinRootHandlesDotAndSlashRoot(t *testing.T) {
	// A plain root+"/" prefix check breaks for these: path.Join(".", "x")
	// cleans away the "./" prefix entirely, and "/" + "/" is "//", which no
	// real absolute path starts with.
	cases := []struct {
		name string
		root string
		p    string
		want bool
	}{
		{"dot root, nested path", ".", "cluster/session/logs/file", true},
		{"dot root, escape", ".", "../escaped", false},
		{"slash root, nested path", "/", "/cluster/session", true},
		{"slash root, relative input can't be reconciled", "/", "escaped", false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			h := &FSHandler{RootDir: c.root}
			if _, ok := h.withinRoot(c.p); ok != c.want {
				t.Fatalf("withinRoot(%q) with RootDir=%q: got %v, want %v", c.p, c.root, ok, c.want)
			}
		})
	}
}

func TestNewWriterAndReaderRejectEmptyRootDir(t *testing.T) {
	if _, err := NewWriter(&types.RayCollectorConfig{}, nil); err == nil {
		t.Fatal("NewWriter must reject an empty RootDir")
	}
	if _, err := NewReader(&types.RayHistoryServerConfig{}, nil); err == nil {
		t.Fatal("NewReader must reject an empty RootDir")
	}
}
