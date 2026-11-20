package azureblob

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/sirupsen/logrus"

	"github.com/ray-project/kuberay/historyserver/pkg/storage/internal/storagetest"
	"github.com/ray-project/kuberay/historyserver/pkg/utils"
)

func TestTrim(t *testing.T) {
	tmpRayRoot := utils.GetTmpRayRoot()
	absoluteLogPathName := fmt.Sprintf(" %s/test/LLogs/events/aa/a.txt  ", tmpRayRoot)
	logdir := fmt.Sprintf("%s/test/lLogs/", tmpRayRoot)

	absoluteLogPathName = strings.TrimSpace(absoluteLogPathName)
	absoluteLogPathName = filepath.Clean(absoluteLogPathName)

	logdir = strings.TrimSpace(logdir)
	logdir = filepath.Clean(logdir)

	relativePath := strings.TrimPrefix(absoluteLogPathName, logdir+"/")
	// Split relative path into subdir and filename
	subdir, filename := filepath.Split(relativePath)
	testPathJoin := path.Join("aa./b/c/d", "e")
	t.Logf("file [%s] logdir [%s] subdir %s filename %s", absoluteLogPathName,
		logdir, subdir, filename)
	t.Logf("testPathJoin [%s]", testPathJoin)
}

// GetContent builds its blob path from three pieces, and a deployment that sets a
// root dir only works if all three end up in the path. These tests assert on the
// path that actually reaches the server rather than on a helper's return value.
// The three path pieces and the request Recorder live in storagetest, since every
// backend's tests ask the same questions and only the transport differs.
const testContainer = "test-container"

func newTestHandler(t *testing.T, srv *httptest.Server) *RayLogsHandler {
	t.Helper()

	client, err := container.NewClientWithNoCredential(srv.URL+"/"+testContainer, nil)
	if err != nil {
		t.Fatalf("creating test container client: %v", err)
	}

	return &RayLogsHandler{
		ContainerClient: client,
		ContainerName:   testContainer,
		RootDir:         storagetest.RootDir,
	}
}

// blobPath returns the blob the request addressed, and whether the request was a
// container listing rather than a blob download.
func blobPath(r *http.Request) (name string, isList bool) {
	name = strings.TrimPrefix(r.URL.Path, "/"+testContainer)
	name = strings.TrimPrefix(name, "/")
	return name, r.URL.Query().Get("comp") == "list"
}

func TestGetContentUsesRootDir(t *testing.T) {
	wantPath := path.Join(storagetest.RootDir, storagetest.ClusterPrefix, storagetest.FileName)
	const wantContent = "core worker log line"

	var requested storagetest.Recorder
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		name, isList := blobPath(r)
		if isList {
			writeListResult(w, r.URL.Query().Get("prefix"))
			return
		}
		requested.Add(name)
		if name != wantPath {
			writeBlobNotFound(w)
			return
		}
		_, _ = io.WriteString(w, wantContent)
	}))
	defer srv.Close()

	reader := newTestHandler(t, srv).GetContent(storagetest.ClusterPrefix, storagetest.FileName)
	gotPaths := requested.Snapshot()
	if reader == nil {
		t.Fatalf("GetContent returned nil; blobs requested: %v, want %q", gotPaths, wantPath)
	}

	got, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("reading returned content: %v", err)
	}
	if string(got) != wantContent {
		t.Errorf("content = %q, want %q", got, wantContent)
	}
	if len(gotPaths) == 0 || gotPaths[0] != wantPath {
		t.Errorf("first requested blob = %v, want %q", gotPaths, wantPath)
	}
}

// When the direct download fails, GetContent lists the containing directory and
// retries any blob whose full path matches. That listing prefix has to be rooted
// too, or the retry has nothing to find. The first download here fails with a
// server error so the fallback is the only way to reach the content.
func TestGetContentFallbackListsUnderRootDir(t *testing.T) {
	wantPath := path.Join(storagetest.RootDir, storagetest.ClusterPrefix, storagetest.FileName)
	const wantContent = "recovered log line"

	var listed storagetest.Recorder
	var downloads storagetest.Recorder
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		name, isList := blobPath(r)
		if isList {
			prefix := r.URL.Query().Get("prefix")
			listed.Add(prefix)
			if strings.HasPrefix(wantPath, prefix) {
				writeListResult(w, prefix, wantPath)
				return
			}
			writeListResult(w, prefix)
			return
		}
		downloads.Add(name)
		// Miss the first attempt so the fallback has to do the work. BlobNotFound
		// is used rather than a server error because the SDK retries the latter,
		// which would satisfy the download before the fallback ever runs.
		if name != wantPath || len(downloads.Snapshot()) == 1 {
			writeBlobNotFound(w)
			return
		}
		_, _ = io.WriteString(w, wantContent)
	}))
	defer srv.Close()

	reader := newTestHandler(t, srv).GetContent(storagetest.ClusterPrefix, storagetest.FileName)
	listPrefixes := listed.Snapshot()
	if reader == nil {
		t.Fatalf("GetContent returned nil; list prefixes tried: %v", listPrefixes)
	}

	got, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("reading returned content: %v", err)
	}
	if string(got) != wantContent {
		t.Errorf("content = %q, want %q", got, wantContent)
	}

	wantPrefix := path.Dir(wantPath) + "/"
	for _, prefix := range listPrefixes {
		if prefix == wantPrefix {
			return
		}
	}
	t.Errorf("list prefixes = %v, want one equal to %q", listPrefixes, wantPrefix)
}

func writeBlobNotFound(w http.ResponseWriter) {
	w.Header().Set("x-ms-error-code", "BlobNotFound")
	w.WriteHeader(http.StatusNotFound)
}

func writeListResult(w http.ResponseWriter, prefix string, names ...string) {
	var blobs strings.Builder
	for _, name := range names {
		blobs.WriteString(fmt.Sprintf("<Blob><Name>%s</Name><Properties></Properties></Blob>", name))
	}
	w.Header().Set("Content-Type", "application/xml")
	_, _ = io.WriteString(w, fmt.Sprintf(`<?xml version="1.0" encoding="utf-8"?>
<EnumerationResults ContainerName="%s"><Prefix>%s</Prefix><Delimiter>/</Delimiter><Blobs>%s</Blobs><NextMarker /></EnumerationResults>`, testContainer, prefix, blobs.String()))
}

func TestWalk(t *testing.T) {
	watchPath := fmt.Sprintf("%s/test/LLogs/", utils.GetTmpRayRoot())
	if err := filepath.Walk(watchPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			logrus.Errorf("Walk path error %v", err)
			return err
		}

		if !info.IsDir() {
			logrus.Infof("Find new file %s", path)
		}
		return nil
	}); err != nil {
		t.Logf("Walk %s: %v", watchPath, err)
	}
}
