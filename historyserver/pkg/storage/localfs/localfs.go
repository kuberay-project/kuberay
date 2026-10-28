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
	"bytes"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/ray-project/kuberay/historyserver/pkg/collector/types"
	"github.com/ray-project/kuberay/historyserver/pkg/storage"
	"github.com/ray-project/kuberay/historyserver/pkg/utils"
)

// tmpInfix marks an in-progress write; a crash between os.Create and
// os.Rename can leave one behind, so listings below must filter it out.
const tmpInfix = ".tmp."

// tmpFileRe matches WriteFile's tmp suffix (<file>.tmp.<pid>.<nanos>),
// anchored so it can't match a legitimate name that contains ".tmp.".
var tmpFileRe = regexp.MustCompile(`\.tmp\.\d+\.\d+$`)

func isTmpFile(name string) bool {
	return tmpFileRe.MatchString(name)
}

// withinRoot cleans p and reports whether it is RootDir itself or something
// under it. clusterId/fileName ultimately come from HTTP request parameters
// (see pkg/historyserver/reader.go), and path.Join alone does not stop a
// crafted "../.." from resolving outside RootDir — every entry point below
// must check this before touching the filesystem.
//
// filepath.Rel (not a root+"/" prefix check) is what makes this correct for
// RootDir values like "." or "/": path.Join(".", "a") cleans away the "./"
// prefix entirely, and root+"/" for RootDir="/" becomes "//" — neither ever
// matches a real path with a plain prefix check.
func (h *FSHandler) withinRoot(p string) (string, bool) {
	p = path.Clean(p)
	root := path.Clean(h.RootDir)
	rel, err := filepath.Rel(root, p)
	if err != nil || rel == ".." || strings.HasPrefix(rel, "../") {
		return "", false
	}
	return p, true
}

// FSHandler implements StorageWriter/StorageReader against a POSIX
// filesystem (a JuiceFS FUSE mount) — no S3 gateway hop needed.
type FSHandler struct {
	RootDir string
}

// jd is unused: unlike s3/gcs/azureblob, localfs has nothing to configure
// beyond RootDir, which the registry already passes via c.RootDir.
func NewWriter(c *types.RayCollectorConfig, jd map[string]interface{}) (storage.StorageWriter, error) {
	if c.RootDir == "" {
		return nil, fmt.Errorf("localfs: RootDir (--ray-root-dir) must be set")
	}
	return &FSHandler{RootDir: c.RootDir}, nil
}

func NewReader(c *types.RayHistoryServerConfig, jd map[string]interface{}) (storage.StorageReader, error) {
	if c.RootDir == "" {
		return nil, fmt.Errorf("localfs: RootDir (--ray-root-dir) must be set")
	}
	return &FSHandler{RootDir: c.RootDir}, nil
}

func (h *FSHandler) CreateDirectory(dir string) error {
	p, ok := h.withinRoot(dir)
	if !ok {
		return fmt.Errorf("path %q escapes root %q", dir, h.RootDir)
	}
	return os.MkdirAll(p, 0o755)
}

// WriteFile replaces file's content.
func (h *FSHandler) WriteFile(file string, r io.ReadSeeker) error {
	file, ok := h.withinRoot(file)
	if !ok {
		return fmt.Errorf("path %q escapes root %q", file, h.RootDir)
	}
	if err := os.MkdirAll(path.Dir(file), 0o755); err != nil {
		return err
	}

	// Callers overwrite the same path in full on every call (e.g. an hourly
	// event bucket), so write to a tmp file and rename for atomicity — a
	// plain Create+Copy could expose a half-written file to a concurrent
	// GetContent. tmp must be in the same dir: rename is only atomic within
	// one filesystem, and the system tmpdir may be a different mount.
	//
	// A crash between Create and Rename leaves the tmp file on disk; List/
	// ListFiles/GetContent filter it out of results, but nothing reclaims
	// the space it uses. Not handled here — sweep stale .tmp. files
	// externally if that ever becomes a real problem.
	tmp := fmt.Sprintf("%s%s%d.%d", file, tmpInfix, os.Getpid(), time.Now().UnixNano())
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err := io.Copy(f, r); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, file); err != nil {
		os.Remove(tmp)
		return err
	}
	return nil
}

// ListFiles lists the immediate children of RootDir/clusterId/dir: files
// as-is, directories suffixed with "/".
func (h *FSHandler) ListFiles(clusterId string, dir string) []string {
	full, ok := h.withinRoot(path.Join(h.RootDir, clusterId, dir))
	if !ok {
		logrus.Errorf("Rejected path escaping root: clusterId=%q dir=%q", clusterId, dir)
		return []string{}
	}

	entries, err := os.ReadDir(full)
	if err != nil {
		logrus.Errorf("Failed to read dir %s: %v", full, err)
		return []string{}
	}

	files := make([]string, 0, len(entries))
	for _, e := range entries {
		if isTmpFile(e.Name()) { // leftover from a crashed write
			continue
		}
		name := e.Name()
		if e.IsDir() {
			name += "/"
		}
		files = append(files, name)
	}
	return files
}

// List walks RootDir/metadir and parses each marker's path:
// metadir/<name>_<namespace>/<sessionName>.
func (h *FSHandler) List() []utils.ClusterInfo {
	root := path.Join(h.RootDir, "metadir")
	clusters := make(utils.ClusterInfoList, 0, 10)

	err := filepath.WalkDir(root, func(p string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() || isTmpFile(d.Name()) { // skip tmp leftovers
			return nil
		}

		rel := strings.TrimPrefix(filepath.ToSlash(p), filepath.ToSlash(root))
		rel = strings.Trim(rel, "/")
		metas := strings.Split(rel, "/")
		if len(metas) < 2 {
			return nil
		}

		namespaceName := strings.Split(metas[0], "_")
		if len(namespaceName) < 2 {
			return nil
		}

		sessionInfo := strings.Split(metas[1], "_")
		if len(sessionInfo) < 3 {
			return nil
		}
		createTime, err := time.Parse("2006-01-02_15-04-05", sessionInfo[1]+"_"+sessionInfo[2])
		if err != nil {
			logrus.Errorf("Failed to parse time from session %s: %v", metas[1], err)
			return nil
		}

		clusters = append(clusters, utils.ClusterInfo{
			Name:            namespaceName[0],
			Namespace:       namespaceName[1],
			SessionName:     metas[1],
			CreateTimeStamp: createTime.Unix(),
			CreateTime:      createTime.UTC().Format("2006-01-02T15:04:05Z"),
		})
		return nil
	})
	if err != nil {
		logrus.Errorf("Failed to walk %s: %v", root, err)
	}

	sort.Sort(clusters)
	return clusters
}

// GetContent reads RootDir/clusterId/fileName, falling back to a recursive
// search by base name if the exact path misses.
func (h *FSHandler) GetContent(clusterId string, fileName string) io.Reader {
	full, ok := h.withinRoot(path.Join(h.RootDir, clusterId, fileName))
	if !ok {
		logrus.Errorf("Rejected path escaping root: clusterId=%q fileName=%q", clusterId, fileName)
		return nil
	}

	if data, err := os.ReadFile(full); err == nil {
		return bytes.NewReader(data)
	} else if !os.IsNotExist(err) {
		// Not a plain miss (e.g. a JuiceFS I/O error) — the fallback below
		// would otherwise make this look identical to "file doesn't exist".
		logrus.Errorf("Failed to read %s: %v", full, err)
	}

	dir := path.Dir(full)
	base := path.Base(full)

	var found []byte
	if err := filepath.WalkDir(dir, func(p string, d os.DirEntry, err error) error {
		if err != nil {
			logrus.Errorf("Failed to walk %s: %v", p, err)
			return nil
		}
		if d.IsDir() || found != nil || isTmpFile(d.Name()) {
			return nil
		}
		if d.Name() == base {
			data, readErr := os.ReadFile(p)
			if readErr != nil {
				logrus.Errorf("Failed to read %s: %v", p, readErr)
				return nil
			}
			found = data
		}
		return nil
	}); err != nil {
		logrus.Errorf("Failed to walk %s: %v", dir, err)
	}

	if found == nil {
		logrus.Errorf("Failed to get file %s", full)
		return nil
	}
	return bytes.NewReader(found)
}
