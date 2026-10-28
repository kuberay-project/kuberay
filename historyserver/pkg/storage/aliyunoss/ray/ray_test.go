package ray

import (
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strings"
	"testing"

	"github.com/sirupsen/logrus"

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
	// relativePath := strings.TrimPrefix(absoluteLogPathName, logdir)
	// Split relative path into subdir and filename
	subdir, filename := filepath.Split(relativePath)
	testPathJoin := path.Join("aa./b/c/d", "e")
	t.Logf("file [%s] logdir [%s] subdir %s filename %s", absoluteLogPathName,
		logdir, subdir, filename)
	t.Logf("testPathJoin [%s]", testPathJoin)
}

func TestGetContentPathComparison(t *testing.T) {
	// fileName is a full path like the caller passes
	fileName := "session_2026-04-09_06-41-42_754637_1/logs/abc123/events/event_RAYLET.log"
	// f is a full key returned by _listFiles (onlyBase=false)
	f := "session_2026-04-09_06-41-42_754637_1/logs/abc123/events/event_RAYLET.log"

	if path.Base(f) != path.Base(fileName) {
		t.Errorf("expected path.Base(%q) == path.Base(%q)", f, fileName)
	}
	// Verify old buggy comparison would fail
	if path.Base(f) == fileName {
		t.Errorf("old comparison should not match: path.Base(%q) == %q", f, fileName)
	}
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
