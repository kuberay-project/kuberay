package run

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/ray-project/kuberay/release-test/pkg/util"
	"github.com/ray-project/kuberay/release-test/pkg/util/report"
)

func TestShell(t *testing.T) {
	work := t.TempDir()
	state := &unitRun{work: work, out: filepath.Join(work, ".out")}
	log, err := os.Create(filepath.Join(work, "log"))
	require.NoError(t, err)
	defer log.Close()
	ctx := context.Background()

	code, _ := state.shell(ctx, 10*time.Second, log, "false\necho not reached")
	assert.Equal(t, 1, code, "bash -e stops at the first failing command")

	t.Setenv(util.EnvKuberayVersion, "v9.9.9")
	code, _ = state.shell(ctx, 10*time.Second, log, `test "$KUBERAY_VERSION" = v9.9.9`)
	assert.Equal(t, 0, code, "steps inherit the environment")

	start := time.Now()
	_, timedOut := state.shell(ctx, time.Second, log, "sleep 30")
	assert.True(t, timedOut)
	assert.Less(t, time.Since(start), 8*time.Second)
}

// fakeCluster answers the kubectl and helm calls of runUnit.
type fakeCluster struct {
	made          []string                       // listed from the second snapshot on, as if the unit's steps created them
	madeInDefault []metav1.PartialObjectMetadata // the same, for the namespaced objects in default
	noBaseline    bool                           // the first snapshot fails
	snapshots     int
	pods          []metav1.PartialObjectMetadata // listed for the evidence
	deleted       []string                       // the delete and uninstall commands run
	collected     []string                       // the describe and logs commands run
	onCall        func(call string)              // runs before each call is answered
}

func inDefault(kind, name string, owners ...metav1.OwnerReference) metav1.PartialObjectMetadata {
	return metav1.PartialObjectMetadata{
		TypeMeta:   metav1.TypeMeta{Kind: kind},
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: name, OwnerReferences: owners},
	}
}

func (c *fakeCluster) cli(ctx context.Context, stdout, _ io.Writer, name string, args ...string) error {
	call := strings.Join(append([]string{name}, args...), " ")
	if c.onCall != nil {
		c.onCall(call)
	}
	// Like exec, a canceled ctx fails the call.
	if err := ctx.Err(); err != nil {
		return err
	}
	switch {
	case call == "helm list -A -o json":
		fmt.Fprint(stdout, "[]")
	case strings.HasPrefix(call, "kubectl get namespaces,"):
		c.snapshots++
		if c.snapshots == 1 && c.noBaseline {
			return errors.New("connection refused")
		}
		if c.snapshots > 1 {
			fmt.Fprint(stdout, strings.Join(c.made, "\n"))
		}
	case strings.HasPrefix(call, "kubectl get pods,"):
		var list metav1.PartialObjectMetadataList
		if c.snapshots > 1 {
			list.Items = c.madeInDefault
		}
		return json.NewEncoder(stdout).Encode(list)
	case call == "kubectl get pods -o json -A":
		return json.NewEncoder(stdout).Encode(metav1.PartialObjectMetadataList{Items: c.pods})
	case strings.HasPrefix(call, "kubectl delete"), strings.HasPrefix(call, "helm uninstall"):
		c.deleted = append(c.deleted, call)
	case strings.HasPrefix(call, "kubectl describe"), strings.HasPrefix(call, "kubectl logs"):
		c.collected = append(c.collected, call)
	}
	return nil
}

func TestRunUnit(t *testing.T) {
	// A kubectl or helm call that misses the fake fails instead of reaching a real cluster.
	t.Setenv("KUBECONFIG", filepath.Join(t.TempDir(), "none"))
	const sha = "0123456789abcdef"
	results := t.TempDir()
	newRunner := func(cluster *fakeCluster) *runner {
		return &runner{path: util.Paths{Results: results}, version: "v9.9.9", cli: cluster.cli}
	}
	ctx := context.Background()

	t.Run("pass", func(t *testing.T) {
		unit := util.Unit{ID: "ok", SourceSHA: sha, Steps: []util.Step{{Run: "echo hello", Check: `grep -qx hello "$OUT"`}}}
		failed, err := newRunner(&fakeCluster{}).runUnit(ctx, util.Item{ID: "ok", Source: "x.yaml"}, unit)
		require.NoError(t, err)
		assert.Equal(t, 0, failed)
		metadata, err := report.ReadFrontMatter(filepath.Join(results, "ok", "report.md"))
		require.NoError(t, err)
		assert.Equal(t, "pass", metadata.Status)
		assert.NoDirExists(t, filepath.Join(results, "ok", "evidence"))
	})

	t.Run("fail", func(t *testing.T) {
		cluster := &fakeCluster{
			made: []string{"namespace/leftover"},
			madeInDefault: []metav1.PartialObjectMetadata{
				inDefault("Pod", "curl"),
				inDefault("Pod", "web-0", metav1.OwnerReference{Kind: "StatefulSet", Name: "web"}),
			},
			pods: []metav1.PartialObjectMetadata{
				{ObjectMeta: metav1.ObjectMeta{Namespace: "kube-system", Name: "coredns"}},
				{ObjectMeta: metav1.ObjectMeta{Namespace: "ray", Name: "head"}},
				{ObjectMeta: metav1.ObjectMeta{Namespace: "ray", Name: "worker"}},
			},
		}
		pidFile := filepath.Join(t.TempDir(), "bg.pid")
		t.Setenv("PID_FILE", pidFile)
		// Steps share the work directory; a background process outlives its step, not the unit.
		unit := util.Unit{ID: "broken", SourceSHA: sha, Steps: []util.Step{
			{Name: "Port-forward", Run: "sleep 300 >/dev/null 2>&1 &\necho $! > bg.pid\ncp bg.pid \"$PID_FILE\""},
			{Name: "Still forwarding", Run: `kill -0 "$(cat bg.pid)"`},
			{Name: "Fails", Run: "false"},
			{Name: "Never reached", Run: "true"},
		}, Cleanup: []string{"echo cleaned"}}
		failed, err := newRunner(cluster).runUnit(ctx, util.Item{ID: "broken", Procedure: "p"}, unit)
		require.NoError(t, err)
		assert.Equal(t, 3, failed)
		dir := filepath.Join(results, "broken")
		metadata, err := report.ReadFrontMatter(filepath.Join(dir, "report.md"))
		require.NoError(t, err)
		assert.Equal(t, "fail", metadata.Status)

		body := readFile(t, filepath.Join(dir, "report.md"))
		assert.Contains(t, body, "| 3 | Fails | fail: run exit 1 |")
		assert.Contains(t, body, "| 4 | Never reached | not run | | |")
		assert.Contains(t, body, "- namespace/leftover")
		assert.Contains(t, body, "- default/Pod/curl")
		assert.NotContains(t, body, "web-0", "an object with an owner goes with its owner")
		assert.Contains(t, readFile(t, filepath.Join(dir, "steps", "cleanup.log")), "cleaned")
		assert.FileExists(t, filepath.Join(dir, "evidence", "pods.txt"))
		assert.Equal(t, []string{
			"kubectl describe pods -n ray",
			"kubectl logs -n ray head --all-containers --prefix --tail=500",
			"kubectl logs -n ray worker --all-containers --prefix --tail=500",
		}, cluster.collected, "evidence skips kube-system and describes each namespace once")
		assert.Equal(t, []string{
			"kubectl delete Pod/curl -n default --ignore-not-found --timeout=60s",
			"kubectl delete --timeout=120s namespace/leftover",
		}, cluster.deleted)
		pid := readPID(t, pidFile)
		assert.Eventually(t, func() bool { return processGone(pid) }, 5*time.Second, 100*time.Millisecond, "the port-forward-like process survived the unit")
	})

	t.Run("snapshot fails", func(t *testing.T) {
		cluster := &fakeCluster{made: []string{"namespace/orphan"}, noBaseline: true}
		unit := util.Unit{ID: "blind", SourceSHA: sha, Steps: []util.Step{{Run: "true"}}}
		_, err := newRunner(cluster).runUnit(ctx, util.Item{ID: "blind", Procedure: "p"}, unit)
		require.NoError(t, err)
		assert.Contains(t, readFile(t, filepath.Join(results, "blind", "report.md")), "## Sweep skipped")
		assert.Empty(t, cluster.deleted, "nothing may be deleted when the baseline is unknown")
	})

	t.Run("interrupted", func(t *testing.T) {
		cctx, cancel := context.WithCancel(ctx)
		time.AfterFunc(300*time.Millisecond, cancel)
		unit := util.Unit{ID: "stopped", SourceSHA: sha, Steps: []util.Step{{Run: "sleep 30"}}, Cleanup: []string{"echo cleaned after interrupt"}}
		_, err := newRunner(&fakeCluster{}).runUnit(cctx, util.Item{ID: "stopped", Procedure: "p"}, unit)
		require.ErrorIs(t, err, errInterrupted)
		dir := filepath.Join(results, "stopped")
		assert.NoFileExists(t, filepath.Join(dir, "report.md"), "an interrupted unit must run again next time")
		assert.Contains(t, readFile(t, filepath.Join(dir, "steps", "cleanup.log")), "cleaned after interrupt", "cleanup runs even though ctx is canceled")
	})

	t.Run("interrupted during evidence", func(t *testing.T) {
		cctx, cancel := context.WithCancel(ctx)
		defer cancel()
		cluster := &fakeCluster{made: []string{"namespace/leftover"}, onCall: func(call string) {
			if call == "kubectl get pods -A -o wide" {
				cancel()
			}
		}}
		unit := util.Unit{ID: "late", SourceSHA: sha, Steps: []util.Step{{Run: "false"}}, Cleanup: []string{"echo cleaned after late interrupt"}}
		_, err := newRunner(cluster).runUnit(cctx, util.Item{ID: "late", Procedure: "p"}, unit)
		require.ErrorIs(t, err, errInterrupted)
		dir := filepath.Join(results, "late")
		assert.NoFileExists(t, filepath.Join(dir, "report.md"))
		assert.Contains(t, readFile(t, filepath.Join(dir, "steps", "cleanup.log")), "cleaned after late interrupt")
		assert.Equal(t, []string{"kubectl delete --timeout=120s namespace/leftover"}, cluster.deleted)
	})
}

func readFile(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(filepath.Clean(path))
	require.NoError(t, err)
	return string(data)
}

func readPID(t *testing.T, path string) int {
	t.Helper()
	pid, err := strconv.Atoi(strings.TrimSpace(readFile(t, path)))
	require.NoError(t, err)
	return pid
}

// processGone: pid is gone; an unreaped zombie counts as gone.
func processGone(pid int) bool {
	err := syscall.Kill(pid, 0)
	stat, _ := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/stat")
	return errors.Is(err, syscall.ESRCH) || strings.Contains(string(stat), ") Z ")
}
