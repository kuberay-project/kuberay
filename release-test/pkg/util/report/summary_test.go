package report

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ray-project/kuberay/release-test/pkg/util"
)

func TestRenderSummary(t *testing.T) {
	results := t.TempDir()
	writeReport := func(id, frontMatter string) {
		require.NoError(t, os.MkdirAll(filepath.Join(results, id), 0o750))
		require.NoError(t, os.WriteFile(filepath.Join(results, id, "report.md"), []byte("---\n"+frontMatter+"---\n\n# "+id+"\n"), 0o600))
	}
	writeReport("doc", "status: pass\n")
	writeReport("broken", "status: fail\nfailed_step: 4\ntriage: doc-bug\nbrief: \"doc says 2 workers | sample has 1\"\n")
	writeReport("old", "status: stale\nbrief: source changed\n")
	writeReport("mangled", "status: fail\nbrief: page says: no\n")

	items := []util.Item{
		{ID: "doc"},
		{ID: "broken"},
		{ID: "old"},
		{ID: "mangled"},
		{ID: "planned"},
		{ID: "unplanned"},
		{ID: "gpu", Source: "g.yaml", Requires: []string{"gpu", "gke"}},
	}
	units := []util.Unit{
		{ID: "doc"},
		{ID: "broken"},
		{ID: "old"},
		{ID: "mangled"},
		{ID: "planned"},
	}
	summary, counts := renderSummary(results, items, units, "v9.9.9", time.Date(2026, 10, 10, 0, 0, 0, 0, time.UTC))

	assert.Equal(t, "1 pass, 1 fail, 1 stale, 1 manual, 1 not run, 1 not planned, 1 unreadable", counts)
	for _, want := range []string{
		"KubeRay v9.9.9, 2026-10-10. " + counts + ".",
		`| [broken](broken/report.md) | 4 | doc-bug | doc says 2 workers \| sample has 1 |`,
		"| gpu | gpu, gke | g.yaml |",
		"| [old](old/report.md) | source changed |",
		"| [mangled](mangled/report.md) | ",
	} {
		assert.Contains(t, summary, want)
	}
}
