package run

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ray-project/kuberay/release-test/pkg/util"
	"github.com/ray-project/kuberay/release-test/pkg/util/report"
	"github.com/ray-project/kuberay/release-test/pkg/util/source"
)

func TestFresh(t *testing.T) {
	results := t.TempDir()
	run := &runner{path: util.Paths{Results: results}, version: "v9.9.9"}
	items := map[string]util.Item{
		"same":    {ID: "same", Procedure: "do this"},
		"changed": {ID: "changed", Procedure: "do that"},
	}
	units := []util.Unit{
		{ID: "same", SourceSHA: source.BlobSHA([]byte("do this"))},
		{ID: "changed", SourceSHA: "0000000000000000000000000000000000000000"},
	}
	fresh, err := run.fresh(context.Background(), items, units)
	require.NoError(t, err)
	require.Len(t, fresh, 1)
	assert.Equal(t, "same", fresh[0].ID)

	reportPath := filepath.Join(results, "changed", "report.md")
	metadata, err := report.ReadFrontMatter(reportPath)
	require.NoError(t, err)
	assert.Equal(t, "stale", metadata.Status)
	assert.False(t, done(reportPath), "a stale report is not a result: the unit may be re-planned")
}

func TestDone(t *testing.T) {
	path := filepath.Join(t.TempDir(), "report.md")
	assert.False(t, done(path), "no report")
	broken := "---\nitem: x\nstatus: fail\nbrief: page says: use -n ray-system\n---\n\n# x\n"
	require.NoError(t, os.WriteFile(path, []byte(broken), 0o600))
	assert.True(t, done(path), "a report that exists but does not parse is kept")
}
