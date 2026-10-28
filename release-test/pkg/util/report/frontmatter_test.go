package report

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The release-test-run skill fills in triage and brief by replacing their lines, quoted.
func TestTriageEdit(t *testing.T) {
	two := 2
	report := Render("front.tmpl", FrontMatter{Item: "x", Status: "fail", Version: "v9.9.9", FailedStep: &two, Brief: "step 2 run exit 1"}) + "# x\n"
	report = strings.Replace(report, `triage: ""`, `triage: "doc-bug"`, 1)
	report = strings.Replace(report, `brief: "step 2 run exit 1"`, `brief: "page says: use -n ray-system"`, 1)
	metadata, err := parseFrontMatter(report)
	require.NoError(t, err)
	assert.Equal(t, "doc-bug", metadata.Triage)
	assert.Equal(t, "page says: use -n ray-system", metadata.Brief)

	_, err = parseFrontMatter(strings.Replace(report, `"page says: use -n ray-system"`, "page says: use -n ray-system", 1))
	require.Error(t, err, "an unquoted brief with a colon does not parse")
}
