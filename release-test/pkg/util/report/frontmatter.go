package report

import (
	"errors"
	"os"
	"path/filepath"
	"strings"

	"sigs.k8s.io/yaml"
)

// FrontMatter heads results/<tag>/<id>/report.md; the runner writes it, the release-test-run skill
// fills in triage and brief.
type FrontMatter struct {
	Item       string `json:"item"`
	Status     string `json:"status"`
	Version    string `json:"version"`
	FailedStep *int   `json:"failed_step"`
	Triage     string `json:"triage"`
	Brief      string `json:"brief"`
}

// ReadFrontMatter parses the YAML front matter at the top of the report at path.
func ReadFrontMatter(path string) (FrontMatter, error) {
	data, err := os.ReadFile(filepath.Clean(path))
	if err != nil {
		return FrontMatter{}, err
	}
	return parseFrontMatter(string(data))
}

func parseFrontMatter(report string) (FrontMatter, error) {
	var metadata FrontMatter
	rest, ok := strings.CutPrefix(report, "---\n")
	if !ok {
		return metadata, errors.New("no front matter")
	}
	head, _, ok := strings.Cut(rest, "\n---\n")
	if !ok {
		return metadata, errors.New("front matter is not closed")
	}
	err := yaml.Unmarshal([]byte(head), &metadata)
	return metadata, err
}
