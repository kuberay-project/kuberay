package util

import (
	"errors"
	"fmt"
	"os"
	"regexp"
	"strings"
)

// hardcodedVersion: KubeRay references that pin or float the version instead of using the env vars.
var hardcodedVersion = regexp.MustCompile(`(?:` + KuberayVersionRefs + `)(?:master|main|latest|nightly|v?[0-9])`)

var ErrLint = errors.New("fix manifest.yaml first")

func PrintProblems(problems []string) {
	fmt.Fprintln(os.Stderr, "manifest.yaml:\n  "+strings.Join(problems, "\n  "))
}

// LintManifest returns one line per problem, nil when the manifest is fine.
func LintManifest(items []Item, units []Unit) []string {
	var problems []string
	add := func(id, format string, args ...any) {
		problems = append(problems, id+": "+fmt.Sprintf(format, args...))
	}
	byID, planned := ItemsByID(items), map[string]bool{}

	for _, unit := range units {
		if planned[unit.ID] {
			add(unit.ID, "duplicate unit")
		}
		planned[unit.ID] = true
	}
	for _, unit := range units {
		item, ok := byID[unit.ID]
		switch {
		case !ok:
			add(unit.ID, "not in items.yaml")
		case len(item.Requires) > 0:
			add(unit.ID, "the Item has requires, so it must not have a unit")
		}
		if unit.SourceSHA == "" {
			add(unit.ID, "no source_sha")
		}
		if len(unit.Steps) == 0 {
			add(unit.ID, "no steps")
		}
		commands := append([]string{}, unit.Cleanup...)
		for i, step := range unit.Steps {
			if strings.TrimSpace(step.Run) == "" {
				add(unit.ID, "step %d has no run", i+1)
			}
			commands = append(commands, step.Run, step.Check)
		}
		for _, cmd := range commands {
			if cmd = strings.ReplaceAll(cmd, "\n", " "); hardcodedVersion.MatchString(cmd) {
				add(unit.ID, "%s  <- hard-coded or floating KubeRay version; use $KUBERAY_VERSION or $CHART_VERSION", cmd)
			}
		}
	}
	return problems
}
