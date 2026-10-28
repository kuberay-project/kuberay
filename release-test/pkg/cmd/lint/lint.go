package lint

import (
	"fmt"

	"github.com/spf13/cobra"
	"k8s.io/kubectl/pkg/util/templates"

	"github.com/ray-project/kuberay/release-test/pkg/util"
)

var lintLong = templates.LongDesc(`
	Check release-test/manifest.yaml against items.yaml: every unit has an Item, a source_sha, and
	steps; Items that require hardware have no unit; and no command pins or floats the KubeRay version instead of using $KUBERAY_VERSION or $CHART_VERSION.
	Unknown keys are errors. run lints first.
`)

type lintOptions struct {
	paths util.Paths
}

func newLintOptions() *lintOptions {
	return &lintOptions{}
}

func NewLintCommand() *cobra.Command {
	options := newLintOptions()

	cmd := &cobra.Command{
		Use:          "lint",
		Short:        "Check release-test/manifest.yaml",
		Long:         lintLong,
		Args:         cobra.NoArgs,
		SilenceUsage: true,
		RunE: func(_ *cobra.Command, _ []string) error {
			if err := options.Complete(); err != nil {
				return err
			}
			return options.Run()
		},
	}

	return cmd
}

func (options *lintOptions) Complete() error {
	repoPaths, err := util.FindPaths()
	if err != nil {
		return err
	}
	options.paths = repoPaths
	return nil
}

func (options *lintOptions) Run() error {
	problems := lintFiles(options.paths)
	if len(problems) == 0 {
		fmt.Println("manifest.yaml: ok")
		return nil
	}
	util.PrintProblems(problems)
	return util.ErrLint
}

func lintFiles(repoPaths util.Paths) []string {
	items, err := util.LoadItems(repoPaths.Items)
	if err != nil {
		return []string{err.Error()}
	}
	units, err := util.LoadManifest(repoPaths.Manifest)
	if err != nil {
		return []string{err.Error()}
	}
	return util.LintManifest(items, units)
}
