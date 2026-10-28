package summary

import (
	"github.com/spf13/cobra"
	"k8s.io/kubectl/pkg/util/templates"

	"github.com/ray-project/kuberay/release-test/pkg/util"
	"github.com/ray-project/kuberay/release-test/pkg/util/report"
)

var (
	summaryLong = templates.LongDesc(`
		Write SUMMARY.md into a version's results directory from the front matter of its reports: one
		row per Item, grouped into Fail, Stale, Manual, Not run, Not planned and Pass. run writes it
		when it finishes; use this after editing a report by hand or after an interrupted run.
	`)

	summaryExample = templates.Examples(`
		# release-test/results/v1.8.0-rc.0/SUMMARY.md
		go run ./release-test summary --kuberay-version v1.8.0-rc.0
	`)
)

type summaryOptions struct {
	paths   util.Paths
	version string
}

func newSummaryOptions() *summaryOptions {
	return &summaryOptions{}
}

func NewSummaryCommand() *cobra.Command {
	options := newSummaryOptions()

	cmd := &cobra.Command{
		Use:          "summary --kuberay-version TAG",
		Short:        "Write SUMMARY.md for a version's results",
		Long:         summaryLong,
		Example:      summaryExample,
		Args:         cobra.NoArgs,
		SilenceUsage: true,
		RunE: func(_ *cobra.Command, _ []string) error {
			if err := options.Complete(); err != nil {
				return err
			}
			if err := options.Validate(); err != nil {
				return err
			}
			return options.Run()
		},
	}
	cmd.Flags().StringVar(&options.version, "kuberay-version", "", "the tag whose results to summarize, e.g. v1.8.0-rc.0")
	cobra.CheckErr(cmd.MarkFlagRequired("kuberay-version"))

	return cmd
}

func (options *summaryOptions) Complete() error {
	repoPaths, err := util.FindPaths()
	if err != nil {
		return err
	}
	repoPaths.Results = repoPaths.ResultsDir(options.version)
	options.paths = repoPaths
	return nil
}

func (options *summaryOptions) Validate() error {
	return util.ValidateVersion(options.version)
}

func (options *summaryOptions) Run() error {
	return report.WriteSummary(options.paths, options.version)
}
