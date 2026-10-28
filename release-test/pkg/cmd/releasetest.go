package cmd

import (
	"github.com/spf13/cobra"
	"k8s.io/kubectl/pkg/util/templates"

	"github.com/ray-project/kuberay/release-test/pkg/cmd/lint"
	"github.com/ray-project/kuberay/release-test/pkg/cmd/run"
	"github.com/ray-project/kuberay/release-test/pkg/cmd/stale"
	"github.com/ray-project/kuberay/release-test/pkg/cmd/summary"
)

var releaseTestLong = templates.LongDesc(`
	Validate a KubeRay release candidate against release-test/items.yaml on a local kind cluster.

	The release-test-plan and release-test-run skills plan units and triage failures; this program runs
	the units. Run it from the kuberay repository. See release-test/README.md.
`)

func NewReleaseTestCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:          "release-test",
		Short:        "Validate a KubeRay release candidate",
		Long:         releaseTestLong,
		SilenceUsage: true,
		Run: func(cmd *cobra.Command, args []string) {
			cmd.HelpFunc()(cmd, args)
		},
		CompletionOptions: cobra.CompletionOptions{
			DisableDefaultCmd: true,
		},
	}

	cmd.AddCommand(stale.NewStaleCommand())
	cmd.AddCommand(lint.NewLintCommand())
	cmd.AddCommand(run.NewRunCommand())
	cmd.AddCommand(summary.NewSummaryCommand())

	return cmd
}
