package cmd

import (
	"github.com/spf13/cobra"
	"k8s.io/cli-runtime/pkg/genericclioptions"
	"k8s.io/cli-runtime/pkg/genericiooptions"
	cmdutil "k8s.io/kubectl/pkg/cmd/util"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"

	"github.com/ray-project/kuberay/kubectl-plugin/pkg/cmd/create"
	kubectlraydelete "github.com/ray-project/kuberay/kubectl-plugin/pkg/cmd/delete"
	"github.com/ray-project/kuberay/kubectl-plugin/pkg/cmd/get"
	"github.com/ray-project/kuberay/kubectl-plugin/pkg/cmd/job"
	"github.com/ray-project/kuberay/kubectl-plugin/pkg/cmd/log"
	"github.com/ray-project/kuberay/kubectl-plugin/pkg/cmd/scale"
	"github.com/ray-project/kuberay/kubectl-plugin/pkg/cmd/session"
	"github.com/ray-project/kuberay/kubectl-plugin/pkg/cmd/version"
)

func init() {
	// Initialize the controller-runtime logger globally
	logger := zap.New(zap.UseDevMode(true))
	ctrl.SetLogger(logger)
}

func NewRayCommand(streams genericiooptions.IOStreams) *cobra.Command {
	cmd := &cobra.Command{
		Use:          "ray",
		Short:        "ray kubectl plugin",
		Long:         "Manage Ray resources on Kubernetes",
		SilenceUsage: true,
		Run: func(cmd *cobra.Command, args []string) {
			cmd.HelpFunc()(cmd, args)
		},
		CompletionOptions: cobra.CompletionOptions{
			DisableDefaultCmd: true,
		},
	}

	configFlags := genericclioptions.NewConfigFlags(true)
	configFlags.AddFlags(cmd.PersistentFlags())

	cmdFactory := cmdutil.NewFactory(configFlags)

	cmd.AddCommand(get.NewGetCommand(cmdFactory, streams))
	cmd.AddCommand(session.NewSessionCommand(cmdFactory, streams))
	cmd.AddCommand(log.NewClusterLogCommand(cmdFactory, streams))
	cmd.AddCommand(job.NewJobCommand(cmdFactory, streams))
	cmd.AddCommand(version.NewVersionCommand(cmdFactory, streams))
	cmd.AddCommand(create.NewCreateCommand(cmdFactory, streams))
	cmd.AddCommand(kubectlraydelete.NewDeleteCommand(cmdFactory, streams))
	cmd.AddCommand(scale.NewScaleCommand(cmdFactory, streams))

	return cmd
}
