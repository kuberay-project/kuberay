package version

import (
	"context"
	"fmt"
	"io"
	"os"
	"runtime/debug"

	"github.com/spf13/cobra"
	"k8s.io/cli-runtime/pkg/genericclioptions"
	cmdutil "k8s.io/kubectl/pkg/cmd/util"

	"github.com/ray-project/kuberay/kubectl-plugin/pkg/util/client"
)

var Version = "development"

type VersionOptions struct {
	cmdFactory cmdutil.Factory
	ioStreams  *genericclioptions.IOStreams
}

func NewVersionOptions(cmdFactory cmdutil.Factory, streams genericclioptions.IOStreams) *VersionOptions {
	return &VersionOptions{
		cmdFactory: cmdFactory,
		ioStreams:  &streams,
	}
}

func NewVersionCommand(cmdFactory cmdutil.Factory, streams genericclioptions.IOStreams) *cobra.Command {
	options := NewVersionOptions(cmdFactory, streams)

	cmd := &cobra.Command{
		Use:          "version",
		Short:        "Output the version of the Ray kubectl plugin and KubeRay operator",
		SilenceUsage: true,
		RunE: func(cmd *cobra.Command, _ []string) error {
			// running cmd.Execute or cmd.ExecuteE sets the context, which will be done by root
			k8sClient, err := client.NewClient(cmdFactory)
			if err != nil {
				return fmt.Errorf("failed to create client: %w", err)
			}
			return options.Run(cmd.Context(), k8sClient, debug.ReadBuildInfo, os.Stdout)
		},
	}

	return cmd
}

func (options *VersionOptions) Run(ctx context.Context, k8sClient client.Client, readBuildInfo func() (*debug.BuildInfo, bool), writer io.Writer) error {
	if Version == "development" {
		commit, buildTime, err := commitAndBuildTime(readBuildInfo)
		if err == nil {
			Version = fmt.Sprintf("development (%s, built %s)", commit[:7], buildTime)
		}

	}
	fmt.Fprintln(writer, "kubectl ray plugin version:", Version)

	operatorVersion, err := k8sClient.GetKubeRayOperatorVersion(ctx)
	if err != nil {
		wrappedError := fmt.Errorf(`warning: KubeRay operator installation cannot be found: %w. Did you install it with the name "kuberay-operator"?`, err)
		fmt.Fprintln(writer, wrappedError)
	} else {
		fmt.Fprintln(writer, "KubeRay operator version:", operatorVersion)
	}
	return nil
}

func commitAndBuildTime(readBuildInfo func() (*debug.BuildInfo, bool)) (commit, buildtime string, err error) {
	info, ok := readBuildInfo()
	if !ok || info == nil {
		return "", "", fmt.Errorf("no debug build info")
	}
	for _, setting := range info.Settings {
		switch setting.Key {
		case "vcs.revision":
			commit = setting.Value
		case "vcs.time":
			buildtime = setting.Value
		}
	}
	if commit == "" || buildtime == "" {
		return "", "", fmt.Errorf("missing revision or build time from build info")
	}
	return
}
