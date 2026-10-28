package get

import (
	"fmt"
	"strings"

	"github.com/spf13/cobra"
	"k8s.io/cli-runtime/pkg/genericclioptions"
	cmdutil "k8s.io/kubectl/pkg/cmd/util"
)

func NewGetCommand(cmdFactory cmdutil.Factory, streams genericclioptions.IOStreams) *cobra.Command {
	cmd := &cobra.Command{
		Use:          "get",
		Short:        "Display one or many Ray resources.",
		Long:         `Prints a table of the most important information about the specified Ray resources.`,
		Aliases:      []string{"list"},
		SilenceUsage: true,
		Run: func(cmd *cobra.Command, args []string) {
			if len(args) > 0 {
				fmt.Println(fmt.Errorf("unknown command(s) %q", strings.Join(args, " ")))
			}
			cmd.HelpFunc()(cmd, args)
		},
	}

	cmd.AddCommand(NewGetClusterCommand(cmdFactory, streams))
	cmd.AddCommand(NewGetWorkerGroupCommand(cmdFactory, streams))
	cmd.AddCommand(NewGetNodesCommand(cmdFactory, streams))
	cmd.AddCommand(NewGetTokenCommand(cmdFactory, streams))
	return cmd
}

// joinLabelMap joins a map of K8s label key-val entries into a label selector string
func joinLabelMap(labelMap map[string]string) string {
	var labels []string
	for k, v := range labelMap {
		labels = append(labels, fmt.Sprintf("%s=%s", k, v))
	}
	return strings.Join(labels, ",")
}
