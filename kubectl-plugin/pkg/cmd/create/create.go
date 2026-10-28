package create

import (
	"fmt"
	"strings"

	"github.com/spf13/cobra"
	"k8s.io/cli-runtime/pkg/genericclioptions"
	cmdutil "k8s.io/kubectl/pkg/cmd/util"
)

func NewCreateCommand(cmdFactory cmdutil.Factory, streams genericclioptions.IOStreams) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "create",
		Short: "Create Ray resources",
		Long:  `Allow users to create Ray resources. And based on input, will generate the necessary files`,
		Run: func(cmd *cobra.Command, args []string) {
			if len(args) > 0 {
				fmt.Println(fmt.Errorf("unknown command(s) %q", strings.Join(args, " ")))
			}
			cmd.HelpFunc()(cmd, args)
		},
	}

	cmd.AddCommand(NewCreateClusterCommand(cmdFactory, streams))
	cmd.AddCommand(NewCreateWorkerGroupCommand(cmdFactory, streams))
	return cmd
}
