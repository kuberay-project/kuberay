package kubectlraydelete

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/spf13/cobra"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/cli-runtime/pkg/genericclioptions"
	"k8s.io/cli-runtime/pkg/genericiooptions"
	cmdutil "k8s.io/kubectl/pkg/cmd/util"
	"k8s.io/kubectl/pkg/util/templates"

	"github.com/ray-project/kuberay/kubectl-plugin/pkg/util"
	"github.com/ray-project/kuberay/kubectl-plugin/pkg/util/client"
	"github.com/ray-project/kuberay/kubectl-plugin/pkg/util/completion"
)

type DeleteOptions struct {
	cmdFactory cmdutil.Factory
	ioStreams  *genericiooptions.IOStreams
	resources  map[util.ResourceType][]string
	namespace  string
	yes        bool
}

var deleteExample = templates.Examples(`
		# Delete Ray cluster
		kubectl ray delete sample-raycluster

		# Delete Ray cluster with specificed Ray resource
		kubectl ray delete raycluster/sample-raycluster

		# Delete RayJob
		kubectl ray delete rayjob/sample-rayjob

		# Delete RayService
		kubectl ray delete rayservice/sample-rayservice
	`)

func NewDeleteOptions(cmdFactory cmdutil.Factory, streams genericiooptions.IOStreams) *DeleteOptions {
	return &DeleteOptions{
		ioStreams:  &streams,
		cmdFactory: cmdFactory,
		resources:  map[util.ResourceType][]string{},
	}
}

func NewDeleteCommand(cmdFactory cmdutil.Factory, streams genericclioptions.IOStreams) *cobra.Command {
	options := NewDeleteOptions(cmdFactory, streams)

	cmd := &cobra.Command{
		Use:     "delete (RAYCLUSTER | TYPE/NAME)",
		Short:   "Delete Ray resources",
		Example: deleteExample,
		Long:    `Deletes Ray custom resources such as RayCluster, RayService, or RayJob`,
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) < 1 {
				return cmdutil.UsageErrorf(cmd, "accepts a minimum of 1 arg, received %d\n%s", len(args), cmd.Use)
			}
			return nil
		},
		ValidArgsFunction: completion.RayClusterResourceNameCompletionFunc(cmdFactory),
		SilenceUsage:      true,
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := options.Complete(cmd, args); err != nil {
				return err
			}
			return options.Run(cmd.Context(), cmdFactory)
		},
	}

	cmd.Flags().BoolVarP(&options.yes, "yes", "y", false, "answer 'yes' to all prompts and run non-interactively")
	return cmd
}

func (options *DeleteOptions) Complete(cmd *cobra.Command, args []string) error {
	namespace, _, err := options.cmdFactory.ToRawKubeConfigLoader().Namespace()
	if err != nil {
		return fmt.Errorf("failed to get namespace: %w", err)
	}
	options.namespace = namespace

	if options.resources == nil {
		options.resources = map[util.ResourceType][]string{}
	}

	for _, arg := range args {
		typeAndName := strings.Split(arg, "/")
		if len(typeAndName) == 1 {
			if _, ok := options.resources[util.RayCluster]; !ok {
				options.resources[util.RayCluster] = []string{}
			}
			options.resources[util.RayCluster] = append(options.resources[util.RayCluster], typeAndName[0])
		} else {
			if len(typeAndName) != 2 || typeAndName[1] == "" {
				return cmdutil.UsageErrorf(cmd, "invalid resource type/name: %s", arg)
			}

			switch strings.ToLower(typeAndName[0]) {
			case string(util.RayCluster):
				if _, ok := options.resources[util.RayCluster]; !ok {
					options.resources[util.RayCluster] = []string{}
				}
				options.resources[util.RayCluster] = append(options.resources[util.RayCluster], typeAndName[1])
			case string(util.RayJob):
				if _, ok := options.resources[util.RayJob]; !ok {
					options.resources[util.RayJob] = []string{}
				}
				options.resources[util.RayJob] = append(options.resources[util.RayJob], typeAndName[1])
			case string(util.RayService):
				if _, ok := options.resources[util.RayService]; !ok {
					options.resources[util.RayService] = []string{}
				}
				options.resources[util.RayService] = append(options.resources[util.RayService], typeAndName[1])
			default:
				return cmdutil.UsageErrorf(cmd, "unsupported resource type: %s", arg)
			}
		}
	}

	return nil
}

func (options *DeleteOptions) Run(ctx context.Context, factory cmdutil.Factory) error {
	k8sClient, err := client.NewClient(factory)
	if err != nil {
		return fmt.Errorf("failed to create client: %w", err)
	}

	var resources strings.Builder
	for resourceType, resourceNames := range options.resources {
		for _, resourceName := range resourceNames {
			fmt.Fprintf(&resources, "\n- %s/%s", resourceType, resourceName)
		}
	}

	if !options.yes {
		// Ask user for confirmation
		reader := bufio.NewReader(os.Stdin)
		fmt.Printf("Are you sure you want to delete the following resources?%s\n(y/yes/n/no)", resources.String())
		confirmation, err := reader.ReadString('\n')
		if err != nil {
			return fmt.Errorf("Failed to read user input: %w", err)
		}

		switch strings.ToLower(strings.TrimSpace(confirmation)) {
		case "y", "yes":
		case "n", "no":
			fmt.Printf("Canceled deletion.\n")
			return nil
		default:
			fmt.Printf("Unknown input %s\n", confirmation)
			return nil
		}
	}

	// Delete the Ray resources
	for resourceType, resourceNames := range options.resources {
		for _, resourceName := range resourceNames {
			if err := deleteResource(ctx, k8sClient, options.namespace, resourceType, resourceName); err != nil {
				return fmt.Errorf("failed to delete %s/%s: %w", resourceType, resourceName, err)
			}
			fmt.Printf("Deleted %s %s\n", resourceType, resourceName)
		}
	}

	return nil
}

func deleteResource(ctx context.Context, k8sClient client.Client, namespace string, resourceType util.ResourceType, resourceName string) error {
	var err error

	switch resourceType {
	case util.RayCluster:
		err = k8sClient.RayClient().RayV1().RayClusters(namespace).Delete(ctx, resourceName, metav1.DeleteOptions{})
	case util.RayJob:
		err = k8sClient.RayClient().RayV1().RayJobs(namespace).Delete(ctx, resourceName, metav1.DeleteOptions{})
	case util.RayService:
		err = k8sClient.RayClient().RayV1().RayServices(namespace).Delete(ctx, resourceName, metav1.DeleteOptions{})
	default:
		err = fmt.Errorf("unknown/unsupported resource type: %s", resourceType)
	}

	if err != nil {
		return err
	}

	return nil
}
