package get

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/spf13/cobra"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/duration"
	"k8s.io/cli-runtime/pkg/genericclioptions"
	"k8s.io/cli-runtime/pkg/printers"
	cmdutil "k8s.io/kubectl/pkg/cmd/util"
	"k8s.io/kubectl/pkg/util/templates"

	"github.com/ray-project/kuberay/kubectl-plugin/pkg/util"
	"github.com/ray-project/kuberay/kubectl-plugin/pkg/util/client"
	"github.com/ray-project/kuberay/kubectl-plugin/pkg/util/completion"
)

type GetNodesOptions struct {
	cmdFactory    cmdutil.Factory
	ioStreams     *genericclioptions.IOStreams
	namespace     string
	cluster       string
	node          string
	allNamespaces bool
}

type node struct {
	creationTimestamp v1.Time
	cpus              resource.Quantity
	gpus              resource.Quantity
	tpus              resource.Quantity
	memory            resource.Quantity
	namespace         string
	cluster           string
	_type             string
	workerGroup       string
	name              string
}

var getNodesExample = templates.Examples(`
		# Get nodes in the default namespace
		kubectl ray get node

		# Get nodes in all namespaces
		kubectl ray get node --all-namespaces

		# Get all nodes in a namespace
		kubectl ray get node --namespace my-namespace

		# Get all nodes for Ray clusters named my-raycluster in all namespaces
		kubectl ray get node --ray-cluster my-raycluster --all-namespaces

		# Get all nodes in a namespace for a Ray cluster
		kubectl ray get node --namespace my-namespace --ray-cluster my-raycluster

		# Get one node in a namespace for a Ray cluster
		kubectl ray get node my-node --namespace my-namespace --ray-cluster my-raycluster
	`)

func NewGetNodesOptions(cmdFactory cmdutil.Factory, streams genericclioptions.IOStreams) *GetNodesOptions {
	return &GetNodesOptions{
		cmdFactory: cmdFactory,
		ioStreams:  &streams,
	}
}

func NewGetNodesCommand(cmdFactory cmdutil.Factory, streams genericclioptions.IOStreams) *cobra.Command {
	options := NewGetNodesOptions(cmdFactory, streams)

	cmd := &cobra.Command{
		Use:               "node [NODE] [(-c|--ray-cluster) RAYCLUSTER]",
		Aliases:           []string{"nodes"},
		Short:             "Get Ray nodes",
		Example:           getNodesExample,
		SilenceUsage:      true,
		Args:              cobra.MaximumNArgs(1),
		ValidArgsFunction: completion.NodeCompletionFunc(cmdFactory),
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := options.Complete(args); err != nil {
				return err
			}
			k8sClient, err := client.NewClient(cmdFactory)
			if err != nil {
				return fmt.Errorf("failed to create client: %w", err)
			}
			return options.Run(cmd.Context(), k8sClient)
		},
	}

	cmd.Flags().StringVarP(&options.cluster, "ray-cluster", "c", "", "Ray cluster")
	cmd.Flags().BoolVarP(&options.allNamespaces, "all-namespaces", "A", false, "If present, list nodes across all namespaces. Namespace in current context is ignored even if specified with --namespace.")

	err := cmd.RegisterFlagCompletionFunc("ray-cluster", completion.RayClusterCompletionFunc(cmdFactory))
	if err != nil {
		fmt.Fprintf(streams.ErrOut, "Error registering completion function for --ray-cluster: %v\n", err)
	}

	return cmd
}

func (options *GetNodesOptions) Complete(args []string) error {
	if options.allNamespaces {
		options.namespace = ""
	} else {
		namespace, _, err := options.cmdFactory.ToRawKubeConfigLoader().Namespace()
		if err != nil {
			return fmt.Errorf("failed to get namespace: %w", err)
		}
		options.namespace = namespace
	}

	if len(args) > 0 {
		options.node = args[0]
	}

	return nil
}

func (options *GetNodesOptions) Run(ctx context.Context, k8sClient client.Client) error {
	listopts := v1.ListOptions{
		LabelSelector: joinLabelMap(createRayNodeLabelSelectors(options.cluster)),
	}

	if options.node != "" {
		listopts.FieldSelector = fmt.Sprintf("metadata.name=%s", options.node)
	}

	pods, err := k8sClient.KubernetesClient().CoreV1().Pods(options.namespace).List(ctx, listopts)
	if err != nil {
		return fmt.Errorf("unable to get Ray nodes: %w", err)
	}

	nodes := podsToNodes(pods.Items)

	if options.node != "" && len(nodes) == 0 {
		return errors.New(errorMessageForNodeNotFound(options.node, options.cluster, options.namespace, options.allNamespaces))
	}

	return printNodes(nodes, options.allNamespaces, options.ioStreams.Out)
}

// createRayNodeLabelSelectors creates a map of K8s label selectors for Ray nodes
func createRayNodeLabelSelectors(cluster string) map[string]string {
	labelSelectors := map[string]string{
		util.RayIsRayNodeLabelKey: "yes",
	}
	if cluster != "" {
		labelSelectors[util.RayClusterLabelKey] = cluster
	}
	return labelSelectors
}

// errorMessageForNodeNotFound returns an error message for when a node is not found
func errorMessageForNodeNotFound(node, cluster, namespace string, allNamespaces bool) string {
	errMsg := fmt.Sprintf("Ray node %s not found", node)

	if allNamespaces {
		errMsg += " in any namespace"
	} else {
		errMsg += fmt.Sprintf(" in namespace %s", namespace)
	}

	if cluster != "" {
		errMsg += fmt.Sprintf(" in Ray cluster %s", cluster)
	} else {
		errMsg += " in any Ray cluster"
	}

	return errMsg
}

// podsToNodes converts an array of K8s Pods to a list of nodes
func podsToNodes(pods []corev1.Pod) []node {
	var nodes []node
	for _, pod := range pods {
		nodes = append(nodes, node{
			namespace:         pod.Namespace,
			cluster:           pod.Labels[util.RayClusterLabelKey],
			_type:             pod.Labels[util.RayNodeTypeLabelKey],
			workerGroup:       pod.Labels[util.RayNodeGroupLabelKey],
			name:              pod.Name,
			cpus:              *pod.Spec.Containers[0].Resources.Requests.Cpu(),
			gpus:              *pod.Spec.Containers[0].Resources.Requests.Name(corev1.ResourceName(util.ResourceNvidiaGPU), resource.DecimalSI),
			tpus:              *pod.Spec.Containers[0].Resources.Requests.Name(corev1.ResourceName(util.ResourceGoogleTPU), resource.DecimalSI),
			memory:            *pod.Spec.Containers[0].Resources.Requests.Memory(),
			creationTimestamp: pod.CreationTimestamp,
		})
	}
	return nodes
}

// printNodes prints a list of nodes to the output
func printNodes(nodes []node, allNamespaces bool, output io.Writer) error {
	resultTablePrinter := printers.NewTablePrinter(printers.PrintOptions{})

	columns := []v1.TableColumnDefinition{}
	if allNamespaces {
		columns = append(columns, v1.TableColumnDefinition{Name: "Namespace", Type: "string"})
	}
	columns = append(columns, []v1.TableColumnDefinition{
		{Name: "Name", Type: "string"},
		{Name: "CPUs", Type: "string"},
		{Name: "GPUs", Type: "string"},
		{Name: "TPUs", Type: "string"},
		{Name: "Memory", Type: "string"},
		{Name: "Cluster", Type: "string"},
		{Name: "Type", Type: "string"},
		{Name: "Worker Group", Type: "string"},
		{Name: "Age", Type: "string"},
	}...)

	resTable := &v1.Table{ColumnDefinitions: columns}

	for _, node := range nodes {
		age := duration.HumanDuration(time.Since(node.creationTimestamp.Time))
		if node.creationTimestamp.Time.IsZero() {
			age = "<unknown>"
		}

		row := v1.TableRow{}
		if allNamespaces {
			row.Cells = append(row.Cells, node.namespace)
		}
		row.Cells = append(row.Cells, []any{
			node.name,
			node.cpus.String(),
			node.gpus.String(),
			node.tpus.String(),
			node.memory.String(),
			node.cluster,
			node._type,
			node.workerGroup,
			age,
		}...)

		resTable.Rows = append(resTable.Rows, row)
	}

	return resultTablePrinter.PrintObj(resTable, output)
}
