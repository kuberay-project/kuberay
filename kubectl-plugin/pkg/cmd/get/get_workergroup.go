package get

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"

	"github.com/spf13/cobra"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/cli-runtime/pkg/genericclioptions"
	"k8s.io/cli-runtime/pkg/printers"
	cmdutil "k8s.io/kubectl/pkg/cmd/util"
	"k8s.io/kubectl/pkg/util/templates"

	"github.com/ray-project/kuberay/kubectl-plugin/pkg/util"
	"github.com/ray-project/kuberay/kubectl-plugin/pkg/util/client"
	"github.com/ray-project/kuberay/kubectl-plugin/pkg/util/completion"
	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
)

type GetWorkerGroupsOptions struct {
	cmdFactory    cmdutil.Factory
	ioStreams     *genericclioptions.IOStreams
	namespace     string
	cluster       string
	workerGroup   string
	allNamespaces bool
}

type enrichedWorkerGroupSpec struct {
	namespace string
	cluster   string
	spec      rayv1.WorkerGroupSpec
}

type workerGroup struct {
	totalCPU        resource.Quantity
	totalGPU        resource.Quantity
	totalTPU        resource.Quantity
	totalMemory     resource.Quantity
	namespace       string
	name            string
	cluster         string
	readyReplicas   int32
	desiredReplicas int32
	minReplicas     int32
	maxReplicas     int32
}

var getWorkerGroupsExample = templates.Examples(`
		# Get worker groups in the default namespace
		kubectl ray get workergroup

		# Get worker groups in all namespaces
		kubectl ray get workergroup --all-namespaces

		# Get all worker groups in a namespace
		kubectl ray get workergroup --namespace my-namespace

		# Get all worker groups for Ray clusters named my-raycluster in all namespaces
		kubectl ray get workergroup --ray-cluster my-raycluster --all-namespaces

		# Get all worker groups in a namespace for a Ray cluster
		kubectl ray get workergroup --namespace my-namespace --ray-cluster my-raycluster

		# Get one worker group in a namespace for a Ray cluster
		kubectl ray get workergroup my-group --namespace my-namespace --ray-cluster my-raycluster
	`)

func NewGetWorkerGroupOptions(cmdFactory cmdutil.Factory, streams genericclioptions.IOStreams) *GetWorkerGroupsOptions {
	return &GetWorkerGroupsOptions{
		cmdFactory: cmdFactory,
		ioStreams:  &streams,
	}
}

func NewGetWorkerGroupCommand(cmdFactory cmdutil.Factory, streams genericclioptions.IOStreams) *cobra.Command {
	options := NewGetWorkerGroupOptions(cmdFactory, streams)

	cmd := &cobra.Command{
		Use:               "workergroup [GROUP] [(-c/--ray-cluster) RAYCLUSTER]",
		Aliases:           []string{"workergroups"},
		Short:             "Get Ray worker groups",
		Example:           getWorkerGroupsExample,
		SilenceUsage:      true,
		Args:              cobra.MaximumNArgs(1),
		ValidArgsFunction: completion.WorkerGroupCompletionFunc(cmdFactory),
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
	cmd.Flags().BoolVarP(&options.allNamespaces, "all-namespaces", "A", false, "If present, list workergroups across all namespaces. Namespace in current context is ignored even if specified with --namespace.")

	err := cmd.RegisterFlagCompletionFunc("ray-cluster", completion.RayClusterCompletionFunc(cmdFactory))
	if err != nil {
		fmt.Fprintf(streams.ErrOut, "Error registering completion function for --ray-cluster: %v\n", err)
	}

	return cmd
}

func (options *GetWorkerGroupsOptions) Complete(args []string) error {
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
		options.workerGroup = args[0]
	}

	return nil
}

func (options *GetWorkerGroupsOptions) Run(ctx context.Context, k8sClient client.Client) error {
	var rayClusters []rayv1.RayCluster
	if options.cluster == "" {
		rayClusterList, err := k8sClient.RayClient().RayV1().RayClusters(options.namespace).List(ctx, v1.ListOptions{})
		if err != nil {
			return fmt.Errorf("unable to list Ray clusters in namespace %s: %w", options.namespace, err)
		}
		rayClusters = append(rayClusters, rayClusterList.Items...)
	} else {
		rayClusterList, err := k8sClient.RayClient().RayV1().RayClusters(options.namespace).List(ctx, v1.ListOptions{
			FieldSelector: fmt.Sprintf("metadata.name=%s", options.cluster),
		})
		if err != nil {
			return fmt.Errorf("unable to get Ray cluster %s in namespace %s: %w", options.cluster, options.namespace, err)
		}
		rayClusters = append(rayClusters, rayClusterList.Items...)
	}

	enrichedWorkerGroupSpecs := []enrichedWorkerGroupSpec{}
	for _, rayCluster := range rayClusters {
		if options.workerGroup == "" {
			for _, spec := range rayCluster.Spec.WorkerGroupSpecs {
				enrichedWorkerGroupSpecs = append(enrichedWorkerGroupSpecs, enrichedWorkerGroupSpec{
					namespace: rayCluster.Namespace,
					cluster:   rayCluster.Name,
					spec:      spec,
				})
			}
		} else {
			for _, spec := range rayCluster.Spec.WorkerGroupSpecs {
				if spec.GroupName == options.workerGroup {
					enrichedWorkerGroupSpecs = append(enrichedWorkerGroupSpecs, enrichedWorkerGroupSpec{
						namespace: rayCluster.Namespace,
						cluster:   rayCluster.Name,
						spec:      spec,
					})
				}
			}
		}
	}

	if options.workerGroup != "" && len(enrichedWorkerGroupSpecs) == 0 {
		return errors.New(errorMessageForWorkerGroupNotFound(options.workerGroup, options.cluster, options.namespace, options.allNamespaces))
	}

	workerGroups, err := getWorkerGroupDetails(ctx, enrichedWorkerGroupSpecs, k8sClient)
	if err != nil {
		return err
	}

	return printWorkerGroups(workerGroups, options.allNamespaces, options.ioStreams.Out)
}

// errorMessageForWorkerGroupNotFound returns an error message for when a worker group is not found
func errorMessageForWorkerGroupNotFound(workerGroup, cluster, namespace string, allNamespaces bool) string {
	errMsg := fmt.Sprintf("Ray worker group %s not found", workerGroup)

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

// getWorkerGroupDetails takes an array of enrichedWorkerGroupSpecs, gets the corresponding K8s Pod, and returns an array of worker groups
func getWorkerGroupDetails(ctx context.Context, enrichedWorkerGroupSpecs []enrichedWorkerGroupSpec, k8sClient client.Client) ([]workerGroup, error) {
	var workerGroups []workerGroup

	for _, ewgs := range enrichedWorkerGroupSpecs {
		selectors, err := createRayWorkerGroupLabelSelectors(ewgs.spec.GroupName, ewgs.cluster)
		if err != nil {
			return nil, fmt.Errorf("could not create K8s label selectors for a worker group in cluster %s, namespace %s: %w", ewgs.cluster, ewgs.namespace, err)
		}

		podList, err := k8sClient.KubernetesClient().CoreV1().Pods(ewgs.namespace).List(ctx, v1.ListOptions{
			LabelSelector: joinLabelMap(selectors),
		})
		if err != nil {
			return nil, err
		}

		readyWorkerReplicas := calculateReadyReplicas(*podList)

		workerGroupResources := calculateDesiredResourcesForWorkerGroup(ewgs.spec)

		var (
			minReplicas int32 // default: 0
			maxReplicas int32 = math.MaxInt32
		)

		if ewgs.spec.MinReplicas != nil {
			minReplicas = *ewgs.spec.MinReplicas
		}

		if ewgs.spec.MaxReplicas != nil {
			maxReplicas = *ewgs.spec.MaxReplicas
		}

		workerGroups = append(workerGroups, workerGroup{
			namespace:       ewgs.namespace,
			name:            ewgs.spec.GroupName,
			readyReplicas:   readyWorkerReplicas,
			desiredReplicas: *ewgs.spec.Replicas,
			totalCPU:        *workerGroupResources.Cpu(),
			totalGPU:        workerGroupResources[corev1.ResourceName(util.ResourceNvidiaGPU)],
			totalTPU:        workerGroupResources[corev1.ResourceName(util.ResourceGoogleTPU)],
			totalMemory:     *workerGroupResources.Memory(),
			cluster:         ewgs.cluster,
			minReplicas:     minReplicas,
			maxReplicas:     maxReplicas,
		})
	}

	return workerGroups, nil
}

// createRayWorkerGroupLabelSelectors creates a map of K8s label selectors for Ray worker groups
func createRayWorkerGroupLabelSelectors(groupName, cluster string) (map[string]string, error) {
	if groupName == "" {
		return nil, errors.New("group name cannot be empty")
	}

	labelSelectors := map[string]string{
		util.RayNodeTypeLabelKey:  string(rayv1.WorkerNode),
		util.RayNodeGroupLabelKey: groupName,
	}
	if cluster != "" {
		labelSelectors[util.RayClusterLabelKey] = cluster
	}

	return labelSelectors, nil
}

// printWorkerGroups prints worker groups
func printWorkerGroups(workerGroups []workerGroup, allNamespaces bool, output io.Writer) error {
	resultTablePrinter := printers.NewTablePrinter(printers.PrintOptions{})

	columns := []v1.TableColumnDefinition{}
	if allNamespaces {
		columns = append(columns, v1.TableColumnDefinition{Name: "Namespace", Type: "string"})
	}

	columns = append(columns, []v1.TableColumnDefinition{
		{Name: "Name", Type: "string"},
		{Name: "Min", Type: "string"},
		{Name: "Max", Type: "string"},
		{Name: "Replicas", Type: "string"},
		{Name: "CPUs", Type: "string"},
		{Name: "GPUs", Type: "string"},
		{Name: "TPUs", Type: "string"},
		{Name: "Memory", Type: "string"},
		{Name: "Cluster", Type: "string"},
	}...)

	resTable := &v1.Table{ColumnDefinitions: columns}

	for _, wg := range workerGroups {
		row := v1.TableRow{}
		if allNamespaces {
			row.Cells = append(row.Cells, wg.namespace)
		}

		minStr := fmt.Sprintf("%d", wg.minReplicas)
		maxStr := fmt.Sprintf("%d", wg.maxReplicas)

		row.Cells = append(row.Cells, []any{
			wg.name,
			minStr,
			maxStr,
			fmt.Sprintf("%d/%d", wg.readyReplicas, wg.desiredReplicas),
			wg.totalCPU.String(),
			wg.totalGPU.String(),
			wg.totalTPU.String(),
			wg.totalMemory.String(),
			wg.cluster,
		}...)

		resTable.Rows = append(resTable.Rows, row)
	}

	return resultTablePrinter.PrintObj(resTable, output)
}

// calculateDesiredResourcesForWorkerGroup calculates the desired resources for a worker group
func calculateDesiredResourcesForWorkerGroup(workerGroupSpec rayv1.WorkerGroupSpec) corev1.ResourceList {
	if workerGroupSpec.Suspend != nil && *workerGroupSpec.Suspend {
		return corev1.ResourceList{}
	}

	podResource := calculatePodResource(workerGroupSpec.Template.Spec)
	totalResource := corev1.ResourceList{}

	for range *workerGroupSpec.Replicas {
		for name, quantity := range podResource {
			totalResource[name] = quantity.DeepCopy()
			quantity := totalResource[name]
			(&quantity).Mul(int64(*workerGroupSpec.Replicas))
			// Mul() doesn't recalculate the "s" field. Call String() to do it.
			_ = quantity.String()
			totalResource[name] = quantity
		}
	}

	return totalResource
}

// calculatePodResource returns the total resources of a pod.
// Request values take precedence over limit values.
func calculatePodResource(podSpec corev1.PodSpec) corev1.ResourceList {
	podResource := corev1.ResourceList{}
	for _, container := range podSpec.Containers {
		containerResource := container.Resources.Requests.DeepCopy()
		if containerResource == nil {
			containerResource = corev1.ResourceList{}
		}
		for name, quantity := range container.Resources.Limits {
			if _, ok := containerResource[name]; !ok {
				containerResource[name] = quantity
			}
		}
		for name, quantity := range containerResource {
			if totalQuantity, ok := podResource[name]; ok {
				totalQuantity.Add(quantity)
				podResource[name] = totalQuantity
			} else {
				podResource[name] = quantity
			}
		}
	}
	return podResource
}

// calculateReadyReplicas calculates ready worker replicas at the cluster level
// A worker is ready if its Pod has a PodCondition with type == Ready and status == True
func calculateReadyReplicas(pods corev1.PodList) int32 {
	count := int32(0)
	for _, pod := range pods.Items {
		if val, ok := pod.Labels[util.RayNodeTypeLabelKey]; !ok || val != string(rayv1.WorkerNode) {
			continue
		}
		if isRunningAndReady(&pod) {
			count++
		}
	}

	return count
}

// isRunningAndReady returns true if Pod is in the PodRunning Phase, if it has a condition of PodReady.
func isRunningAndReady(pod *corev1.Pod) bool {
	if pod.Status.Phase != corev1.PodRunning {
		return false
	}
	for _, cond := range pod.Status.Conditions {
		if cond.Type == corev1.PodReady && cond.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}
